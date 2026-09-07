package redis

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/arck1/ody/execution"
)

// Pipeline runs use hashes so status changes do not require rewriting the potentially large input.
func encodeRun(run execution.PipelineRun) map[string]any {
	finished := ""
	if run.FinishedAt != nil {
		finished = run.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	return map[string]any{
		"id": run.ID.String(), "pipeline_name": run.PipelineName, "pipeline_version": run.PipelineVersion,
		"input": string(run.Input), "status": string(run.Status), "error": run.Error,
		"idempotency_key": run.IdempotencyKey,
		"revision":        run.Revision,
		"created_at":      run.CreatedAt.UTC().Format(time.RFC3339Nano), "updated_at": run.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"finished_at": finished,
	}
}

func decodeRun(values map[string]string) (execution.PipelineRun, error) {
	id, err := uuid.Parse(values["id"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	version := 0
	if _, err = fmt.Sscan(values["pipeline_version"], &version); err != nil {
		return execution.PipelineRun{}, err
	}
	revision := uint64(0)
	if values["revision"] != "" {
		if _, err = fmt.Sscan(values["revision"], &revision); err != nil {
			return execution.PipelineRun{}, err
		}
	}
	created, err := time.Parse(time.RFC3339Nano, values["created_at"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	updated, err := time.Parse(time.RFC3339Nano, values["updated_at"])
	if err != nil {
		return execution.PipelineRun{}, err
	}
	run := execution.PipelineRun{
		ID: id, PipelineName: values["pipeline_name"], PipelineVersion: version,
		Input: json.RawMessage(values["input"]), Status: execution.RunStatus(values["status"]), Error: values["error"],
		IdempotencyKey: values["idempotency_key"],
		Revision:       revision,
		CreatedAt:      created, UpdatedAt: updated,
	}
	if values["finished_at"] != "" {
		finished, parseErr := time.Parse(time.RFC3339Nano, values["finished_at"])
		if parseErr != nil {
			return execution.PipelineRun{}, parseErr
		}
		run.FinishedAt = &finished
	}
	return run, nil
}

func clearLease(item *execution.Execution) {
	item.LeaseOwner, item.LeaseToken, item.LeaseUntil = "", uuid.Nil, time.Time{}
}

// Redis sorted-set scores use milliseconds: sufficient for polling while remaining exactly
// representable in float64 for contemporary Unix timestamps.
func score(value time.Time) float64 { return float64(value.UnixMilli()) }

func cloneJSON(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func cloneUUID(value *uuid.UUID) *uuid.UUID {
	if value == nil {
		return nil
	}
	return new(*value)
}
