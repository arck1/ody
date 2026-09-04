package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"schedulor/execution"
)

// Engine starts and advances durable pipeline runs.
type Engine struct {
	store    execution.Store
	registry *Registry
}

type Snapshot struct {
	Run        execution.PipelineRun
	Executions []execution.Execution
}

func NewEngine(store execution.Store, registry *Registry) (*Engine, error) {
	if store == nil {
		return nil, errors.New("pipeline store is nil")
	}
	if registry == nil {
		return nil, errors.New("pipeline registry is nil")
	}
	return &Engine{store: store, registry: registry}, nil
}

// Run persists a pipeline invocation and schedules its root nodes.
func Run[I any](ctx context.Context, engine *Engine, definition *Definition[I], input I) (execution.PipelineRun, error) {
	if engine == nil {
		return execution.PipelineRun{}, errors.New("pipeline engine is nil")
	}
	if err := definition.validate(); err != nil {
		return execution.PipelineRun{}, err
	}
	if _, ok := engine.registry.definitions[registryKey(definition.name, definition.version)]; !ok {
		return execution.PipelineRun{}, fmt.Errorf("pipeline definition %s is not registered", registryKey(definition.name, definition.version))
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return execution.PipelineRun{}, fmt.Errorf("encode pipeline input: %w", err)
	}
	run, err := engine.store.CreatePipelineRun(ctx, execution.CreatePipelineRun{PipelineName: definition.name, PipelineVersion: definition.version, Input: raw})
	if err != nil {
		return execution.PipelineRun{}, err
	}
	if err = engine.Advance(ctx, run.ID); err != nil {
		return execution.PipelineRun{}, err
	}
	return engine.store.GetPipelineRun(ctx, run.ID)
}

// Advance idempotently schedules ready nodes and updates terminal run state.
func (e *Engine) Advance(ctx context.Context, runID uuid.UUID) error {
	run, err := e.store.GetPipelineRun(ctx, runID)
	if err != nil {
		return err
	}
	definition, ok := e.registry.definitions[registryKey(run.PipelineName, run.PipelineVersion)]
	if !ok {
		return fmt.Errorf("pipeline definition %s is not registered", registryKey(run.PipelineName, run.PipelineVersion))
	}
	executions, err := e.store.ListRunExecutions(ctx, runID)
	if err != nil {
		return err
	}
	byNode := make(map[string]execution.Execution, len(executions))
	outputs := make(map[string]json.RawMessage)
	for _, item := range executions {
		byNode[item.NodeKey] = item
		if item.Status == execution.StatusSucceeded {
			outputs[item.NodeKey] = item.Output
		}
		if item.Status == execution.StatusFailed || item.Status == execution.StatusCancelled {
			return e.store.SetPipelineRunStatus(ctx, runID, execution.RunFailed, fmt.Sprintf("node %s: %s", item.NodeKey, item.LastError))
		}
	}
	created := false
	for _, node := range definition.nodes {
		if _, exists := byNode[node.key]; exists {
			continue
		}
		ready := true
		for _, dependency := range node.dependencies {
			if predecessor, ok := byNode[dependency]; !ok || predecessor.Status != execution.StatusSucceeded {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		input, buildErr := safeBuildInput(node, run.Input, outputs)
		if buildErr != nil {
			_ = e.store.SetPipelineRunStatus(ctx, runID, execution.RunFailed, fmt.Sprintf("build node %s input: %v", node.key, buildErr))
			return buildErr
		}
		_, err = e.store.CreateExecution(ctx, execution.CreateExecution{TaskName: node.taskName, TaskVersion: node.taskVersion, Input: input, MaxAttempts: node.maxAttempts, PipelineRunID: &runID, NodeKey: node.key, IdempotencyKey: runID.String() + ":" + node.key})
		if err != nil {
			return err
		}
		created = true
	}
	if len(definition.nodes) == 0 {
		return e.store.SetPipelineRunStatus(ctx, runID, execution.RunSucceeded, "")
	}
	if len(byNode) == len(definition.nodes) {
		allSucceeded := true
		for _, item := range byNode {
			if item.Status != execution.StatusSucceeded {
				allSucceeded = false
				break
			}
		}
		if allSucceeded {
			return e.store.SetPipelineRunStatus(ctx, runID, execution.RunSucceeded, "")
		}
	}
	if created || run.Status == execution.RunPending {
		return e.store.SetPipelineRunStatus(ctx, runID, execution.RunRunning, "")
	}
	return nil
}

func safeBuildInput(node *nodeDefinition, initial json.RawMessage, outputs map[string]json.RawMessage) (input json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("pipeline mapper panic: %v", recovered)
		}
	}()
	return node.buildInput(initial, outputs)
}

// Reconcile resumes pipeline runs left between durable task completion and DAG advancement.
func (e *Engine) Reconcile(ctx context.Context) error {
	runs, err := e.store.ListPipelineRuns(ctx, []execution.RunStatus{execution.RunPending, execution.RunRunning})
	if err != nil {
		return err
	}
	for _, run := range runs {
		if err = e.Advance(ctx, run.ID); err != nil {
			return err
		}
	}
	return nil
}

// Inspect returns the complete persisted state of a pipeline run.
func (e *Engine) Inspect(ctx context.Context, runID uuid.UUID) (Snapshot, error) {
	run, err := e.store.GetPipelineRun(ctx, runID)
	if err != nil {
		return Snapshot{}, err
	}
	items, err := e.store.ListRunExecutions(ctx, runID)
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Run: run, Executions: items}, nil
}

// Cancel marks a run cancelled and invalidates all unfinished node deliveries.
func (e *Engine) Cancel(ctx context.Context, runID uuid.UUID, reason string) error {
	items, err := e.store.ListRunExecutions(ctx, runID)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.Status == execution.StatusPending || item.Status == execution.StatusRetry || item.Status == execution.StatusRunning {
			if err = e.store.CancelExecution(ctx, item.ID, reason); err != nil {
				return err
			}
		}
	}
	return e.store.SetPipelineRunStatus(ctx, runID, execution.RunCancelled, reason)
}

// Output decodes one node's stored result.
func Output[O any](ctx context.Context, store execution.Store, runID uuid.UUID, node Node[O]) (O, error) {
	var zero O
	executions, err := store.ListRunExecutions(ctx, runID)
	if err != nil {
		return zero, err
	}
	for _, item := range executions {
		if item.NodeKey == node.key {
			if item.Status != execution.StatusSucceeded {
				return zero, fmt.Errorf("node %q is %s", node.key, item.Status)
			}
			if err = json.Unmarshal(item.Output, &zero); err != nil {
				return zero, err
			}
			return zero, nil
		}
	}
	return zero, execution.ErrNotFound
}
