package execution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreRejectsStaleLease(t *testing.T) {
	store := NewMemoryStore()
	created, err := store.CreateExecution(context.Background(), CreateExecution{TaskName: "work", TaskVersion: 1, Input: []byte(`{}`), MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(context.Background(), "worker-a", []TaskKey{{Name: "work", Version: 1}}, 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %+v %v", claimed, err)
	}
	if err = store.Succeed(context.Background(), created.ID, created.LeaseToken, []byte(`{}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected stale lease error, got %v", err)
	}
	if err = store.Succeed(context.Background(), created.ID, claimed[0].LeaseToken, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreReapsCrashedFinalAttempt(t *testing.T) {
	store := NewMemoryStore()
	store.now = func() time.Time { return time.Unix(100, 0).UTC() }
	created, _ := store.CreateExecution(context.Background(), CreateExecution{TaskName: "work", TaskVersion: 1, MaxAttempts: 1})
	_, _ = store.Claim(context.Background(), "worker", []TaskKey{{Name: "work", Version: 1}}, 1, time.Second)
	store.now = func() time.Time { return time.Unix(102, 0).UTC() }
	if _, err := store.ReapExpired(context.Background()); err != nil {
		t.Fatal(err)
	}
	item, _ := store.GetExecution(context.Background(), created.ID)
	if item.Status != StatusFailed {
		t.Fatalf("status = %s", item.Status)
	}
	events, _ := store.Events(context.Background(), created.ID)
	if len(events) != 3 || events[2].Type != EventFailed {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestMemoryStoreIdempotency(t *testing.T) {
	store := NewMemoryStore()
	request := CreateExecution{TaskName: "work", TaskVersion: 1, IdempotencyKey: "same"}
	first, _ := store.CreateExecution(context.Background(), request)
	second, _ := store.CreateExecution(context.Background(), request)
	if first.ID != second.ID {
		t.Fatalf("idempotency returned %s and %s", first.ID, second.ID)
	}
}

func TestMemoryStoreClaimsOnlyAdvertisedTaskVersions(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	oldVersion, err := store.CreateExecution(ctx, CreateExecution{TaskName: "work", TaskVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	newVersion, err := store.CreateExecution(ctx, CreateExecution{TaskName: "work", TaskVersion: 2})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := store.Claim(ctx, "old-worker", []TaskKey{{Name: "work", Version: 1}}, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != oldVersion.ID {
		t.Fatalf("claimed = %+v, want only %s", claimed, oldVersion.ID)
	}
	remaining, err := store.GetExecution(ctx, newVersion.ID)
	if err != nil {
		t.Fatal(err)
	}
	if remaining.Status != StatusPending {
		t.Fatalf("new version status = %s, want pending", remaining.Status)
	}
}

func TestMemoryStoreUsesStableCursorPagination(t *testing.T) {
	store := NewMemoryStore()
	store.now = func() time.Time { return time.Unix(100, 0).UTC() }
	ctx := context.Background()
	for range 3 {
		if _, err := store.CreateExecution(ctx, CreateExecution{TaskName: "page"}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.ListExecutions(ctx, ListFilter{TaskName: "page", Limit: 2})
	if err != nil || len(first) != 2 {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	cursor := Cursor{CreatedAt: first[1].CreatedAt, ID: first[1].ID}
	second, err := store.ListExecutions(ctx, ListFilter{TaskName: "page", Limit: 2, Before: &cursor})
	if err != nil || len(second) != 1 {
		t.Fatalf("second page = %+v, %v", second, err)
	}
	if second[0].ID == first[0].ID || second[0].ID == first[1].ID {
		t.Fatalf("pages overlap: %+v / %+v", first, second)
	}
}

func TestMemoryStorePurgesOnlyTerminalHistory(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0).UTC()
	store.now = func() time.Time { return now }
	ctx := context.Background()
	terminal, _ := store.CreateExecution(ctx, CreateExecution{TaskName: "old", TaskVersion: 1})
	claimed, _ := store.Claim(ctx, "worker", []TaskKey{{Name: "old", Version: 1}}, 1, time.Minute)
	if err := store.Succeed(ctx, terminal.ID, claimed[0].LeaseToken, nil); err != nil {
		t.Fatal(err)
	}
	active, _ := store.CreateExecution(ctx, CreateExecution{TaskName: "active", TaskVersion: 1})
	run, _ := store.CreatePipelineRun(ctx, CreatePipelineRun{PipelineName: "old-flow", PipelineVersion: 1})
	node, _ := store.CreateExecution(ctx, CreateExecution{TaskName: "node", TaskVersion: 1, PipelineRunID: &run.ID, NodeKey: "node"})
	claimed, _ = store.Claim(ctx, "worker", []TaskKey{{Name: "node", Version: 1}}, 1, time.Minute)
	if err := store.Succeed(ctx, node.ID, claimed[0].LeaseToken, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.SetPipelineRunStatus(ctx, run.ID, RunSucceeded, ""); err != nil {
		t.Fatal(err)
	}

	result, err := store.Purge(ctx, now.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Executions != 2 || result.PipelineRuns != 1 {
		t.Fatalf("purge result = %+v", result)
	}
	if _, err = store.GetExecution(ctx, active.ID); err != nil {
		t.Fatalf("active execution was purged: %v", err)
	}
	if _, err = store.GetExecution(ctx, terminal.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("terminal execution still exists: %v", err)
	}
}
