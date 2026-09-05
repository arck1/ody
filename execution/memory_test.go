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
