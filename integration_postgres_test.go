//go:build integration

package schedulor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"schedulor/elector"
	"schedulor/execution"
	executionpostgres "schedulor/execution/postgres"
	"schedulor/pipeline"
	"schedulor/queue"
	taskv2 "schedulor/task"
	workerv2 "schedulor/worker"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type sqlConnector struct {
	db *sql.DB
}

func (c sqlConnector) GetConnect(ctx context.Context) (*sql.DB, error) {
	return c.db, nil
}

func TestPostgresQueueLifecycleIntegration(t *testing.T) {
	t.Parallel()

	db := setupIntegrationDB(t)
	q := queue.NewPostgresQueue(sqlConnector{db: db}, queue.PostgresQueueOptions{
		TaskMaxAttempts: 2,
		TaskVisibility:  500 * time.Millisecond,
	})

	ctx := context.Background()
	payload, err := queue.NewJSONPayload(map[string]any{"kind": "email", "to": "a@b.c"})
	if err != nil {
		t.Fatalf("payload error: %v", err)
	}
	firstID, err := q.Enqueue(ctx, "mail", payload, time.Now().UTC(), "idem-1")
	if err != nil {
		t.Fatalf("enqueue error: %v", err)
	}
	secondID, err := q.Enqueue(ctx, "mail", payload, time.Now().UTC(), "idem-1")
	if err != nil {
		t.Fatalf("idempotent enqueue error: %v", err)
	}
	if *firstID != *secondID {
		t.Fatalf("expected same task id for idempotency key, got %d and %d", *firstID, *secondID)
	}

	claimed, err := q.Claim(ctx, []string{"mail"}, 1)
	if err != nil {
		t.Fatalf("claim error: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed task, got %d", len(claimed))
	}
	if claimed[0].Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", claimed[0].Attempts)
	}

	ok, err := q.Nack(ctx, claimed[0].TaskID, claimed[0].LeaseToken, "temporary", 0)
	if err != nil || !ok {
		t.Fatalf("nack failed: ok=%v err=%v", ok, err)
	}

	claimed2 := eventuallyClaimOne(t, q, []string{"mail"}, 1*time.Second)
	if claimed2.Attempts != 2 {
		t.Fatalf("expected attempts=2, got %d", claimed2.Attempts)
	}

	moved, err := q.MoveToDLQ(ctx, claimed2.TaskID, claimed2.LeaseToken, "attempts exhausted")
	if err != nil {
		t.Fatalf("move to dlq error: %v", err)
	}
	if !moved {
		t.Fatalf("expected task moved to dlq")
	}

	var liveCount int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM lq_tasks`).Scan(&liveCount); err != nil {
		t.Fatalf("count tasks error: %v", err)
	}
	if liveCount != 0 {
		t.Fatalf("expected no tasks left, got %d", liveCount)
	}

	var dlqCount int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM lq_tasks_dlq`).Scan(&dlqCount); err != nil {
		t.Fatalf("count dlq error: %v", err)
	}
	if dlqCount != 1 {
		t.Fatalf("expected 1 task in dlq, got %d", dlqCount)
	}
}

func TestExecutorWithPostgresQueueIntegration(t *testing.T) {
	t.Parallel()

	db := setupIntegrationDB(t)
	q := queue.NewPostgresQueue(sqlConnector{db: db}, queue.PostgresQueueOptions{
		TaskMaxAttempts: 3,
		TaskVisibility:  500 * time.Millisecond,
	})

	var handled atomic.Int32
	exec, err := NewLqExecutor(
		testLogger(),
		q,
		NewCodeTaskExecutor([]TaskHandler{{
			TaskName: "mail",
			Handler: func(ctx context.Context, payload map[string]any) error {
				handled.Add(1)
				return nil
			},
		}}),
		&LqExecutorOptions{PoolingTimeout: 10 * time.Millisecond, PoolingBatch: 1},
	)
	if err != nil {
		t.Fatalf("NewLqExecutor error: %v", err)
	}

	payload, err := queue.NewJSONPayload(map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("payload error: %v", err)
	}
	if _, err = q.Enqueue(context.Background(), "mail", payload, time.Now().UTC(), "exec-1"); err != nil {
		t.Fatalf("enqueue error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go exec.Run(ctx)

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if handled.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if handled.Load() == 0 {
		t.Fatalf("executor did not process task")
	}

	var cnt int
	if err = db.QueryRowContext(context.Background(), `SELECT count(*) FROM lq_tasks`).Scan(&cnt); err != nil {
		t.Fatalf("count tasks error: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected queue to be empty after ack, got %d", cnt)
	}
}

func TestPgLeaderElectorIntegration(t *testing.T) {
	t.Parallel()

	db := setupIntegrationDB(t)
	ctx := context.Background()

	e1 := elector.NewPgLeaderElector(sqlConnector{db: db}, elector.Options{
		LeaderKey: "test_leader",
		LeaderId:  "node-1",
		LeaderTTL: 300 * time.Millisecond,
	})
	e2 := elector.NewPgLeaderElector(sqlConnector{db: db}, elector.Options{
		LeaderKey: "test_leader",
		LeaderId:  "node-2",
		LeaderTTL: 300 * time.Millisecond,
	})

	if err := e1.IsLeader(ctx); err != nil {
		t.Fatalf("node-1 should become leader: %v", err)
	}
	if err := e2.IsLeader(ctx); err == nil {
		t.Fatalf("node-2 must not become leader while ttl is valid")
	}

	time.Sleep(350 * time.Millisecond)
	if err := e2.IsLeader(ctx); err != nil {
		t.Fatalf("node-2 should become leader after ttl expiry: %v", err)
	}
}

func TestPersistentPipelineIntegration(t *testing.T) {
	t.Parallel()
	db := setupIntegrationDB(t)
	store, err := executionpostgres.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	double := taskv2.New[int, int]("pipeline.double")
	stringify := taskv2.New[int, string]("pipeline.stringify")
	module, err := taskv2.NewModule("pipeline",
		taskv2.Handle(double, func(_ context.Context, message taskv2.Message[int]) (int, error) { return message.Input * 2, nil }),
		taskv2.Handle(stringify, func(_ context.Context, message taskv2.Message[int]) (string, error) {
			return fmt.Sprintf("result:%d", message.Input), nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := taskv2.NewRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	flow := pipeline.New[int]("persistent-flow", 1)
	first := pipeline.Start(flow, "double", double, func(input int) int { return input })
	last := pipeline.Then(flow, first, "stringify", stringify, func(output int) int { return output })
	pipelines, err := pipeline.NewRegistry(flow)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := pipeline.NewEngine(store, pipelines)
	if err != nil {
		t.Fatal(err)
	}
	run, err := pipeline.Run(context.Background(), engine, flow, 21)
	if err != nil {
		t.Fatal(err)
	}
	w, err := workerv2.New(store, tasks, engine, nil, workerv2.Options{Concurrency: 2, PollInterval: 5 * time.Millisecond, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, getErr := store.GetPipelineRun(context.Background(), run.ID)
		if getErr == nil && current.Status == execution.RunSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	current, err := store.GetPipelineRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != execution.RunSucceeded {
		t.Fatalf("pipeline status: %s (%s)", current.Status, current.Error)
	}
	result, err := pipeline.Output(context.Background(), store, run.ID, last)
	if err != nil {
		t.Fatal(err)
	}
	if result != "result:42" {
		t.Fatalf("result = %q", result)
	}
	executions, err := store.ListRunExecutions(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var lastExecution execution.Execution
	for _, item := range executions {
		if item.NodeKey == "stringify" {
			lastExecution = item
		}
	}
	restarted, err := store.RestartExecution(context.Background(), lastExecution.ID, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Status != execution.StatusPending || restarted.Attempt != 0 || restarted.Output != nil {
		t.Fatalf("unexpected restarted execution: %+v", restarted)
	}
	current, err = store.GetPipelineRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != execution.RunRunning {
		t.Fatalf("pipeline was not reopened: %s", current.Status)
	}
}

func eventuallyClaimOne(t *testing.T, q *queue.PostgresQueue, tasks []string, timeout time.Duration) queue.Claimed {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		claimed, err := q.Claim(context.Background(), tasks, 1)
		if err != nil {
			t.Fatalf("claim error: %v", err)
		}
		if len(claimed) == 1 {
			return claimed[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for claim")
	return queue.Claimed{}
}

func setupIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	container, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("schedulor_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp").WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Skipf("skipping integration test: postgres container is unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string error: %v", err)
	}

	db, err := sql.Open("pgx", connString)
	if err != nil {
		t.Fatalf("sql open error: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if err = waitForDB(ctx, db, 10*time.Second); err != nil {
		t.Fatalf("db not ready: %v", err)
	}

	schemaPath := findSchemaPath(t)
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema error: %v", err)
	}
	if _, err = db.ExecContext(ctx, string(schema)); err != nil {
		t.Fatalf("apply schema error: %v", err)
	}
	return db
}

func waitForDB(ctx context.Context, db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func findSchemaPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("runtime caller failed")
	}
	root := filepath.Dir(thisFile)
	path := filepath.Join(root, "schema.sql")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("schema file not found at %s: %v", path, err)
	}
	return path
}
