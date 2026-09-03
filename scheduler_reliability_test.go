package schedulor

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

func TestSchedulerRemovesDeletedDBSchedulesOnly(t *testing.T) {
	gocronScheduler, err := gocron.NewScheduler()
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	t.Cleanup(func() { _ = gocronScheduler.Shutdown() })

	scheduler := &LqScheduler{
		logger:       testLogger(),
		scheduler:    gocronScheduler,
		scheduleJobs: make(map[uuid.UUID]time.Time),
	}
	scheduleID := uuid.New()
	if err = scheduler.CreateSchedule(LqSchedule{
		Id: scheduleID, TaskName: "work", Cron: "* * * * *", IsActive: true,
	}); err != nil {
		t.Fatalf("create DB schedule: %v", err)
	}
	internal, err := scheduler.AddJob(
		gocron.CronJob("* * * * *", false),
		gocron.NewTask(func() {}),
	)
	if err != nil {
		t.Fatalf("create internal job: %v", err)
	}

	scheduler.removeMissingSchedules(map[uuid.UUID]struct{}{})
	if scheduler.GetJob(scheduleID) != nil {
		t.Fatal("deleted DB schedule is still registered")
	}
	if scheduler.GetJob(internal.ID()) == nil {
		t.Fatal("non-DB job was removed during schedule refresh")
	}
}

func TestCreateScheduleReturnsCronValidationError(t *testing.T) {
	gocronScheduler, err := gocron.NewScheduler()
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	t.Cleanup(func() { _ = gocronScheduler.Shutdown() })
	scheduler := &LqScheduler{
		logger:       testLogger(),
		scheduler:    gocronScheduler,
		scheduleJobs: make(map[uuid.UUID]time.Time),
	}

	err = scheduler.CreateSchedule(LqSchedule{
		Id: uuid.New(), TaskName: "work", Cron: "not-a-cron", IsActive: true,
	})
	if err == nil {
		t.Fatal("expected invalid cron expression to be returned")
	}
}

func TestUpdateScheduleUsesConfigurationVersionNotLastRun(t *testing.T) {
	gocronScheduler, err := gocron.NewScheduler()
	if err != nil {
		t.Fatalf("new scheduler: %v", err)
	}
	t.Cleanup(func() { _ = gocronScheduler.Shutdown() })
	scheduler := &LqScheduler{
		logger:       testLogger(),
		scheduler:    gocronScheduler,
		scheduleJobs: make(map[uuid.UUID]time.Time),
	}
	id := uuid.New()
	loadedVersion := time.Now().Add(-time.Minute)
	dbVersion := time.Now()
	job, err := scheduler.AddJob(
		gocron.DurationJob(time.Millisecond),
		gocron.NewTask(func() {}),
		gocron.WithIdentifier(id),
	)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	scheduler.scheduleJobs[id] = loadedVersion
	gocronScheduler.Start()
	eventually(t, time.Second, func() bool {
		lastRun, lastRunErr := job.LastRun()
		return lastRunErr == nil && lastRun.After(dbVersion)
	})

	err = scheduler.UpdateSchedule(job, LqSchedule{
		Id: id, TaskName: "work", Cron: "*/5 * * * *", IsActive: true, Updated: dbVersion,
	})
	if err != nil {
		t.Fatalf("update schedule: %v", err)
	}
	if got := scheduler.scheduleJobs[id]; !got.Equal(dbVersion) {
		t.Fatalf("configuration version was not updated: got %v, want %v", got, dbVersion)
	}
}

func TestUpdateScheduleRunReturnsConnectionError(t *testing.T) {
	want := errors.New("database unavailable")
	scheduler := &LqScheduler{db: failingDBConnector{err: want}, logger: testLogger()}
	err := scheduler.UpdateScheduleRun(context.Background(), LqSchedule{TaskName: "work"}, nil)
	if !errors.Is(err, want) {
		t.Fatalf("expected connection error, got %v", err)
	}
}

type failingDBConnector struct{ err error }

func (c failingDBConnector) GetConnect(context.Context) (*sql.DB, error) {
	return nil, c.err
}
