package schedulor

import (
	"context"
	"database/sql"
	"fmt"
	"schedulor/elector"
	"schedulor/queue"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

// LqSchedulerType describes scheduler lifecycle integration contract.
type LqSchedulerType interface {
	// Init registers scheduler lifecycle hooks in fx.
	Init(lifecycle fx.Lifecycle)
}

// LqScheduler manages cron schedules and enqueues runtime tasks.
type LqScheduler struct {
	// db provides SQL connections for reading/updating schedules.
	db DbConnector
	// logger writes scheduler events and warnings.
	logger *zap.SugaredLogger
	// scheduler runs leader-only jobs.
	scheduler gocron.Scheduler
	// localScheduler runs jobs on every instance regardless of leadership.
	localScheduler gocron.Scheduler
	// queue is used to enqueue executable tasks.
	queue queue.TasksQueue
	// options stores resolved scheduler options.
	options LqSchedulerOptions
}

// NewLqScheduler creates scheduler instances and registers internal maintenance jobs.
func NewLqScheduler(
	db DbConnector,
	logger *zap.SugaredLogger,
	executor *LqExecutor,
	options *LqSchedulerOptions,
) *LqScheduler {
	settings := GetSettings(&LqSettings{
		LqExecutorOptions:  &(executor.options),
		LqSchedulerOptions: options,
	})
	localScheduler, err := gocron.NewScheduler(
		gocron.WithLogger(LqLogger{logger}),
		gocron.WithLocation(time.Local),
	)
	if err != nil {
		panic(err)
	}
	leaderElector := settings.LqSchedulerOptions.LeaderElector
	if leaderElector == nil {
		leaderElector = elector.NewPgLeaderElector(db, elector.Options{
			LeaderKey: settings.LeaderKey,
			LeaderId:  settings.LeaderId,
			LeaderTTL: settings.LeaderTTL,
		})
	}
	scheduler, err := gocron.NewScheduler(
		gocron.WithDistributedElector(leaderElector),
		gocron.WithLogger(LqLogger{logger}),
		gocron.WithLocation(time.Local),
	)
	if err != nil {
		panic(err)
	}
	sch := &LqScheduler{
		db:             db,
		logger:         logger,
		scheduler:      scheduler,
		localScheduler: localScheduler,
		queue:          executor.GetQueue(),
		options:        *settings.LqSchedulerOptions,
	}

	if settings.LeaderHeartbeatEnabled {
		_, err = sch.AddJob(
			gocron.DurationJob(settings.LeaderTTL>>1),
			gocron.NewTask(sch.leaderHeartbeat),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			logger.Fatalw("failed to init leader heartbeat task", "err", err)
		}
	}

	if settings.TasksRefreshEnabled {
		ctx := context.Background()
		err = sch.refreshTasks(ctx)
		if err != nil {
			logger.Fatalw("failed to init refresh tasks", "err", err)
		}
		_, err := sch.AddLocalJob(
			gocron.DurationJob(settings.TasksRefreshTimeout),
			gocron.NewTask(sch.refreshTasks),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		)
		if err != nil {
			panic(err)
		}
	}

	return sch
}

// AddJob registers a leader-aware scheduled job.
func (s *LqScheduler) AddJob(
	definition gocron.JobDefinition,
	task gocron.Task,
	options ...gocron.JobOption,
) (gocron.Job, error) {
	return s.scheduler.NewJob(
		definition,
		task,
		options...,
	)
}

// AddLocalJob registers a schedule that runs on every instance.
func (s *LqScheduler) AddLocalJob(
	definition gocron.JobDefinition,
	task gocron.Task,
	options ...gocron.JobOption,
) (gocron.Job, error) {
	return s.localScheduler.NewJob(
		definition,
		task,
		options...,
	)
}

// Init registers scheduler start/stop hooks in fx lifecycle.
func (s *LqScheduler) Init(lifecycle fx.Lifecycle) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			s.scheduler.Start()
			s.localScheduler.Start()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			err := s.localScheduler.Shutdown()
			if err != nil {
				s.logger.Warn("Failed to shutdown local scheduler")
			}
			return s.scheduler.Shutdown()
		},
	})
}

// leaderHeartbeat is a no-op task to keep leader lease active.
func (s *LqScheduler) leaderHeartbeat(ctx context.Context) error {
	return nil
}

// refreshTasks syncs in-memory cron jobs with DB schedules.
func (s *LqScheduler) refreshTasks(ctx context.Context) error {
	s.logger.Info("Refresh Tasks")
	db, err := s.db.GetConnect(ctx)
	if err != nil {
		return err
	}
	schedules, err := loadSchedules(ctx, db)
	if err != nil {
		return err
	}
	currentJobs := lo.Associate(s.scheduler.Jobs(), func(item gocron.Job) (uuid.UUID, gocron.Job) {
		return item.ID(), item
	})
	for _, schedule := range schedules {
		if item, ok := currentJobs[schedule.Id]; ok {
			err = s.UpdateSchedule(item, schedule)
			if err != nil {
				s.logger.Warnw("Failed to update job", "id", schedule.Id, "err", err)
			}
		} else {
			err = s.CreateSchedule(schedule)
			if err != nil {
				s.logger.Warnw("Failed to create job", "id", schedule.Id, "err", err)
			}
		}
	}
	return nil
}

// loadSchedules fetches all schedule records from DB.
func loadSchedules(ctx context.Context, db *sql.DB) ([]LqSchedule, error) {
	rows, err := db.QueryContext(
		ctx,
		`SELECT id, task_name, cron, payload, is_active, next_run, last_run, task_id, updated, description
		 FROM lq_schedules`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schedules := make([]LqSchedule, 0)
	for rows.Next() {
		schedule, scanErr := scanSchedule(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		schedules = append(schedules, schedule)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

// scanSchedule converts a DB row into LqSchedule model.
func scanSchedule(rows *sql.Rows) (LqSchedule, error) {
	var (
		idRaw      string
		payloadRaw []byte
		nextRunRaw sql.NullTime
		lastRunRaw sql.NullTime
		taskIDRaw  sql.NullInt64
		descRaw    sql.NullString
		schedule   LqSchedule
	)
	if err := rows.Scan(
		&idRaw,
		&schedule.TaskName,
		&schedule.Cron,
		&payloadRaw,
		&schedule.IsActive,
		&nextRunRaw,
		&lastRunRaw,
		&taskIDRaw,
		&schedule.Updated,
		&descRaw,
	); err != nil {
		return LqSchedule{}, err
	}

	id, err := uuid.Parse(idRaw)
	if err != nil {
		return LqSchedule{}, fmt.Errorf("invalid schedule id %q: %w", idRaw, err)
	}
	schedule.Id = id
	schedule.Payload = queue.JSONPayload(payloadRaw)
	if nextRunRaw.Valid {
		nextRun := nextRunRaw.Time
		schedule.NextRun = &nextRun
	}
	if lastRunRaw.Valid {
		lastRun := lastRunRaw.Time
		schedule.LastRun = &lastRun
	}
	if taskIDRaw.Valid {
		taskID := taskIDRaw.Int64
		schedule.TaskID = &taskID
	}
	if descRaw.Valid {
		desc := descRaw.String
		schedule.Description = &desc
	}

	return schedule, nil
}

// UpdateSchedule updates existing in-memory job when DB row changed.
func (s *LqScheduler) UpdateSchedule(item gocron.Job, schedule LqSchedule) error {
	if !schedule.IsActive {
		err := s.scheduler.RemoveJob(item.ID())
		if err != nil {
			s.logger.Warnf("Failed to remove job %s", schedule.Id)
		}
		return nil
	}
	update := false
	lastRun, err := item.LastRun()
	if err != nil {
		update = true
	} else if schedule.Updated.After(lastRun) {
		update = true
	}

	if update {
		_, err = s.scheduler.Update(
			item.ID(),
			gocron.CronJob(schedule.Cron, false),
			gocron.NewTask(s.Run, schedule),
			gocron.WithIdentifier(schedule.Id),
		)
		if err != nil {
			s.logger.Warnf("Failed to update job %s", schedule.Id)
		}
	}
	return nil
}

// CreateSchedule creates a new in-memory job for active DB schedule.
func (s *LqScheduler) CreateSchedule(schedule LqSchedule) error {
	if !schedule.IsActive {
		return nil
	}
	_, err := s.AddJob(
		gocron.CronJob(schedule.Cron, false),
		gocron.NewTask(s.Run, schedule),
		gocron.WithIdentifier(schedule.Id),
	)
	if err != nil {
		s.logger.Warnf("Failed to update job %s", schedule.Id)
	}
	return nil
}

// Run enqueues a runtime task for schedule execution and updates run metadata.
func (s *LqScheduler) Run(ctx context.Context, schedule LqSchedule) (err error) {
	s.logger.Infof("Run Task %s", schedule.TaskName)
	var taskID *int64
	taskID, err = s.queue.Enqueue(
		ctx,
		schedule.TaskName,
		schedule.Payload,
		time.Now().UTC(),
		schedule.Id.String(),
	)

	if err == nil {
		updateErr := s.UpdateScheduleRun(ctx, schedule, taskID)
		if updateErr != nil {
			s.logger.Warnw("Failed to update job", "id", schedule.Id, "err", err)
		}
	}

	return
}

// GetJob returns currently registered job by schedule id.
func (s *LqScheduler) GetJob(id uuid.UUID) gocron.Job {
	for _, job := range s.scheduler.Jobs() {
		if job.ID() == id {
			return job
		}
	}
	return nil
}

// UpdateScheduleRun persists last/next run information in DB.
func (s *LqScheduler) UpdateScheduleRun(
	ctx context.Context,
	schedule LqSchedule,
	taskID *int64,
) error {
	db, err := s.db.GetConnect(ctx)
	if err != nil {
		s.logger.Warnf("Failed to get db connection for task %s", schedule.TaskName)
		return nil
	}

	job := s.GetJob(schedule.Id)
	var nextRun *time.Time
	if job != nil {
		nextJobRun, nextErr := job.NextRun()
		if nextErr == nil {
			nextRun = &nextJobRun
		}
	}

	now := time.Now().UTC()
	_, err = db.ExecContext(
		ctx,
		`UPDATE lq_schedules SET task_id = $1, last_run = $2, next_run = $3 WHERE id = $4`,
		taskID,
		now,
		nextRun,
		schedule.Id.String(),
	)
	if err != nil {
		return err
	}

	return nil
}
