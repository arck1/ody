package schedulor

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/go-co-op/gocron/v2"
	"go.uber.org/fx"
)

type LqSchedulerType interface {
	Init(lifecycle fx.Lifecycle)
}

type LqScheduler struct {
	db     DbConnector
	logger *zap.SugaredLogger
	// scheduler Запускает задачи с проверкой лидерсва
	scheduler gocron.Scheduler
	// localScheduler Запускает задачи на каждом инстансе вне зависимости от лидерства
	localScheduler gocron.Scheduler
	// queue Очередь для постановки задач на выполнение
	queue TasksQueue
	// settings Настройки
	options LqSchedulerOptions
}

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
	scheduler, err := gocron.NewScheduler(
		gocron.WithDistributedElector(NewPgLeaderElector(db, *settings.LqLeaderElectorOptions)),
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
		// Регистрируем задачу обновления задач из базы
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

// leaderHeartbeat Пустая задача, для поддержания актуального статуса лидера
func (s *LqScheduler) leaderHeartbeat(ctx context.Context) error {
	return nil
}

func (s *LqScheduler) refreshTasks(ctx context.Context) error {
	s.logger.Info("Refresh Tasks")
	db, err := s.db.GetConnect(ctx)
	if err != nil {
		return err
	}
	schedules, err := gorm.G[LqSchedule](db).Find(ctx)
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

func (s *LqScheduler) UpdateSchedule(item gocron.Job, schedule LqSchedule) error {
	if !schedule.IsActive {
		// Если job стал неактивным, то исключаем его
		err := s.scheduler.RemoveJob(item.ID())
		if err != nil {
			s.logger.Warnf("Failed to remove job %s", schedule.Id)
		}
		return nil
	}
	var update = false
	lastRun, err := item.LastRun()
	if err != nil {
		update = true
	} else if schedule.Updated.After(lastRun) {
		update = true
	}

	if update {
		// Обновляем job если он не запускался, либо после последнего запуска
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

func (s *LqScheduler) CreateSchedule(schedule LqSchedule) error {
	if !schedule.IsActive {
		return nil
	}
	// Обновляем job если он не запускался, либо после последнего запуска
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

func (s *LqScheduler) Run(ctx context.Context, schedule LqSchedule) (err error) {
	s.logger.Infof("Run Task %s", schedule.TaskName)
	var taskId *int64
	taskId, err = s.queue.Enqueue(
		ctx,
		schedule.TaskName,
		schedule.Payload,
		time.Now().UTC(),
		schedule.Id.String(),
	)

	if err == nil {
		updateErr := s.UpdateScheduleRun(ctx, schedule, taskId)
		if updateErr != nil {
			s.logger.Warnw("Failed to update job", "id", schedule.Id, "err", err)
		}
	}

	return
}

func (s *LqScheduler) GetJob(id uuid.UUID) gocron.Job {
	for _, job := range s.scheduler.Jobs() {
		if job.ID() == id {
			return job
		}
	}
	return nil
}

func (s *LqScheduler) UpdateScheduleRun(
	ctx context.Context,
	schedule LqSchedule,
	taskId *int64,
) error {
	db, err := s.db.GetConnect(ctx)
	if err != nil {
		s.logger.Warnf("Failed to get db connection for task %s", schedule.TaskName)
		// если задача выполнилась, но не получилось обновить информацию
		return nil
	}

	job := s.GetJob(schedule.Id)

	var nextRun *time.Time = nil

	if job != nil {
		var nextJobRun time.Time
		nextJobRun, err = job.NextRun()
		if err == nil {
			nextRun = &nextJobRun
		}
	}
	now := time.Now().UTC()
	_, err = gorm.G[LqSchedule](db).Where("id = ?", schedule.Id).Updates(ctx, LqSchedule{
		TaskID:  taskId,
		LastRun: &now,
		NextRun: nextRun,
	})

	if err != nil {
		return err
	}

	return nil
}
