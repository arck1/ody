package schedulor

import (
	"fmt"
	"log"
	"os"
	"schedulor/elector"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

type LqPostgresQueueOptions struct {
	// TaskMaxAttempts Максимальное количество попыток обработать задачу
	TaskMaxAttempts int `env:"local_queue.postgres_queue.task_max_attempts"`
	// TaskVisibility Время на которое блокируется задача для обработки
	TaskVisibility time.Duration `env:"local_queue.postgres_queue.task_visibility"`
}

type LqSchedulerOptions struct {
	// TasksRefreshEnabled Включает обновление задач из базы данных
	TasksRefreshEnabled bool `env:"local_queue.scheduler.tasks_refresh_enabled"`
	// TasksRefreshTimeout Таймаут обновления задач из базы данных, если включен TasksRefreshEnabled
	TasksRefreshTimeout time.Duration `env:"local_queue.scheduler.tasks_refresh_timeout"`
	// LeaderElector Опциональный кастомный elector для алгоритма выбора лидера
	LeaderElector elector.LeaderElector `env:"-"`
}

type LqLeaderElectorOptions struct {
	// LeaderKey Ключ группы для выбора лидера
	LeaderKey string `env:"local_queue.leader_elector.leader_key"`
	// LeaderId Id лидера (default: $hostname)
	LeaderId string `env:"local_queue.leader_elector.leader_id"`
	// LeaderTTL Время жизни лидера
	LeaderTTL time.Duration `env:"local_queue.leader_elector.leader_ttl"`
	// Включение heartbeat для поддержания статуса лидера
	LeaderHeartbeatEnabled bool `env:"local_queue.leader_elector.leader_heartbeat_enabled"`
}

type LqExecutorOptions struct {
	// Время ожидания между проверками наличия задач
	PoolingTimeout time.Duration `env:"local_queue.executor.pooling_timeout"`
	// Количество задач блокируемые одновременно для исполнения
	PoolingBatch int `env:"local_queue.executor.pooling_batch"`
}
type LqSettings struct {
	*LqPostgresQueueOptions
	*LqSchedulerOptions
	*LqLeaderElectorOptions
	*LqExecutorOptions
}

// getLeaderId
// Генерирует уникальный LeaderId в рамках хоста, если доступно
func getLeaderId(uniqOnHost bool) string {
	hostname, err := os.Hostname()
	if err != nil {
		return uuid.NewString()
	}
	if uniqOnHost {
		return fmt.Sprintf("%s_%s", hostname, uuid.NewString())
	}
	return hostname
}

var defaultSettings = LqSettings{
	LqPostgresQueueOptions: &LqPostgresQueueOptions{
		TaskMaxAttempts: 25,
		TaskVisibility:  60 * time.Second,
	},
	LqSchedulerOptions: &LqSchedulerOptions{
		TasksRefreshEnabled: true,
		TasksRefreshTimeout: 30 * time.Minute,
	},
	LqLeaderElectorOptions: &LqLeaderElectorOptions{
		LeaderKey:              "lq_leader",
		LeaderId:               getLeaderId(false),
		LeaderTTL:              5 * time.Minute,
		LeaderHeartbeatEnabled: true,
	},
	LqExecutorOptions: &LqExecutorOptions{
		PoolingTimeout: 30 * time.Second,
		PoolingBatch:   1,
	},
}

func init() {
	loadSettingsFromEnv()
}

func loadSettingsFromEnv() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Printf("Error loading .env file: %v", err)
	}
	if err = env.Parse(&defaultSettings); err != nil {
		log.Fatalf("Error reading the environment variables: %v", err)
	}
}

func GetSettings(options *LqSettings) LqSettings {
	settings := mergeOptionsWithDefault(options)
	return settings
}

func mergeOptionsWithDefault(options *LqSettings) LqSettings {
	if options == nil {
		return defaultSettings
	}
	if options.LqPostgresQueueOptions == nil {
		options.LqPostgresQueueOptions = defaultSettings.LqPostgresQueueOptions
	} else {
		if options.TaskMaxAttempts == 0 {
			options.TaskMaxAttempts = defaultSettings.TaskMaxAttempts
		}
		if options.TaskVisibility == 0 {
			options.TaskVisibility = defaultSettings.TaskVisibility
		}
	}

	if options.LqLeaderElectorOptions == nil {
		options.LqLeaderElectorOptions = defaultSettings.LqLeaderElectorOptions
	} else {
		if options.LeaderKey == "" {
			options.LeaderKey = defaultSettings.LeaderKey
		}
		if options.LeaderId == "" {
			options.LeaderId = defaultSettings.LeaderId
		}
		if options.LeaderTTL == 0 {
			options.LeaderTTL = defaultSettings.LeaderTTL
		}
	}
	if options.LqSchedulerOptions == nil {
		options.LqSchedulerOptions = defaultSettings.LqSchedulerOptions
	} else if options.TasksRefreshTimeout == 0 {
		options.TasksRefreshTimeout = defaultSettings.TasksRefreshTimeout
	}
	if options.LqExecutorOptions == nil {
		options.LqExecutorOptions = defaultSettings.LqExecutorOptions
	} else {
		if options.PoolingTimeout == 0 {
			options.PoolingTimeout = defaultSettings.PoolingTimeout
		}
		if options.PoolingBatch == 0 {
			options.PoolingBatch = defaultSettings.PoolingBatch
		}
	}
	return *options
}
