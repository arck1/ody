package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"schedulor"
	"schedulor/queue"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type sqlConnector struct {
	db *sql.DB
}

func (c sqlConnector) GetConnect(ctx context.Context) (*sql.DB, error) {
	return c.db, nil
}

type cliConfig struct {
	queueBackend      string
	dbDSN             string
	redisURL          string
	redisPrefix       string
	executorType      string
	bashCommandsFile  string
	executorTaskNames string
	commandField      string
	logLevel          string
	withScheduler     bool
}

type backendRuntime struct {
	backend queue.Backend
	db      schedulor.DbConnector
	close   func() error
}

type backendFactory interface {
	Build(ctx context.Context, cfg cliConfig, settings schedulor.LqSettings, logger *zap.SugaredLogger) (*backendRuntime, error)
}

func main() {
	cfg := parseConfig()
	logger := newLogger(cfg.logLevel)
	defer func() { _ = logger.Sync() }()
	sugared := logger.Sugar()

	if err := schedulor.LoadSettingsFromEnv(); err != nil {
		sugared.Fatalw("failed to load settings from env", "err", err)
	}
	settings := schedulor.GetSettings(nil)
	runtime, err := buildBackendRuntime(context.Background(), cfg, settings, sugared)
	if err != nil {
		sugared.Fatalw("failed to build queue backend", "backend", cfg.queueBackend, "err", err)
	}
	if runtime.close != nil {
		defer func() {
			if closeErr := runtime.close(); closeErr != nil {
				sugared.Errorw("failed to close queue backend", "err", closeErr)
			}
		}()
	}

	taskExec := buildTaskExecutor(cfg, sugared)
	libraryLogger := schedulor.NewZapLogger(sugared)
	lqExecutor, err := schedulor.NewLqExecutor(libraryLogger, runtime.backend, taskExec, settings.LqExecutorOptions)
	if err != nil {
		sugared.Fatalw("failed to create executor", "err", err)
	}
	components := []schedulor.FxLifecycleComponent{lqExecutor}

	if cfg.withScheduler {
		if runtime.db == nil {
			sugared.Fatalw("selected backend does not provide db connector required by scheduler", "backend", cfg.queueBackend)
		}
		lqScheduler, schedulerErr := schedulor.NewLqScheduler(runtime.db, libraryLogger, lqExecutor, settings.LqSchedulerOptions)
		if schedulerErr != nil {
			sugared.Fatalw("failed to create scheduler", "err", schedulerErr)
		}
		components = append(components, lqScheduler)
	}

	app, err := schedulor.NewFxApp(schedulor.FxAppOptions{Components: components})
	if err != nil {
		sugared.Fatalw("failed to create fx app", "err", err)
	}
	app.Run()
}

func parseConfig() cliConfig {
	cfg := cliConfig{}
	flag.StringVar(&cfg.queueBackend, "queue-backend", envOrDefault("SCHEDULOR_QUEUE_BACKEND", "postgres"), "Queue backend: postgres|redis|kafka|noop")
	flag.StringVar(&cfg.dbDSN, "db-dsn", envOrDefault("SCHEDULOR_DB_DSN", ""), "PostgreSQL DSN (required for postgres backend)")
	flag.StringVar(&cfg.redisURL, "redis-url", envOrDefault("SCHEDULOR_REDIS_URL", "redis://localhost:6379/0"), "Redis URL (required for redis backend)")
	flag.StringVar(&cfg.redisPrefix, "redis-prefix", envOrDefault("SCHEDULOR_REDIS_PREFIX", "schedulor:{queue}:"), "Redis key prefix")
	flag.StringVar(&cfg.executorType, "executor", envOrDefault("SCHEDULOR_EXECUTOR", "bash"), "Executor type: bash")
	flag.StringVar(&cfg.bashCommandsFile, "bash-commands-file", envOrDefault("SCHEDULOR_BASH_COMMANDS_FILE", ""), "Path to JSON file with task commands")
	flag.StringVar(&cfg.executorTaskNames, "executor-task-names", envOrDefault("SCHEDULOR_EXECUTOR_TASK_NAMES", "bash"), "Comma-separated task names for payload command mode")
	flag.StringVar(&cfg.commandField, "executor-command-field", envOrDefault("SCHEDULOR_EXECUTOR_COMMAND_FIELD", "command"), "Payload field with command for payload mode")
	flag.StringVar(&cfg.logLevel, "log-level", envOrDefault("SCHEDULOR_LOG_LEVEL", "info"), "Logger level: debug|info|warn|error")
	flag.BoolVar(&cfg.withScheduler, "with-scheduler", envBoolOrDefault("SCHEDULOR_WITH_SCHEDULER", true), "Run scheduler component")
	flag.Parse()
	return cfg
}

func buildBackendRuntime(
	ctx context.Context,
	cfg cliConfig,
	settings schedulor.LqSettings,
	logger *zap.SugaredLogger,
) (*backendRuntime, error) {
	factories := map[string]backendFactory{
		"postgres": postgresBackendFactory{},
		"redis":    redisBackendFactory{},
		"kafka":    kafkaBackendFactory{},
		"noop":     noopBackendFactory{},
	}
	factory, ok := factories[cfg.queueBackend]
	if !ok {
		return nil, fmt.Errorf("unsupported queue backend %q", cfg.queueBackend)
	}
	return factory.Build(ctx, cfg, settings, logger)
}

type redisBackendFactory struct{}

func (redisBackendFactory) Build(
	ctx context.Context,
	cfg cliConfig,
	settings schedulor.LqSettings,
	logger *zap.SugaredLogger,
) (*backendRuntime, error) {
	options, err := redis.ParseURL(cfg.redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(options)
	if err = client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	backend, err := queue.NewRedisQueue(client, queue.RedisQueueOptions{
		Prefix:          cfg.redisPrefix,
		TaskMaxAttempts: settings.TaskMaxAttempts,
		TaskVisibility:  settings.TaskVisibility,
	})
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	logger.Infow("initialized queue backend", "backend", "redis", "prefix", cfg.redisPrefix)
	return &backendRuntime{backend: backend, close: client.Close}, nil
}

type kafkaBackendFactory struct{}

func (kafkaBackendFactory) Build(
	ctx context.Context,
	cfg cliConfig,
	settings schedulor.LqSettings,
	logger *zap.SugaredLogger,
) (*backendRuntime, error) {
	logger.Infow("initialized queue backend", "backend", "kafka")
	return &backendRuntime{backend: queue.NewKafkaQueue()}, nil
}

type noopBackendFactory struct{}

func (noopBackendFactory) Build(
	ctx context.Context,
	cfg cliConfig,
	settings schedulor.LqSettings,
	logger *zap.SugaredLogger,
) (*backendRuntime, error) {
	logger.Infow("initialized queue backend", "backend", "noop")
	return &backendRuntime{backend: noopQueue{}}, nil
}

type postgresBackendFactory struct{}

func (postgresBackendFactory) Build(
	ctx context.Context,
	cfg cliConfig,
	settings schedulor.LqSettings,
	logger *zap.SugaredLogger,
) (*backendRuntime, error) {
	if cfg.dbDSN == "" {
		return nil, fmt.Errorf("SCHEDULOR_DB_DSN (or --db-dsn) is required for postgres backend")
	}
	db, err := sql.Open("pgx", cfg.dbDSN)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	connector := sqlConnector{db: db}
	backend := queue.NewPostgresQueue(connector, queue.PostgresQueueOptions{
		TaskMaxAttempts: settings.TaskMaxAttempts,
		TaskVisibility:  settings.TaskVisibility,
	})
	logger.Infow("initialized queue backend", "backend", "postgres")
	return &backendRuntime{
		backend: backend,
		db:      connector,
		close:   db.Close,
	}, nil
}

func buildTaskExecutor(cfg cliConfig, logger *zap.SugaredLogger) schedulor.TaskExecutor {
	switch cfg.executorType {
	case "", "bash", "bash_file", "bash_payload":
		if cfg.bashCommandsFile != "" {
			commands, err := schedulor.LoadBashTaskCommandsFromFile(cfg.bashCommandsFile)
			if err != nil {
				logger.Fatalw("failed to load bash executor config", "file", cfg.bashCommandsFile, "err", err)
			}
			return schedulor.NewBashTaskExecutorWithCommands(commands)
		}
		return schedulor.NewBashTaskExecutor(splitCSV(cfg.executorTaskNames), cfg.commandField)
	default:
		logger.Fatalw("unsupported executor type", "executor", cfg.executorType)
	}
	return nil
}

func newLogger(level string) *zap.Logger {
	cfg := zap.NewProductionConfig()
	switch strings.ToLower(level) {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn", "warning":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		fail(fmt.Sprintf("unsupported log level %q", level))
	}
	logger, err := cfg.Build()
	if err != nil {
		fail(fmt.Sprintf("failed to build logger: %v", err))
	}
	return logger
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		fail(fmt.Sprintf("invalid boolean value %q for %s", value, key))
		return fallback
	}
}

func splitCSV(input string) []string {
	parts := strings.Split(input, ",")
	result := make([]string, 0, len(parts))
	for _, item := range parts {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

type noopQueue struct{}

func (noopQueue) Enqueue(
	ctx context.Context,
	taskName string,
	payload queue.JSONPayload,
	availableAt time.Time,
	idemKey string,
) (*int64, error) {
	return new(int64(0)), nil
}

func (noopQueue) Claim(ctx context.Context, tasks []string, limit int) ([]queue.Claimed, error) {
	return nil, nil
}

func (noopQueue) StartHeartbeat(ctx context.Context, taskId int64, leaseToken uuid.UUID, lost chan struct{}) {
}

func (noopQueue) Ack(ctx context.Context, taskId int64, leaseToken uuid.UUID) (bool, error) {
	return true, nil
}

func (noopQueue) Nack(
	ctx context.Context,
	taskId int64,
	leaseToken uuid.UUID,
	errText string,
	delay time.Duration,
) (bool, error) {
	return true, nil
}

func (noopQueue) MoveToDLQ(ctx context.Context, taskId int64, leaseToken uuid.UUID, errText string) (bool, error) {
	return true, nil
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
