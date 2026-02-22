package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"schedulor"
	"schedulor/queue"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

type sqlConnector struct {
	db *sql.DB
}

func (c sqlConnector) GetConnect(ctx context.Context) (*sql.DB, error) {
	return c.db, nil
}

type cliConfig struct {
	queueBackend     string
	dbDSN            string
	executorType     string
	bashCommandsFile string
	logLevel         string
	withScheduler    bool
}

type backendRuntime struct {
	backend queue.QueueBackend
	db      schedulor.DbConnector
	close   func() error
}

type backendFactory interface {
	Build(ctx context.Context, cfg cliConfig, settings schedulor.LqSettings, logger *zap.SugaredLogger) (*backendRuntime, error)
}

func main() {
	cfg := parseConfig()
	logger := newLogger(cfg.logLevel)
	defer logger.Sync()
	sugared := logger.Sugar()

	settings := schedulor.GetSettings(nil)
	runtime, err := buildBackendRuntime(context.Background(), cfg, settings, sugared)
	if err != nil {
		sugared.Fatalw("failed to build queue backend", "backend", cfg.queueBackend, "err", err)
	}
	if runtime.close != nil {
		defer runtime.close()
	}

	taskExec := buildTaskExecutor(cfg, sugared)
	lqExecutor := schedulor.NewLqExecutor(sugared, runtime.backend, taskExec, settings.LqExecutorOptions)
	components := []schedulor.FxLifecycleComponent{lqExecutor}

	if cfg.withScheduler {
		if runtime.db == nil {
			sugared.Fatalw("selected backend does not provide db connector required by scheduler", "backend", cfg.queueBackend)
		}
		lqScheduler := schedulor.NewLqScheduler(runtime.db, sugared, lqExecutor, settings.LqSchedulerOptions)
		components = append(components, lqScheduler)
	}

	app := schedulor.NewFxApp(schedulor.FxAppOptions{Components: components})
	app.Run()
}

func parseConfig() cliConfig {
	cfg := cliConfig{}
	flag.StringVar(&cfg.queueBackend, "queue-backend", envOrDefault("SCHEDULOR_QUEUE_BACKEND", "postgres"), "Queue backend: postgres")
	flag.StringVar(&cfg.dbDSN, "db-dsn", envOrDefault("SCHEDULOR_DB_DSN", ""), "PostgreSQL DSN (required for postgres backend)")
	flag.StringVar(&cfg.executorType, "executor", envOrDefault("SCHEDULOR_EXECUTOR", "bash_file"), "Executor type: bash_file")
	flag.StringVar(&cfg.bashCommandsFile, "bash-commands-file", envOrDefault("SCHEDULOR_BASH_COMMANDS_FILE", ""), "Path to JSON file with bash commands")
	flag.StringVar(&cfg.logLevel, "log-level", envOrDefault("SCHEDULOR_LOG_LEVEL", "info"), "Logger level: debug|info|warn|error")
	flag.BoolVar(&cfg.withScheduler, "with-scheduler", envBoolOrDefault("SCHEDULOR_WITH_SCHEDULER", true), "Run scheduler component")
	flag.Parse()

	if cfg.executorType != "bash_file" {
		fail("only --executor=bash_file is supported in CLI")
	}
	if cfg.bashCommandsFile == "" {
		fail("SCHEDULOR_BASH_COMMANDS_FILE (or --bash-commands-file) is required")
	}
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
	}
	factory, ok := factories[cfg.queueBackend]
	if !ok {
		return nil, fmt.Errorf("unsupported queue backend %q", cfg.queueBackend)
	}
	return factory.Build(ctx, cfg, settings, logger)
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
	backend := queue.NewPostgresQueue(connector, &queue.PostgresQueueOptions{
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
	taskExec, err := schedulor.NewBashFileTaskExecutorFromFile(cfg.bashCommandsFile)
	if err != nil {
		logger.Fatalw("failed to load bash executor config", "file", cfg.bashCommandsFile, "err", err)
	}
	return taskExec
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

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
