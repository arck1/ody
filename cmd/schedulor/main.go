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
	dbDSN            string
	executorType     string
	bashCommandsFile string
	logLevel         string
}

func main() {
	cfg := parseConfig()
	logger := newLogger(cfg.logLevel)
	defer logger.Sync()
	sugared := logger.Sugar()

	db, err := sql.Open("pgx", cfg.dbDSN)
	if err != nil {
		sugared.Fatalw("failed to open db connection", "err", err)
	}
	defer db.Close()

	if err = db.PingContext(context.Background()); err != nil {
		sugared.Fatalw("failed to ping db", "err", err)
	}

	settings := schedulor.GetSettings(nil)
	connector := sqlConnector{db: db}
	backend := queue.NewPostgresQueue(connector, &queue.PostgresQueueOptions{
		TaskMaxAttempts: settings.TaskMaxAttempts,
		TaskVisibility:  settings.TaskVisibility,
	})

	taskExec := buildTaskExecutor(cfg, sugared)
	lqExecutor := schedulor.NewLqExecutor(sugared, backend, taskExec, settings.LqExecutorOptions)
	lqScheduler := schedulor.NewLqScheduler(connector, sugared, lqExecutor, settings.LqSchedulerOptions)

	app := schedulor.NewFxApp(schedulor.FxAppOptions{
		Components: []schedulor.FxLifecycleComponent{
			lqExecutor,
			lqScheduler,
		},
	})

	app.Run()
}

func parseConfig() cliConfig {
	cfg := cliConfig{}
	flag.StringVar(&cfg.dbDSN, "db-dsn", envOrDefault("SCHEDULOR_DB_DSN", ""), "PostgreSQL DSN")
	flag.StringVar(&cfg.executorType, "executor", envOrDefault("SCHEDULOR_EXECUTOR", "bash_file"), "Executor type: bash_file")
	flag.StringVar(&cfg.bashCommandsFile, "bash-commands-file", envOrDefault("SCHEDULOR_BASH_COMMANDS_FILE", ""), "Path to JSON file with bash commands")
	flag.StringVar(&cfg.logLevel, "log-level", envOrDefault("SCHEDULOR_LOG_LEVEL", "info"), "Logger level: debug|info|warn|error")
	flag.Parse()

	if cfg.dbDSN == "" {
		fail("SCHEDULOR_DB_DSN (or --db-dsn) is required")
	}
	if cfg.executorType != "bash_file" {
		fail("only --executor=bash_file is supported in CLI")
	}
	if cfg.bashCommandsFile == "" {
		fail("SCHEDULOR_BASH_COMMANDS_FILE (or --bash-commands-file) is required")
	}
	return cfg
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

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
