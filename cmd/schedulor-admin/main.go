package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"schedulor/execution"
	executionpostgres "schedulor/execution/postgres"
	"schedulor/monitoring"
	"schedulor/monitoring/httpui"
	monitoringprom "schedulor/monitoring/prometheus"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "schedulor-admin:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	global := flag.NewFlagSet("schedulor-admin", flag.ContinueOnError)
	dsn := global.String("db-dsn", env("SCHEDULOR_DB_DSN", ""), "PostgreSQL DSN")
	addr := global.String("addr", env("SCHEDULOR_ADMIN_ADDR", "127.0.0.1:8081"), "HTTP listen address")
	if err := global.Parse(args); err != nil {
		return err
	}
	if *dsn == "" {
		return errors.New("SCHEDULOR_DB_DSN or --db-dsn is required")
	}
	remaining := global.Args()
	if len(remaining) == 0 {
		return errors.New("command is required: tasks|task|pipelines|pipeline|restart|cancel|serve")
	}
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	store, err := executionpostgres.New(db)
	if err != nil {
		return err
	}
	service, err := monitoring.New(store)
	if err != nil {
		return err
	}
	return command(ctx, service, store, *addr, remaining[0], remaining[1:])
}

func command(ctx context.Context, service *monitoring.Service, store execution.Store, addr, name string, args []string) error {
	switch name {
	case "tasks":
		flags := flag.NewFlagSet(name, flag.ContinueOnError)
		status := flags.String("status", "", "execution status")
		taskName := flags.String("name", "", "task name")
		limit := flags.Int("limit", 100, "result limit")
		if err := flags.Parse(args); err != nil {
			return err
		}
		items, err := service.ListTasks(ctx, execution.ListFilter{TaskName: *taskName, Status: execution.Status(*status), Limit: *limit})
		return printJSON(items, err)
	case "task":
		id, err := argumentID(args)
		if err != nil {
			return err
		}
		item, err := service.Task(ctx, id)
		return printJSON(item, err)
	case "pipelines":
		var statuses []execution.RunStatus
		if len(args) > 0 && args[0] != "" {
			for _, status := range strings.Split(args[0], ",") {
				statuses = append(statuses, execution.RunStatus(status))
			}
		}
		items, err := service.ListPipelines(ctx, statuses)
		return printJSON(items, err)
	case "pipeline":
		id, err := argumentID(args)
		if err != nil {
			return err
		}
		item, err := service.Pipeline(ctx, id)
		return printJSON(item, err)
	case "restart":
		id, err := argumentID(args)
		if err != nil {
			return err
		}
		item, err := service.RestartTask(ctx, id)
		return printJSON(item, err)
	case "cancel":
		id, err := argumentID(args)
		if err != nil {
			return err
		}
		reason := "cancelled by operator"
		if len(args) > 1 {
			reason = strings.Join(args[1:], " ")
		}
		return service.CancelTask(ctx, id, reason)
	case "serve":
		return serve(service, store, addr)
	default:
		return fmt.Errorf("unknown command %q", name)
	}
}

func serve(service monitoring.API, store execution.Store, addr string) error {
	registry := prom.NewRegistry()
	if _, err := monitoringprom.New(registry, store); err != nil {
		return err
	}
	handler, err := httpui.New(service, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err = <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}

func argumentID(args []string) (uuid.UUID, error) {
	if len(args) == 0 {
		return uuid.Nil, errors.New("execution or pipeline id is required")
	}
	return uuid.Parse(args[0])
}

func printJSON(value any, err error) error {
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
