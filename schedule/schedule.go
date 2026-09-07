// Package schedule turns cron ticks into ordinary durable task executions and pipeline runs.
package schedule

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/pipeline"
	"github.com/arck1/ody/task"
)

type MisfirePolicy string

const (
	MisfireSkip    MisfirePolicy = "skip"
	MisfireLatest  MisfirePolicy = "latest"
	MisfireCatchUp MisfirePolicy = "catch_up"
)

type OverlapPolicy string

const (
	OverlapAllow OverlapPolicy = "allow"
	OverlapSkip  OverlapPolicy = "skip"
)

type definitionOptions struct {
	location   *time.Location
	misfire    MisfirePolicy
	overlap    OverlapPolicy
	lookback   time.Duration
	maxCatchUp int
}

type DefinitionOption func(*definitionOptions)

func WithLocation(location *time.Location) DefinitionOption {
	return func(options *definitionOptions) { options.location = location }
}

func WithMisfire(policy MisfirePolicy, lookback time.Duration, maxCatchUp int) DefinitionOption {
	return func(options *definitionOptions) {
		options.misfire, options.lookback, options.maxCatchUp = policy, lookback, maxCatchUp
	}
}

func WithOverlap(policy OverlapPolicy) DefinitionOption {
	return func(options *definitionOptions) { options.overlap = policy }
}

type Definition struct {
	name     string
	schedule cron.Schedule
	options  definitionOptions
	trigger  func(context.Context, execution.Store, *pipeline.Engine, time.Time, string) error
	isActive func(context.Context, execution.Store) (bool, error)
}

// Task creates a cron definition whose input is derived from the scheduled instant.
func Task[I, O any](name, expression string, definition task.Definition[I, O], input func(time.Time) I, options ...DefinitionOption) (Definition, error) {
	if input == nil {
		return Definition{}, errors.New("schedule task input factory is nil")
	}
	configured, err := newDefinition(name, expression, options...)
	if err != nil {
		return Definition{}, err
	}
	configured.trigger = func(ctx context.Context, store execution.Store, _ *pipeline.Engine, scheduledAt time.Time, key string) error {
		_, enqueueErr := definition.Enqueue(ctx, store, input(scheduledAt), task.WithIdempotencyKey(key))
		return enqueueErr
	}
	configured.isActive = func(ctx context.Context, store execution.Store) (bool, error) {
		for _, status := range []execution.Status{execution.StatusPending, execution.StatusRetry, execution.StatusRunning} {
			items, listErr := store.ListExecutions(ctx, execution.ListFilter{TaskName: definition.Name(), Status: status, Limit: 1})
			if listErr != nil || len(items) > 0 {
				return len(items) > 0, listErr
			}
		}
		return false, nil
	}
	return configured, nil
}

// Pipeline creates a cron definition for a registered durable pipeline.
func Pipeline[I any](name, expression string, definition *pipeline.Definition[I], input func(time.Time) I, options ...DefinitionOption) (Definition, error) {
	if definition == nil || input == nil {
		return Definition{}, errors.New("schedule pipeline definition and input factory are required")
	}
	configured, err := newDefinition(name, expression, options...)
	if err != nil {
		return Definition{}, err
	}
	configured.trigger = func(ctx context.Context, _ execution.Store, engine *pipeline.Engine, scheduledAt time.Time, key string) error {
		if engine == nil {
			return errors.New("pipeline scheduler requires pipeline engine")
		}
		_, runErr := pipeline.Run(ctx, engine, definition, input(scheduledAt), pipeline.WithIdempotencyKey(key))
		return runErr
	}
	configured.isActive = func(ctx context.Context, store execution.Store) (bool, error) {
		runs, listErr := store.ListPipelineRuns(ctx, execution.RunFilter{Statuses: []execution.RunStatus{execution.RunPending, execution.RunRunning}})
		if listErr != nil {
			return false, listErr
		}
		for _, run := range runs {
			if run.PipelineName == definition.Name() && run.PipelineVersion == definition.Version() {
				return true, nil
			}
		}
		return false, nil
	}
	return configured, nil
}

func newDefinition(name, expression string, options ...DefinitionOption) (Definition, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Definition{}, errors.New("schedule name is empty")
	}
	configured := definitionOptions{location: time.UTC, misfire: MisfireSkip, overlap: OverlapAllow, maxCatchUp: 100}
	for _, option := range options {
		if option != nil {
			option(&configured)
		}
	}
	if configured.location == nil {
		return Definition{}, errors.New("schedule location is nil")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	parsed, err := parser.Parse("CRON_TZ=" + configured.location.String() + " " + expression)
	if err != nil {
		return Definition{}, fmt.Errorf("parse schedule %q: %w", name, err)
	}
	return Definition{name: name, schedule: parsed, options: configured}, nil
}

type Observer interface {
	Triggered(context.Context, string, time.Time, error)
}

type nopObserver struct{}

func (nopObserver) Triggered(context.Context, string, time.Time, error) {}

type Options struct {
	PollInterval time.Duration
	Observer     Observer
}

type Scheduler struct {
	store       execution.Store
	engine      *pipeline.Engine
	definitions []Definition
	options     Options

	mu   sync.Mutex
	next map[string]time.Time
}

func New(store execution.Store, engine *pipeline.Engine, definitions []Definition, options Options) (*Scheduler, error) {
	if store == nil {
		return nil, errors.New("schedule store is nil")
	}
	seen := map[string]struct{}{}
	for _, definition := range definitions {
		if definition.name == "" || definition.trigger == nil {
			return nil, errors.New("invalid schedule definition")
		}
		if _, exists := seen[definition.name]; exists {
			return nil, fmt.Errorf("duplicate schedule %q", definition.name)
		}
		seen[definition.name] = struct{}{}
	}
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	if options.Observer == nil {
		options.Observer = nopObserver{}
	}
	return &Scheduler{store: store, engine: engine, definitions: append([]Definition(nil), definitions...), options: options, next: map[string]time.Time{}}, nil
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.initialize(ctx, time.Now().UTC())
	ticker := time.NewTicker(s.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			s.tick(ctx, now.UTC())
		}
	}
}

func (s *Scheduler) initialize(ctx context.Context, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, definition := range s.definitions {
		if definition.options.lookback > 0 && definition.options.misfire != MisfireSkip {
			due := occurrences(definition.schedule, now.Add(-definition.options.lookback), now, definition.options.maxCatchUp)
			if definition.options.misfire == MisfireLatest && len(due) > 1 {
				due = due[len(due)-1:]
			}
			for _, scheduledAt := range due {
				s.fire(ctx, definition, scheduledAt)
			}
		}
		s.next[definition.name] = definition.schedule.Next(now)
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, definition := range s.definitions {
		next := s.next[definition.name]
		if next.After(now) {
			continue
		}
		due := occurrences(definition.schedule, next.Add(-time.Nanosecond), now, definition.options.maxCatchUp)
		if definition.options.misfire == MisfireLatest && len(due) > 1 {
			due = due[len(due)-1:]
		} else if definition.options.misfire == MisfireSkip && len(due) > 1 {
			due = due[len(due)-1:]
		}
		for _, scheduledAt := range due {
			s.fire(ctx, definition, scheduledAt)
		}
		s.next[definition.name] = definition.schedule.Next(now)
	}
}

func (s *Scheduler) fire(ctx context.Context, definition Definition, scheduledAt time.Time) {
	if definition.options.overlap == OverlapSkip {
		active, err := definition.isActive(ctx, s.store)
		if err != nil || active {
			s.options.Observer.Triggered(ctx, definition.name, scheduledAt, err)
			return
		}
	}
	key := "schedule:" + definition.name + ":" + scheduledAt.UTC().Format(time.RFC3339Nano)
	err := definition.trigger(ctx, s.store, s.engine, scheduledAt, key)
	s.options.Observer.Triggered(ctx, definition.name, scheduledAt, err)
}

func occurrences(schedule cron.Schedule, after, through time.Time, limit int) []time.Time {
	if limit <= 0 {
		limit = 100
	}
	result := make([]time.Time, 0)
	for next := schedule.Next(after); !next.After(through) && len(result) < limit; next = schedule.Next(next) {
		result = append(result, next)
	}
	return result
}
