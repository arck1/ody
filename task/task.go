// Package task defines typed task contracts independently from worker infrastructure.
package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"schedulor/execution"
)

var ErrEmptyName = errors.New("task name is empty")

// RetryPolicy returns the delay after a completed attempt. attempt starts at one.
type RetryPolicy func(attempt int) time.Duration

type specification struct {
	name        string
	version     int
	maxAttempts int
	timeout     time.Duration
	retry       RetryPolicy
}

// Definition describes a versioned task with typed input and stored output.
type Definition[Input, Output any] struct{ spec specification }

type Option func(*specification)

// WithVersion changes the durable task contract version.
func WithVersion(version int) Option { return func(s *specification) { s.version = version } }

// WithMaxAttempts includes the first delivery and all subsequent retries.
func WithMaxAttempts(attempts int) Option { return func(s *specification) { s.maxAttempts = attempts } }

// WithTimeout limits one handler attempt, not the lifetime of the durable execution.
func WithTimeout(timeout time.Duration) Option { return func(s *specification) { s.timeout = timeout } }

// WithRetryPolicy controls delays when a handler returns an ordinary error.
func WithRetryPolicy(policy RetryPolicy) Option { return func(s *specification) { s.retry = policy } }

// New creates a typed task definition.
func New[Input, Output any](name string, options ...Option) Definition[Input, Output] {
	spec := specification{
		name:        strings.TrimSpace(name),
		version:     1,
		maxAttempts: 3,
		retry:       ExponentialBackoff(100*time.Millisecond, 30*time.Second),
	}
	for _, option := range options {
		if option != nil {
			option(&spec)
		}
	}
	return Definition[Input, Output]{spec: spec}
}

func (d Definition[I, O]) Name() string           { return d.spec.name }
func (d Definition[I, O]) Version() int           { return d.spec.version }
func (d Definition[I, O]) MaxAttempts() int       { return d.spec.maxAttempts }
func (d Definition[I, O]) Timeout() time.Duration { return d.spec.timeout }

func (d Definition[I, O]) Validate() error {
	if d.spec.name == "" {
		return ErrEmptyName
	}
	if d.spec.version <= 0 {
		return fmt.Errorf("task %q version must be positive", d.spec.name)
	}
	if d.spec.maxAttempts <= 0 {
		return fmt.Errorf("task %q max attempts must be positive", d.spec.name)
	}
	if d.spec.retry == nil {
		return fmt.Errorf("task %q retry policy is nil", d.spec.name)
	}
	return nil
}

func (d Definition[I, O]) EncodeInput(input I) (json.RawMessage, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode task %q input: %w", d.Name(), err)
	}
	return raw, nil
}

// Enqueue creates a durable standalone execution.
func (d Definition[I, O]) Enqueue(ctx context.Context, store ExecutionCreator, input I, options ...EnqueueOption) (execution.Execution, error) {
	if store == nil {
		return execution.Execution{}, errors.New("execution store is nil")
	}
	raw, err := d.EncodeInput(input)
	if err != nil {
		return execution.Execution{}, err
	}
	request := execution.CreateExecution{
		TaskName: d.Name(), TaskVersion: d.Version(), Input: raw,
		MaxAttempts: d.MaxAttempts(),
	}
	for _, option := range options {
		if option != nil {
			option(&request)
		}
	}
	return store.CreateExecution(ctx, request)
}

type EnqueueOption func(*execution.CreateExecution)

// ExecutionCreator is the minimal persistence capability required to enqueue a task.
type ExecutionCreator interface {
	CreateExecution(context.Context, execution.CreateExecution) (execution.Execution, error)
}

func WithAvailableAt(value time.Time) EnqueueOption {
	return func(r *execution.CreateExecution) { r.AvailableAt = value }
}
func WithIdempotencyKey(value string) EnqueueOption {
	return func(r *execution.CreateExecution) { r.IdempotencyKey = value }
}

// ExponentialBackoff returns a capped exponential retry policy.
func ExponentialBackoff(base, maximum time.Duration) RetryPolicy {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		delay := base
		for i := 1; i < attempt && delay < maximum/2; i++ {
			delay *= 2
		}
		if delay > maximum {
			return maximum
		}
		return delay
	}
}
