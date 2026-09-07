package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"ody/execution"
)

var ErrUnknownTask = errors.New("unknown task")

// Message is the typed handler input plus delivery metadata for the current attempt.
type Message[I any] struct {
	ExecutionID string
	Input       I
	Attempt     int
	MaxAttempts int
}

type Handler[I, O any] func(context.Context, Message[I]) (O, error)

// Permanent marks an error as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

// RetryAfter overrides the definition's retry policy for one failed attempt.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return retryAfterError{error: err, delay: delay}
}

type retryAfterError struct {
	error
	delay time.Duration
}

func (e retryAfterError) Unwrap() error { return e.error }

func RetryDelay(err error) (time.Duration, bool) {
	if target, ok := errors.AsType[retryAfterError](err); ok {
		return target.delay, true
	}
	return 0, false
}

type descriptor struct {
	name                 string
	version, maxAttempts int
	timeout              time.Duration
	retry                RetryPolicy
	execute              func(context.Context, execution.Execution) (json.RawMessage, error)
}

type Binding struct {
	descriptor descriptor
	err        error
}

// Handle binds a definition to typed business logic.
func Handle[I, O any](definition Definition[I, O], handler Handler[I, O]) Binding {
	if err := definition.Validate(); err != nil {
		return Binding{err: err}
	}
	if handler == nil {
		return Binding{err: errors.New("task handler is nil")}
	}
	d := descriptor{
		name: definition.Name(), version: definition.Version(), maxAttempts: definition.MaxAttempts(),
		timeout: definition.Timeout(), retry: definition.spec.retry,
	}
	d.execute = func(ctx context.Context, item execution.Execution) (output json.RawMessage, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("task panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		var input I
		if err = json.Unmarshal(item.Input, &input); err != nil {
			return nil, Permanent(fmt.Errorf("decode task %q input: %w", d.name, err))
		}
		message := Message[I]{
			ExecutionID: item.ID.String(), Input: input,
			Attempt: item.Attempt, MaxAttempts: item.MaxAttempts,
		}
		value, err := handler(ctx, message)
		if err != nil {
			return nil, err
		}
		output, err = json.Marshal(value)
		if err != nil {
			return nil, Permanent(fmt.Errorf("encode task %q output: %w", d.name, err))
		}
		return output, nil
	}
	return Binding{descriptor: d}
}

type Module struct {
	name     string
	bindings []Binding
}

// NewModule groups bindings by application capability and rejects duplicates inside the group.
func NewModule(name string, bindings ...Binding) (Module, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Module{}, errors.New("module name is empty")
	}
	seen := map[string]struct{}{}
	for _, binding := range bindings {
		if binding.err != nil {
			return Module{}, binding.err
		}
		key := descriptorKey(binding.descriptor.name, binding.descriptor.version)
		if _, ok := seen[key]; ok {
			return Module{}, fmt.Errorf("module %q contains duplicate task %q", name, key)
		}
		seen[key] = struct{}{}
	}
	return Module{name: name, bindings: append([]Binding(nil), bindings...)}, nil
}

func (m Module) Name() string { return m.name }

// Registry composes modules and executes type-erased task bindings.
type Registry struct {
	tasks map[string]descriptor
	keys  []execution.TaskKey
}

func NewRegistry(modules ...Module) (*Registry, error) {
	r := &Registry{tasks: map[string]descriptor{}}
	owners := map[string]string{}
	for _, module := range modules {
		if module.name == "" {
			return nil, errors.New("invalid zero-value module")
		}
		for _, binding := range module.bindings {
			key := descriptorKey(binding.descriptor.name, binding.descriptor.version)
			if owner, ok := owners[key]; ok {
				return nil, fmt.Errorf("task %q is registered by modules %q and %q", key, owner, module.name)
			}
			owners[key], r.tasks[key] = module.name, binding.descriptor
			r.keys = append(r.keys, execution.TaskKey{Name: binding.descriptor.name, Version: binding.descriptor.version})
		}
	}
	sort.Slice(r.keys, func(i, j int) bool {
		if r.keys[i].Name == r.keys[j].Name {
			return r.keys[i].Version < r.keys[j].Version
		}
		return r.keys[i].Name < r.keys[j].Name
	})
	return r, nil
}

func (r *Registry) Keys() []execution.TaskKey { return append([]execution.TaskKey(nil), r.keys...) }

func (r *Registry) Execute(ctx context.Context, item execution.Execution) (json.RawMessage, error) {
	d, ok := r.tasks[descriptorKey(item.TaskName, item.TaskVersion)]
	if !ok {
		return nil, Permanent(fmt.Errorf("%w: %s@v%d", ErrUnknownTask, item.TaskName, item.TaskVersion))
	}
	return d.execute(ctx, item)
}

func (r *Registry) Policy(name string, version int) (int, time.Duration, RetryPolicy, bool) {
	d, ok := r.tasks[descriptorKey(name, version)]
	if !ok {
		return 0, 0, nil, false
	}
	return d.maxAttempts, d.timeout, d.retry, true
}

func descriptorKey(name string, version int) string { return fmt.Sprintf("%s@v%d", name, version) }
