// Package pipeline defines durable typed DAGs whose intermediate outputs are stored.
package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"schedulor/task"
)

type Definition[Input any] struct {
	name    string
	version int
	nodes   []*nodeDefinition
}

type nodeDefinition struct {
	key          string
	taskName     string
	taskVersion  int
	maxAttempts  int
	dependencies []string
	buildInput   func(json.RawMessage, map[string]json.RawMessage) (json.RawMessage, error)
	err          error
}

// Node is a typed reference to a pipeline node's stored output.
type Node[Output any] struct {
	key             string
	pipelineName    string
	pipelineVersion int
}

func New[Input any](name string, version int) *Definition[Input] {
	return &Definition[Input]{name: strings.TrimSpace(name), version: version}
}

func (d *Definition[I]) Name() string { return d.name }
func (d *Definition[I]) Version() int { return d.version }

// Start adds a root node whose input is derived from the pipeline input.
func Start[P, I, O any](pipeline *Definition[P], key string, definition task.Definition[I, O], mapper func(P) I) Node[O] {
	node := Node[O]{key: key, pipelineName: pipeline.name, pipelineVersion: pipeline.version}
	validationErr := definition.Validate()
	if mapper == nil {
		validationErr = errors.New("pipeline node mapper is nil")
	}
	pipeline.nodes = append(pipeline.nodes, &nodeDefinition{key: key, taskName: definition.Name(), taskVersion: definition.Version(), maxAttempts: definition.MaxAttempts(), err: validationErr, buildInput: func(initial json.RawMessage, _ map[string]json.RawMessage) (json.RawMessage, error) {
		var input P
		if err := json.Unmarshal(initial, &input); err != nil {
			return nil, err
		}
		return json.Marshal(mapper(input))
	}})
	return node
}

// Then adds a node whose input is derived from one predecessor's stored output.
func Then[P, Previous, I, O any](pipeline *Definition[P], previous Node[Previous], key string, definition task.Definition[I, O], mapper func(Previous) I) Node[O] {
	node := Node[O]{key: key, pipelineName: pipeline.name, pipelineVersion: pipeline.version}
	validationErr := definition.Validate()
	if mapper == nil {
		validationErr = errors.New("pipeline node mapper is nil")
	}
	if previous.pipelineName != pipeline.name || previous.pipelineVersion != pipeline.version {
		validationErr = errors.New("pipeline node dependency belongs to another pipeline")
	}
	pipeline.nodes = append(pipeline.nodes, &nodeDefinition{key: key, taskName: definition.Name(), taskVersion: definition.Version(), maxAttempts: definition.MaxAttempts(), dependencies: []string{previous.key}, err: validationErr, buildInput: func(_ json.RawMessage, outputs map[string]json.RawMessage) (json.RawMessage, error) {
		var value Previous
		if err := json.Unmarshal(outputs[previous.key], &value); err != nil {
			return nil, err
		}
		return json.Marshal(mapper(value))
	}})
	return node
}

// Join2 adds a fan-in node derived from two predecessor outputs.
func Join2[P, A, B, I, O any](pipeline *Definition[P], first Node[A], second Node[B], key string, definition task.Definition[I, O], mapper func(A, B) I) Node[O] {
	node := Node[O]{key: key, pipelineName: pipeline.name, pipelineVersion: pipeline.version}
	validationErr := definition.Validate()
	if mapper == nil {
		validationErr = errors.New("pipeline node mapper is nil")
	}
	if first.pipelineName != pipeline.name || second.pipelineName != pipeline.name || first.pipelineVersion != pipeline.version || second.pipelineVersion != pipeline.version {
		validationErr = errors.New("pipeline node dependency belongs to another pipeline")
	}
	pipeline.nodes = append(pipeline.nodes, &nodeDefinition{key: key, taskName: definition.Name(), taskVersion: definition.Version(), maxAttempts: definition.MaxAttempts(), dependencies: []string{first.key, second.key}, err: validationErr, buildInput: func(_ json.RawMessage, outputs map[string]json.RawMessage) (json.RawMessage, error) {
		var a A
		if err := json.Unmarshal(outputs[first.key], &a); err != nil {
			return nil, err
		}
		var b B
		if err := json.Unmarshal(outputs[second.key], &b); err != nil {
			return nil, err
		}
		return json.Marshal(mapper(a, b))
	}})
	return node
}

func (d *Definition[I]) validate() error {
	if d == nil || d.name == "" {
		return errors.New("pipeline name is empty")
	}
	if d.version <= 0 {
		return fmt.Errorf("pipeline %q version must be positive", d.name)
	}
	seen := map[string]struct{}{}
	for _, node := range d.nodes {
		if node.err != nil {
			return fmt.Errorf("pipeline node %q: %w", node.key, node.err)
		}
		node.key = strings.TrimSpace(node.key)
		if node.key == "" {
			return errors.New("pipeline node key is empty")
		}
		if _, ok := seen[node.key]; ok {
			return fmt.Errorf("duplicate pipeline node %q", node.key)
		}
		for _, dependency := range node.dependencies {
			if _, ok := seen[dependency]; !ok {
				return fmt.Errorf("node %q depends on unknown or forward node %q", node.key, dependency)
			}
		}
		seen[node.key] = struct{}{}
	}
	return nil
}

type compiledDefinition struct {
	name    string
	version int
	nodes   []*nodeDefinition
}

// Registry contains the pipeline definitions required to resume runs after restart.
type Registry struct{ definitions map[string]compiledDefinition }

func NewRegistry(definitions ...RegisteredDefinition) (*Registry, error) {
	r := &Registry{definitions: map[string]compiledDefinition{}}
	for _, definition := range definitions {
		compiled, err := definition.compile()
		if err != nil {
			return nil, err
		}
		key := registryKey(compiled.name, compiled.version)
		if _, ok := r.definitions[key]; ok {
			return nil, fmt.Errorf("duplicate pipeline %s", key)
		}
		r.definitions[key] = compiled
	}
	return r, nil
}

// RegisteredDefinition is implemented by every typed pipeline definition.
type RegisteredDefinition interface {
	compile() (compiledDefinition, error)
}

func (d *Definition[I]) compile() (compiledDefinition, error) {
	if err := d.validate(); err != nil {
		return compiledDefinition{}, err
	}
	return compiledDefinition{name: d.name, version: d.version, nodes: append([]*nodeDefinition(nil), d.nodes...)}, nil
}
func registryKey(name string, version int) string { return fmt.Sprintf("%s@v%d", name, version) }
