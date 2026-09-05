package redis

import (
	"encoding/base64"

	"github.com/google/uuid"

	"schedulor/execution"
)

// User-controlled names are encoded so separators in task/node/idempotency values cannot collide
// with the structural separators of the Redis keyspace.
func encodeKey(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func (s *Store) executionKey(id uuid.UUID) string { return s.prefix + "execution:" + id.String() }
func (s *Store) eventsKey(id uuid.UUID) string    { return s.prefix + "events:" + id.String() }
func (s *Store) executionsKey() string            { return s.prefix + "executions" }
func (s *Store) leasesKey() string                { return s.prefix + "leases" }
func (s *Store) queueKey(name string) string      { return s.prefix + "queue:" + encodeKey(name) }
func (s *Store) taskIndexKey(name string) string  { return s.prefix + "task:" + encodeKey(name) }

func (s *Store) statusKey(status execution.Status) string {
	return s.prefix + "status:" + string(status)
}

func (s *Store) idempotencyKey(taskName, key string) string {
	return s.prefix + "idempotency:" + encodeKey(taskName+"\x00"+key)
}

func (s *Store) nodeKey(runID uuid.UUID, node string) string {
	return s.prefix + "node:" + runID.String() + ":" + encodeKey(node)
}

func (s *Store) runExecutionsKey(id uuid.UUID) string {
	return s.prefix + "run-executions:" + id.String()
}

func (s *Store) pipelineKey(id uuid.UUID) string { return s.prefix + "pipeline:" + id.String() }
func (s *Store) runsKey() string                 { return s.prefix + "pipelines" }

func (s *Store) runStatusKey(status execution.RunStatus) string {
	return s.prefix + "pipeline-status:" + string(status)
}
