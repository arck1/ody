package queue

import (
	"testing"
	"time"
)

func TestNewPostgresQueueNormalizesUnsafeOptions(t *testing.T) {
	q := NewPostgresQueue(nil, PostgresQueueOptions{
		TaskMaxAttempts: -1,
		TaskVisibility:  -time.Second,
	})
	if q.options.TaskMaxAttempts != 25 {
		t.Fatalf("unexpected max attempts: %d", q.options.TaskMaxAttempts)
	}
	if q.options.TaskVisibility != 60*time.Second {
		t.Fatalf("unexpected task visibility: %v", q.options.TaskVisibility)
	}

	ticker := q.GetHeartbeatTicker()
	ticker.Stop()
}
