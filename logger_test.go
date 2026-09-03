package schedulor

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewZapLoggerWritesStructuredFields(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := NewZapLogger(zap.New(core).Sugar())

	logger.Info("task completed", "task_id", "42", "attempt", 2)

	entries := recorded.All()
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1", len(entries))
	}
	if entries[0].Message != "task completed" {
		t.Fatalf("message = %q, want task completed", entries[0].Message)
	}
	fields := entries[0].ContextMap()
	if fields["task_id"] != "42" || fields["attempt"] != int64(2) {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestNewZapLoggerReturnsNilForNilLogger(t *testing.T) {
	if logger := NewZapLogger(nil); logger != nil {
		t.Fatalf("NewZapLogger(nil) = %#v, want nil", logger)
	}
}

func TestGocronLoggerFormatsMessages(t *testing.T) {
	recorder := &recordingLogger{}
	logger := gocronLogger{Logger: recorder}

	logger.Warn("job %s failed after %d attempts", "sync", 3)

	if recorder.message != "job sync failed after 3 attempts" {
		t.Fatalf("message = %q", recorder.message)
	}
}

type recordingLogger struct {
	message string
	fields  []any
}

func (l *recordingLogger) Debug(message string, fields ...any) { l.record(message, fields) }
func (l *recordingLogger) Info(message string, fields ...any)  { l.record(message, fields) }
func (l *recordingLogger) Warn(message string, fields ...any)  { l.record(message, fields) }
func (l *recordingLogger) Error(message string, fields ...any) { l.record(message, fields) }

func (l *recordingLogger) record(message string, fields []any) {
	l.message = message
	l.fields = append([]any(nil), fields...)
}
