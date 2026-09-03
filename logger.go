package schedulor

import (
	"fmt"

	"go.uber.org/zap"
)

// Logger is the logging contract used by schedulor components.
// Fields are passed as alternating key/value pairs.
type Logger interface {
	Debug(message string, fields ...any)
	Info(message string, fields ...any)
	Warn(message string, fields ...any)
	Error(message string, fields ...any)
}

type zapLogger struct {
	logger *zap.SugaredLogger
}

var _ Logger = zapLogger{}

// NewZapLogger adapts zap.SugaredLogger to Logger. It returns nil for a nil
// logger so constructor validation remains predictable.
func NewZapLogger(logger *zap.SugaredLogger) Logger {
	if logger == nil {
		return nil
	}
	return zapLogger{logger: logger}
}

func (l zapLogger) Debug(message string, fields ...any) { l.logger.Debugw(message, fields...) }
func (l zapLogger) Info(message string, fields ...any)  { l.logger.Infow(message, fields...) }
func (l zapLogger) Warn(message string, fields ...any)  { l.logger.Warnw(message, fields...) }
func (l zapLogger) Error(message string, fields ...any) { l.logger.Errorw(message, fields...) }

// LqLogger adapts Logger to the printf-style logger expected by gocron.
// Deprecated: schedulor constructs this adapter internally.
type LqLogger struct {
	Logger
}

func (l LqLogger) Error(message string, args ...any) { l.Logger.Error(fmt.Sprintf(message, args...)) }
func (l LqLogger) Info(message string, args ...any)  { l.Logger.Info(fmt.Sprintf(message, args...)) }
func (l LqLogger) Warn(message string, args ...any)  { l.Logger.Warn(fmt.Sprintf(message, args...)) }
func (l LqLogger) Debug(message string, args ...any) { l.Logger.Debug(fmt.Sprintf(message, args...)) }
