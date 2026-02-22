package schedulor

import (
	"go.uber.org/zap"
)

type LqLogger struct {
	*zap.SugaredLogger
}

func (l LqLogger) Error(msg string, args ...any) {
	l.Errorf(msg, args...)
}

func (l LqLogger) Info(msg string, args ...any) {
	l.Infof(msg, args...)
}

func (l LqLogger) Warn(msg string, args ...any) {
	l.Warnf(msg, args...)
}

func (l LqLogger) Debug(msg string, args ...any) {
	l.Debugf(msg, args...)
}
