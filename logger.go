package schedulor

import (
	"go.uber.org/zap"
)

// LqLogger adapts zap.SugaredLogger to gocron logger interface.
type LqLogger struct {
	*zap.SugaredLogger
}

// Error adapts gocron logger interface to zap Errorf.
func (l LqLogger) Error(msg string, args ...any) {
	l.Errorf(msg, args...)
}

// Info adapts gocron logger interface to zap Infof.
func (l LqLogger) Info(msg string, args ...any) {
	l.Infof(msg, args...)
}

// Warn adapts gocron logger interface to zap Warnf.
func (l LqLogger) Warn(msg string, args ...any) {
	l.Warnf(msg, args...)
}

// Debug adapts gocron logger interface to zap Debugf.
func (l LqLogger) Debug(msg string, args ...any) {
	l.Debugf(msg, args...)
}
