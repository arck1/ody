package zapobserver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/worker"
)

type recordingLogger struct {
	infos  int
	errors int
}

func (l *recordingLogger) Infow(string, ...any)  { l.infos++ }
func (l *recordingLogger) Errorw(string, ...any) { l.errors++ }

func TestObserverUsesLoggerInterface(t *testing.T) {
	logger := &recordingLogger{}
	observer, err := New(logger)
	require.NoError(t, err)
	observer.Transition(context.Background(), execution.Execution{TaskName: "ok"}, execution.StatusSucceeded, nil)
	observer.Transition(context.Background(), execution.Execution{TaskName: "failed"}, execution.StatusFailed, errors.New("failed"))
	observer.InfrastructureError(context.Background(), worker.OperationClaim, errors.New("offline"))
	require.Equal(t, 1, logger.infos)
	require.Equal(t, 2, logger.errors)
}
