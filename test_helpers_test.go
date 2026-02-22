package schedulor

import "go.uber.org/zap"

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}
