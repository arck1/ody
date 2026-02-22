package schedulor

import (
	"context"
	"time"

	"gorm.io/datatypes"
)

type TasksQueue interface {
	Enqueue(
		ctx context.Context,
		taskName string,
		payload datatypes.JSONType[map[string]any],
		availableAt time.Time,
		idemKey string,
	) (*int64, error)
}
