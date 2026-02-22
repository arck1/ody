package schedulor

import (
	"context"

	"gorm.io/gorm"
)

type DbConnector interface {
	GetConnect(ctx context.Context) (*gorm.DB, error)
}
