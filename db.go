package schedulor

import (
	"context"
	"database/sql"
)

type DbConnector interface {
	GetConnect(ctx context.Context) (*sql.DB, error)
}
