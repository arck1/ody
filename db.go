package schedulor

import (
	"context"
	"database/sql"
)

type DbConnector interface {
	// GetConnect returns an initialized SQL connection for the current request context.
	GetConnect(ctx context.Context) (*sql.DB, error)
}
