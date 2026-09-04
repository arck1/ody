//go:build integration

package functional_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	executionpostgres "schedulor/execution/postgres"
)

const postgresImage = "postgres:16-alpine"

func startPostgres(t *testing.T) (*sql.DB, *executionpostgres.Store) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := postgrescontainer.Run(ctx, postgresImage,
		postgrescontainer.WithDatabase("schedulor_functional"),
		postgrescontainer.WithUsername("schedulor"),
		postgrescontainer.WithPassword("schedulor"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp").WithStartupTimeout(time.Minute)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.Eventually(t, func() bool { return db.PingContext(ctx) == nil }, 15*time.Second, 100*time.Millisecond)
	store, err := executionpostgres.New(db)
	require.NoError(t, err)
	require.NoError(t, store.Migrate(ctx))
	return db, store
}
