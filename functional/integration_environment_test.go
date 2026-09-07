//go:build integration

package functional_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redislib "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	executionpostgres "schedulor/execution/postgres"
)

const (
	postgresImage = "postgres:16-alpine"
	redisImage    = "redis:7.4-alpine"
)

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

func startRedis(t *testing.T) *redislib.Client {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        redisImage,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	client := redislib.NewClient(&redislib.Options{Addr: endpoint})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Eventually(t, func() bool { return client.Ping(ctx).Err() == nil }, 15*time.Second, 100*time.Millisecond)
	return client
}
