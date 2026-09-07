// Package postgres provides a PostgreSQL-backed execution.Store.
//
// Executions, pipeline runs, and append-only events are normalized into separate tables. Claim uses
// SELECT ... FOR UPDATE SKIP LOCKED, allowing several worker processes to poll the same task names
// without blocking each other. Completion and retry statements require the current lease token, so
// a stale worker cannot overwrite the result of a newer delivery.
//
// Store.Migrate applies the embedded idempotent schema. Store does not own the supplied *sql.DB;
// connection pool sizing, health checks, migration timing, and shutdown belong to the application.
package postgres
