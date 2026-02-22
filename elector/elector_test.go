package elector

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sqlConnector struct {
	db  *sql.DB
	err error
}

func (c sqlConnector) GetConnect(ctx context.Context) (*sql.DB, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.db, nil
}

func TestStaticLeaderElector(t *testing.T) {
	opts := Options{LeaderKey: "k", LeaderId: "id", LeaderTTL: 5 * time.Second}
	e := NewStaticLeaderElector(opts)

	if err := e.IsLeader(context.Background()); err != nil {
		t.Fatalf("IsLeader error: %v", err)
	}
	if e.GetLeaderKey() != "k" {
		t.Fatalf("unexpected key: %s", e.GetLeaderKey())
	}
	if e.GetLeaderId() != "id" {
		t.Fatalf("unexpected id: %s", e.GetLeaderId())
	}
	if e.GetLeaderTTL() != 5*time.Second {
		t.Fatalf("unexpected ttl: %v", e.GetLeaderTTL())
	}
}

func TestPgLeaderElectorIsLeaderSuccess(t *testing.T) {
	db := newStubDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		if !strings.Contains(query, "INSERT INTO lq_schedule_leader") {
			t.Fatalf("unexpected query: %s", query)
		}
		if len(args) != 4 {
			t.Fatalf("unexpected args count: %d", len(args))
		}
		if args[0].Value != "leader_key" || args[1].Value != "leader_id" {
			t.Fatalf("unexpected args values: %+v", args)
		}
		return driver.RowsAffected(1), nil
	})
	defer db.Close()

	e := NewPgLeaderElector(sqlConnector{db: db}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	if err := e.IsLeader(context.Background()); err != nil {
		t.Fatalf("expected leader, got error: %v", err)
	}
}

func TestPgLeaderElectorIsLeaderNotLeader(t *testing.T) {
	db := newStubDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		return driver.RowsAffected(0), nil
	})
	defer db.Close()

	e := NewPgLeaderElector(sqlConnector{db: db}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	err := e.IsLeader(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("expected not leader error, got: %v", err)
	}
}

func TestPgLeaderElectorConnectionError(t *testing.T) {
	e := NewPgLeaderElector(sqlConnector{err: errors.New("db down")}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	err := e.IsLeader(context.Background())
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected db error, got: %v", err)
	}
}

func TestPgLeaderElectorExecError(t *testing.T) {
	db := newStubDB(t, func(query string, args []driver.NamedValue) (driver.Result, error) {
		return nil, errors.New("exec failed")
	})
	defer db.Close()

	e := NewPgLeaderElector(sqlConnector{db: db}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	err := e.IsLeader(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exec failed") {
		t.Fatalf("expected exec error, got: %v", err)
	}
}

func newStubDB(t *testing.T, execFn func(query string, args []driver.NamedValue) (driver.Result, error)) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("elector_stub_%d", stubDriverCounter.Add(1))
	sql.Register(name, stubDriver{execFn: execFn})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql open: %v", err)
	}
	return db
}

var stubDriverCounter atomic.Int64

type stubDriver struct {
	execFn func(query string, args []driver.NamedValue) (driver.Result, error)
}

func (d stubDriver) Open(name string) (driver.Conn, error) {
	return &stubConn{execFn: d.execFn}, nil
}

type stubConn struct {
	execFn func(query string, args []driver.NamedValue) (driver.Result, error)
}

func (c *stubConn) Prepare(query string) (driver.Stmt, error) { return stubStmt{}, nil }
func (c *stubConn) Close() error                              { return nil }
func (c *stubConn) Begin() (driver.Tx, error)                 { return stubTx{}, nil }

func (c *stubConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.execFn == nil {
		return driver.RowsAffected(0), nil
	}
	return c.execFn(query, args)
}

func (c *stubConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return stubRows{}, nil
}

type stubStmt struct{}

func (s stubStmt) Close() error  { return nil }
func (s stubStmt) NumInput() int { return -1 }
func (s stubStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (s stubStmt) Query(args []driver.Value) (driver.Rows, error) { return stubRows{}, nil }

type stubTx struct{}

func (t stubTx) Commit() error   { return nil }
func (t stubTx) Rollback() error { return nil }

type stubRows struct{}

func (r stubRows) Columns() []string              { return []string{} }
func (r stubRows) Close() error                   { return nil }
func (r stubRows) Next(dest []driver.Value) error { return io.EOF }
