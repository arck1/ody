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

	"github.com/stretchr/testify/suite"
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

type LeaderElectorSuite struct{ suite.Suite }

func TestLeaderElectorSuite(t *testing.T) {
	suite.Run(t, new(LeaderElectorSuite))
}

func (s *LeaderElectorSuite) TestStaticLeaderElector() {
	opts := Options{LeaderKey: "k", LeaderId: "id", LeaderTTL: 5 * time.Second}
	e := NewStaticLeaderElector(opts)

	s.Require().NoError(e.IsLeader(context.Background()), "IsLeader should return nil")
	s.Equal("k", e.GetLeaderKey(), "Leader key should be k")
	s.Equal("id", e.GetLeaderId(), "Leader id should be id")
	s.Equal(5*time.Second, e.GetLeaderTTL(), "Leader TTL should be 5s")
}

func (s *LeaderElectorSuite) TestPgLeaderElectorIsLeaderSuccess() {
	db := s.newStubDB(func(query string, args []driver.NamedValue) (driver.Result, error) {
		if !strings.Contains(query, "INSERT INTO lq_schedule_leader") {
			s.Failf("unexpected query: %s", query)
		}
		if len(args) != 4 {
			s.Fail("unexpected args count", len(args))
		}
		if args[0].Value != "leader_key" || args[1].Value != "leader_id" {
			s.Fail("unexpected args values", args)
		}
		return driver.RowsAffected(1), nil
	})
	defer db.Close()

	e := NewPgLeaderElector(sqlConnector{db: db}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	s.Require().NoError(e.IsLeader(context.Background()), "IsLeader should return nil")
}

func (s *LeaderElectorSuite) TestPgLeaderElectorIsLeaderNotLeader() {
	db := s.newStubDB(func(query string, args []driver.NamedValue) (driver.Result, error) {
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
		s.Fail("expected not leader error", err)
	}
}

func (s *LeaderElectorSuite) TestPgLeaderElectorConnectionError() {
	e := NewPgLeaderElector(sqlConnector{err: errors.New("db down")}, Options{
		LeaderKey: "leader_key",
		LeaderId:  "leader_id",
		LeaderTTL: time.Second,
	})

	err := e.IsLeader(context.Background())
	if err == nil || !strings.Contains(err.Error(), "db down") {
		s.Fail("expected db error, got", err)
	}
}

func (s *LeaderElectorSuite) TestPgLeaderElectorExecError() {
	db := s.newStubDB(func(query string, args []driver.NamedValue) (driver.Result, error) {
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
		s.Fail("expected exec error, got", err)
	}
}

func (s *LeaderElectorSuite) newStubDB(execFn func(query string, args []driver.NamedValue) (driver.Result, error)) *sql.DB {
	s.T().Helper()
	name := fmt.Sprintf("elector_stub_%d", stubDriverCounter.Add(1))
	sql.Register(name, stubDriver{execFn: execFn})
	db, err := sql.Open(name, "")
	if err != nil {
		s.Fail("sql open", err)
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
