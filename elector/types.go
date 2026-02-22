package elector

import (
	"context"
	"database/sql"
	"time"
)

type LeaderElector interface {
	IsLeader(ctx context.Context) error
	GetLeaderKey() string
	GetLeaderId() string
	GetLeaderTTL() time.Duration
}

type DbConnector interface {
	GetConnect(ctx context.Context) (*sql.DB, error)
}

type Options struct {
	LeaderKey string
	LeaderId  string
	LeaderTTL time.Duration
}
