package elector

import (
	"context"
	"database/sql"
	"time"
)

// LeaderElector abstracts distributed leadership check/acquisition.
type LeaderElector interface {
	// IsLeader validates or acquires leader lease for current node.
	IsLeader(ctx context.Context) error
	// GetLeaderKey returns election group key.
	GetLeaderKey() string
	// GetLeaderId returns current node identifier.
	GetLeaderId() string
	// GetLeaderTTL returns lease duration.
	GetLeaderTTL() time.Duration
}

// DbConnector is minimal DB dependency required by electors.
type DbConnector interface {
	// GetConnect returns SQL connection used for election operations.
	GetConnect(ctx context.Context) (*sql.DB, error)
}

// Options define common leader election settings.
type Options struct {
	// LeaderKey identifies shared election group.
	LeaderKey string
	// LeaderId identifies current node.
	LeaderId string
	// LeaderTTL is lease validity duration.
	LeaderTTL time.Duration
}
