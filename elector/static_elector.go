package elector

import (
	"context"
	"time"
)

// StaticLeaderElector is single-node elector that always acts as leader.
type StaticLeaderElector struct {
	// options stores static leader metadata.
	options Options
}

// NewStaticLeaderElector creates elector that always reports leader status.
func NewStaticLeaderElector(options Options) *StaticLeaderElector {
	return &StaticLeaderElector{options: options}
}

// IsLeader always succeeds for single-node mode.
func (e *StaticLeaderElector) IsLeader(ctx context.Context) error {
	return nil
}

// GetLeaderKey returns configured leader key.
func (e *StaticLeaderElector) GetLeaderKey() string {
	return e.options.LeaderKey
}

// GetLeaderId returns configured leader id.
func (e *StaticLeaderElector) GetLeaderId() string {
	return e.options.LeaderId
}

// GetLeaderTTL returns configured leader lease TTL.
func (e *StaticLeaderElector) GetLeaderTTL() time.Duration {
	return e.options.LeaderTTL
}
