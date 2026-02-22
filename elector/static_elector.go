package elector

import (
	"context"
	"time"
)

type StaticLeaderElector struct {
	options Options
}

func NewStaticLeaderElector(options Options) *StaticLeaderElector {
	return &StaticLeaderElector{options: options}
}

func (e *StaticLeaderElector) IsLeader(ctx context.Context) error {
	return nil
}

func (e *StaticLeaderElector) GetLeaderKey() string {
	return e.options.LeaderKey
}

func (e *StaticLeaderElector) GetLeaderId() string {
	return e.options.LeaderId
}

func (e *StaticLeaderElector) GetLeaderTTL() time.Duration {
	return e.options.LeaderTTL
}
