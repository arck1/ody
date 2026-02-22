package elector

import (
	"context"
	"errors"
	"time"
)

type PgLeaderElector struct {
	db      DbConnector
	options Options
}

func NewPgLeaderElector(db DbConnector, options Options) *PgLeaderElector {
	return &PgLeaderElector{
		db:      db,
		options: options,
	}
}

func (p *PgLeaderElector) IsLeader(ctx context.Context) error {
	isLeader, err := p.tryBecomeLeader(ctx)
	if err != nil {
		return err
	}
	if isLeader {
		return nil
	}
	return errors.New("not leader")
}

func (p *PgLeaderElector) GetLeaderKey() string        { return p.options.LeaderKey }
func (p *PgLeaderElector) GetLeaderId() string         { return p.options.LeaderId }
func (p *PgLeaderElector) GetLeaderTTL() time.Duration { return p.options.LeaderTTL }

func (p *PgLeaderElector) tryBecomeLeader(ctx context.Context) (bool, error) {
	db, err := p.db.GetConnect(ctx)
	if err != nil {
		return false, err
	}

	now := time.Now().UTC()
	validUntil := now.Add(p.GetLeaderTTL())
	result, err := db.ExecContext(
		ctx,
		`INSERT INTO lq_schedule_leader (key, leader_id, valid_until)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE
		 SET leader_id = EXCLUDED.leader_id,
		     valid_until = EXCLUDED.valid_until
		 WHERE lq_schedule_leader.leader_id = EXCLUDED.leader_id
		    OR lq_schedule_leader.valid_until IS NULL
		    OR lq_schedule_leader.valid_until < $4`,
		p.GetLeaderKey(),
		p.GetLeaderId(),
		validUntil,
		now,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}
