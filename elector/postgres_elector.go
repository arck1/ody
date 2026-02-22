package elector

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	result := gorm.WithResult()
	err = gorm.G[lqScheduleLeader](db, result, clause.OnConflict{
		Columns: []clause.Column{{Name: "key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"leader_id":   gorm.Expr("EXCLUDED.leader_id"),
			"valid_until": gorm.Expr("EXCLUDED.valid_until"),
		}),
		Where: clause.Where{
			Exprs: []clause.Expression{
				gorm.Expr(`lq_schedule_leader.leader_id = EXCLUDED.leader_id 
				OR lq_schedule_leader.valid_until is null 
				OR lq_schedule_leader.valid_until < ?`,
					now,
				),
			},
		},
	}).Create(ctx, &lqScheduleLeader{
		LeaderId:   p.GetLeaderId(),
		Key:        p.GetLeaderKey(),
		ValidUntil: now.Add(p.GetLeaderTTL()),
	})
	return result.RowsAffected > 0, err
}

type lqScheduleLeader struct {
	Key        string    `json:"key"         gorm:"column:key"`
	LeaderId   string    `json:"leader_id"   gorm:"column:leader_id"`
	ValidUntil time.Time `json:"valid_until" gorm:"column:valid_until"`
}

func (lqScheduleLeader) TableName() string {
	return "lq_schedule_leader"
}
