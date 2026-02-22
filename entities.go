package schedulor

import (
	"time"

	"gorm.io/datatypes"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

type LqTask struct {
	TaskID       int           `json:"task_id"       gorm:"column:task_id"`
	TaskName     string        `json:"task_name"     gorm:"column:task_name"`
	ProcessAfter time.Time     `json:"process_after" gorm:"column:process_after"`
	LeaderId     *string       `json:"leader_id"     gorm:"column:leader_id"`
	CreatedAt    time.Time     `json:"created_at"    gorm:"column:created_at"`
	Data         pgtype.JSONB  `json:"data"          gorm:"column:data"`
	Meta         *pgtype.JSONB `json:"meta"          gorm:"column:meta"`
}

func (LqTask) TableName() string {
	return "lq_tasks"
}

type LqSchedule struct {
	Id          uuid.UUID                          `json:"id"          gorm:"primary_key;type:uuid;column:id"`
	TaskName    string                             `json:"task_name"   gorm:"column:task_name"`
	Cron        string                             `json:"cron"        gorm:"column:cron"`
	Payload     datatypes.JSONType[map[string]any] `json:"payload"     gorm:"column:payload;type:jsonb"`
	IsActive    bool                               `json:"is_active"   gorm:"column:is_active"`
	NextRun     *time.Time                         `json:"next_run"    gorm:"column:next_run"`
	LastRun     *time.Time                         `json:"last_run"    gorm:"column:last_run"`
	TaskID      *int64                             `json:"task_id"     gorm:"column:task_id"`
	Updated     time.Time                          `json:"updated"     gorm:"column:updated"`
	Description *string                            `json:"description" gorm:"column:description"`
}

func (LqSchedule) TableName() string {
	return "lq_schedules"
}

type LqScheduleLeader struct {
	Key        string    `json:"key"         gorm:"column:key"`
	LeaderId   string    `json:"leader_id"   gorm:"column:leader_id"`
	ValidUntil time.Time `json:"valid_until" gorm:"column:valid_until"`
}

func (LqScheduleLeader) TableName() string {
	return "lq_schedule_leader"
}
