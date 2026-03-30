package sqlite

import (
	"database/sql"
	"time"
)

type sqlNullString struct {
	sql.NullString
}

func (n *sqlNullString) Scan(value any) error {
	if err := n.NullString.Scan(value); err != nil {
		return err
	}
	return nil
}

// SupplementalLinkRecord 表示持久化在数据库中的补链记录。
type SupplementalLinkRecord struct {
	LinkUUID       string
	Listener       string
	Dialer         string
	Status         int
	FailureReason  string
	LastTransition time.Time
}

// PlannerMetrics 捕获补链调度器持久化的聚合计数和元数据。
type PlannerMetrics struct {
	Dispatched     uint64
	Success        uint64
	Failures       uint64
	Dropped        uint64
	Recycled       uint64
	QueueHigh      int
	LastFailure    string
	RepairAttempts uint64
	RepairSuccess  uint64
	RepairFailures uint64
}
