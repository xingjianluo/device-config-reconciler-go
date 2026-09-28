// Package domain 定义设备配置协调组件使用的核心领域模型：
// 期望配置(DesiredConfig)、设备实际状态(DeviceState)、协调事件(Event)、
// 协调结果(Result) 以及错误分类。
package domain

import "errors"

// Phase 表示一个设备配置的生命周期阶段。
type Phase int

const (
	// PhaseActive 正常存在、期望生效中的配置。
	PhaseActive Phase = iota
	// PhaseDeleting 已发起删除，等待设备端清理动作完成。
	PhaseDeleting
	// PhaseNotFound 配置不存在（查询用）。
	PhaseNotFound
)

func (p Phase) String() string {
	switch p {
	case PhaseActive:
		return "Active"
	case PhaseDeleting:
		return "Deleting"
	default:
		return "NotFound"
	}
}

// TerminalReason 描述永久失败（不可重试）的原因分类。
type TerminalReason string

const (
	ReasonInvalidField     TerminalReason = "InvalidField"
	ReasonOutOfRange       TerminalReason = "OutOfRange"
	ReasonMissingField     TerminalReason = "MissingField"
	ReasonInvalidParameter TerminalReason = "InvalidParameter"
)

// DesiredConfig 是一份期望配置。ResourceVersion 由期望存储单调递增分配，
// 每次 Create/Update 产生新版本；Delete 会把 Phase 置为 Deleting 并保留墓碑。
type DesiredConfig struct {
	DeviceID        string
	ResourceVersion int64
	Phase           Phase
	// Config 期望下发到设备的配置内容。
	Config map[string]string
}

// DeviceState 是设备端的实际状态，由本地存储模拟。
type DeviceState struct {
	DeviceID string
	// AppliedVersion 当前实际状态对应的期望版本；未应用过任何配置时为 0。
	AppliedVersion int64
	// Config 设备当前实际配置。
	Config map[string]string
	// Present 设备上是否还存在该配置；清理动作完成后为 false。
	Present bool
}

// EventType 事件类型。
type EventType string

const (
	EventUpsert EventType = "Upsert"
	EventDelete EventType = "Delete"
)

// Event 是一次配置变更通知。事件是自描述的：
// Upsert 携带期望内容，Delete 表示发起删除。
// EventID 用于重复投递去重；Revision 是生产端按因果序单调递增分配的资源版本，
// 协调只向最高 Revision 收敛，乱序/迟到的旧版本不得覆盖新版本。
type Event struct {
	EventID  string
	DeviceID string
	Type     EventType
	// Revision 该事件对应的期望资源版本，必须 >0 且按设备单调递增。
	Revision int64
	// Upsert 时携带的期望配置。
	Config map[string]string
}

// ReconcileStatus 一次协调处理的结果状态。
type ReconcileStatus string

const (
	// StatusConverged 已收敛（含本次幂等无动作）。
	StatusConverged ReconcileStatus = "Converged"
	// StatusDeleted 删除清理已完成。
	StatusDeleted ReconcileStatus = "Deleted"
	// StatusTransient 瞬时失败，已按退避重试策略处理但本轮仍未成功（或需要重新入队）。
	StatusTransient ReconcileStatus = "Transient"
	// StatusTerminal 永久失败，已停止重试。
	StatusTerminal ReconcileStatus = "Terminal"
)

// Result 是处理单个事件的对外结果。
type Result struct {
	EventID         string
	DeviceID        string
	Status          ReconcileStatus
	AppliedVersion  int64
	Attempts        int
	TerminalReason  TerminalReason
	TerminalMessage string
}

// OpKind 记账条目的动作类别。
type OpKind string

const (
	OpApply  OpKind = "Apply"  // 下发/更新配置
	OpDrift  OpKind = "Drift"  // 漂移纠正
	OpClean  OpKind = "Clean"  // 删除清理
)

// LedgerEntry 是一次实际设备动作的记账条目（幂等收敛时不记账）。
type LedgerEntry struct {
	DeviceID string
	Op       OpKind
	Version  int64
	Detail   string
}

// ReconcileError 是设备端返回的、可分类的协调错误。
type ReconcileError struct {
	Transient bool
	Reason    TerminalReason
	Message   string
}

func (e *ReconcileError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// AsReconcileError 从错误中提取 *ReconcileError。
func AsReconcileError(err error) (*ReconcileError, bool) {
	var re *ReconcileError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// 预定义的典型错误，便于测试注入与分类判定。
var (
	ErrLocked    = &ReconcileError{Transient: true, Message: "device temporarily locked"}
	ErrUnavail   = &ReconcileError{Transient: true, Message: "device temporarily unavailable"}
	ErrBadField  = &ReconcileError{Transient: false, Reason: ReasonInvalidField, Message: "invalid config field"}
	ErrOutOfRng  = &ReconcileError{Transient: false, Reason: ReasonOutOfRange, Message: "parameter out of range"}
)
