// Package reconcile 实现本地设备配置协调引擎：
// 期望配置与设备实际状态的收敛、漂移纠正、删除清理、
// 并发去重（单飞 + 每设备串行）、事件幂等、错误分类与退避重试。
package reconcile

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
	"github.com/xingjianluo/device-config-reconciler-go/internal/store"
)

// 默认调参。
const (
	defaultShards    = 16
	defaultBaseDelay = 10 * time.Millisecond
	defaultMaxDelay  = time.Second
)

// Sleeper 抽象退避等待，便于测试中用假时钟立即推进。
type Sleeper interface {
	Sleep(ctx context.Context, d time.Duration) error
}

type realSleeper struct{}

func (realSleeper) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Config 协调引擎可调参数。
type Config struct {
	// MaxAttempts 瞬时失败最大尝试次数；0 表示不限（仅靠 ctx 取消终止）。
	MaxAttempts int
	// BaseDelay 指数退避初始延迟。
	BaseDelay time.Duration
	// MaxDelay 单次退避上限。
	MaxDelay time.Duration
	Sleeper  Sleeper
}

// Option 配置可选项。
type Option func(*Config)

// WithMaxAttempts 设置瞬时失败最大尝试次数。
func WithMaxAttempts(n int) Option { return func(c *Config) { c.MaxAttempts = n } }

// WithBackoff 设置退避参数。
func WithBackoff(base, max time.Duration) Option {
	return func(c *Config) { c.BaseDelay, c.MaxDelay = base, max }
}

// WithSleeper 注入退避等待实现（测试用）。
func WithSleeper(s Sleeper) Option { return func(c *Config) { c.Sleeper = s } }

// terminalCache 记录某个设备在某个期望版本上的永久失败结论；
// 版本变化（新 Upsert）后旧结论自动失效。
type terminalCache struct {
	version int64
	result  domain.Result
}

// Reconciler 协调引擎。
type Reconciler struct {
	desired *store.DesiredStore
	devices *store.DeviceStore
	cfg     Config

	// eventMu 保证“同一 EventID 只真正处理一次”：并发重复事件单飞共享结果。
	eventMu   sync.Mutex
	inflight  map[string]*eventCall
	eventDone map[string]domain.Result

	// 每设备分片锁：保证同一设备的协调动作串行（不同设备可并行）。
	shards   uint32
	shardMu  [defaultShards]sync.Mutex

	// ledgerMu 记账。
	ledgerMu sync.Mutex
	ledger   map[string][]domain.LedgerEntry

	// termMu 永久失败结论缓存。
	termMu sync.Mutex
	term   map[string]terminalCache
}

type eventCall struct {
	done   chan struct{}
	result domain.Result
}

// New 创建协调引擎。
func New(desired *store.DesiredStore, devices *store.DeviceStore, opts ...Option) *Reconciler {
	cfg := Config{
		MaxAttempts: 0,
		BaseDelay:   defaultBaseDelay,
		MaxDelay:    defaultMaxDelay,
		Sleeper:     realSleeper{},
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = defaultBaseDelay
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = defaultMaxDelay
	}
	if cfg.Sleeper == nil {
		cfg.Sleeper = realSleeper{}
	}
	return &Reconciler{
		desired:   desired,
		devices:   devices,
		cfg:       cfg,
		shards:    defaultShards,
		inflight:  make(map[string]*eventCall),
		eventDone: make(map[string]domain.Result),
		ledger:    make(map[string][]domain.LedgerEntry),
		term:      make(map[string]terminalCache),
	}
}

// lockDevice 取设备分片锁。
func (r *Reconciler) lockDevice(deviceID string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID))
	idx := h.Sum32() % r.shards
	r.shardMu[idx].Lock()
	return r.shardMu[idx].Unlock
}

// HandleEvent 处理单个 Upsert/Delete 事件。
// 重复投递同一 EventID 时返回与首次完全一致的结果（幂等）；
// 并发投递同一 EventID 时只真正处理一次，并发方共享同一结果。
func (r *Reconciler) HandleEvent(ctx context.Context, e domain.Event) domain.Result {
	// 已完成的事件：直接回放首次结果。
	r.eventMu.Lock()
	if res, ok := r.eventDone[e.EventID]; ok {
		r.eventMu.Unlock()
		return res
	}
	if call, ok := r.inflight[e.EventID]; ok {
		// 单飞：等待先到者处理完。
		r.eventMu.Unlock()
		select {
		case <-ctx.Done():
			return domain.Result{EventID: e.EventID, DeviceID: e.DeviceID, Status: domain.StatusTransient}
		case <-call.done:
			return call.result
		}
	}
	call := &eventCall{done: make(chan struct{})}
	r.inflight[e.EventID] = call
	r.eventMu.Unlock()

	result := r.processEvent(ctx, e)

	r.eventMu.Lock()
	call.result = result
	close(call.done)
	delete(r.inflight, e.EventID)
	// 永久失败与收敛/删除结论都缓存，保证重复投递结果一致；
	// 瞬时失败不缓存（调用方需要靠重试再次投递推进）。
	if result.Status != domain.StatusTransient {
		r.eventDone[e.EventID] = result
	}
	r.eventMu.Unlock()
	return result
}

// processEvent 在设备分片锁内写入期望存储并执行带重试的协调。
func (r *Reconciler) processEvent(ctx context.Context, e domain.Event) domain.Result {
	unlock := r.lockDevice(e.DeviceID)
	defer unlock()

	var target domain.DesiredConfig
	switch e.Type {
	case domain.EventUpsert:
		// 参数级永久失败：不入存储、不触设备，立即终止。
		if err := validate(e.DeviceID, e.Revision, e.Config); err != nil {
			res := terminalResult(e.EventID, e.DeviceID, 0, err)
			res.Attempts = 1 // 已处理一次，在触达设备前判定为永久失败
			r.saveTerminal(e.DeviceID, res)
			return res
		}
		dc, _, accepted := r.desired.Put(e.DeviceID, e.Revision, e.Config)
		if !accepted {
			// 乱序/迟到的旧版本事件：不覆盖新期望，改为向当前最新版本收敛。
			latest, ok := r.desired.Get(e.DeviceID)
			if !ok {
				return domain.Result{EventID: e.EventID, DeviceID: e.DeviceID,
					Status: domain.StatusConverged, AppliedVersion: 0}
			}
			dc = latest
		}
		target = dc
	case domain.EventDelete:
		dc, ok := r.desired.Get(e.DeviceID)
		if !ok {
			// 删除不存在的对象：幂等成功。
			return domain.Result{EventID: e.EventID, DeviceID: e.DeviceID, Status: domain.StatusDeleted}
		}
		if dc.Phase != domain.PhaseDeleting {
			ndc, err, _ := r.desired.MarkDeleting(e.DeviceID, e.Revision)
			if err != nil {
				// 迟到的旧删除：不能删除更新的期望，改为向当前最新期望收敛。
				latest, exists := r.desired.Get(e.DeviceID)
				if !exists {
					return domain.Result{EventID: e.EventID, DeviceID: e.DeviceID,
						Status: domain.StatusDeleted}
				}
				target = latest
				break
			}
			dc = ndc
		}
		target = dc
	default:
		return domain.Result{EventID: e.EventID, DeviceID: e.DeviceID,
			Status: domain.StatusTerminal, TerminalReason: domain.ReasonInvalidParameter,
			TerminalMessage: "unknown event type: " + string(e.Type)}
	}

	// 新版本到来会清除上一版本的永久失败结论。
	r.clearTerminalIfStale(e.DeviceID, target.ResourceVersion)

	return r.reconcileWithRetry(ctx, e.EventID, target)
}

// reconcileWithRetry 针对一个期望目标做“带分类重试”的协调。
func (r *Reconciler) reconcileWithRetry(ctx context.Context, eventID string, target domain.DesiredConfig) domain.Result {
	// 若该版本已有永久失败结论，直接回放（同一轮重复事件不再触设备）。
	if cached, ok := r.getTerminal(target.DeviceID, target.ResourceVersion); ok {
		res := cached
		res.EventID = eventID
		return res
	}

	var attempt int
	delay := r.cfg.BaseDelay
	for {
		attempt++
		res, err := r.reconcileOnce(ctx, eventID, target)
		if err == nil {
			res.Attempts = attempt
			return res
		}
		re, classified := domain.AsReconcileError(err)
		if classified && !re.Transient {
			// 永久失败：立即停止重试并记录原因。
			out := domain.Result{
				EventID:         eventID,
				DeviceID:        target.DeviceID,
				Status:          domain.StatusTerminal,
				Attempts:        attempt,
				TerminalReason:  re.Reason,
				TerminalMessage: re.Message,
			}
			if target.Phase == domain.PhaseActive {
				out.AppliedVersion = target.ResourceVersion
			}
			r.saveTerminal(target.DeviceID, out)
			return out
		}
		// 瞬时失败：退避重试；到达上限或上下文结束则返回 Transient（事件不丢，可再投递/再同步）。
		if r.cfg.MaxAttempts > 0 && attempt >= r.cfg.MaxAttempts {
			return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
				Status: domain.StatusTransient, Attempts: attempt,
				TerminalMessage: transientMsg(err)}
		}
		if err := r.cfg.Sleeper.Sleep(ctx, delay); err != nil {
			return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
				Status: domain.StatusTransient, Attempts: attempt,
				TerminalMessage: transientMsg(err)}
		}
		delay *= 2
		if delay > r.cfg.MaxDelay {
			delay = r.cfg.MaxDelay
		}
	}
}

func transientMsg(err error) string {
	if err == nil {
		return "transient failure"
	}
	return "transient: " + err.Error()
}

// reconcileOnce 执行单次协调动作。返回的 error 为设备错误（已分类）。
func (r *Reconciler) reconcileOnce(ctx context.Context, eventID string, target domain.DesiredConfig) (domain.Result, error) {
	if err := ctx.Err(); err != nil {
		return domain.Result{}, err
	}
	state, exists := r.devices.Get(target.DeviceID)

	if target.Phase == domain.PhaseDeleting {
		// 设备已无配置：清理幂等完成，移除墓碑，删除才算真正结束。
		if !exists || !state.Present {
			if err := r.desired.Purge(target.DeviceID); err != nil {
				return domain.Result{}, &domain.ReconcileError{Transient: true, Message: "purge: " + err.Error()}
			}
			r.clearTerminal(target.DeviceID)
			return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
				Status: domain.StatusDeleted, AppliedVersion: 0}, nil
		}
		if err := r.devices.Cleanup(target.DeviceID); err != nil {
			return domain.Result{}, err
		}
		r.appendLedger(domain.LedgerEntry{
			DeviceID: target.DeviceID, Op: domain.OpClean,
			Version: target.ResourceVersion, Detail: "cleanup before purge",
		})
		if err := r.desired.Purge(target.DeviceID); err != nil {
			return domain.Result{}, &domain.ReconcileError{Transient: true, Message: "purge: " + err.Error()}
		}
		r.clearTerminal(target.DeviceID)
		return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
			Status: domain.StatusDeleted}, nil
	}

	// Active：版本一致且内容相同 => 已收敛，无动作（不记账）。
	if exists && state.Present && state.AppliedVersion == target.ResourceVersion &&
		configEqual(state.Config, target.Config) {
		return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
			Status: domain.StatusConverged, AppliedVersion: target.ResourceVersion}, nil
	}

	op := domain.OpApply
	detail := "initial apply"
	switch {
	case !exists || !state.Present:
		op, detail = domain.OpApply, "apply to absent device"
	case state.AppliedVersion < target.ResourceVersion:
		op, detail = domain.OpApply, fmt.Sprintf("update v%d -> v%d", state.AppliedVersion, target.ResourceVersion)
	case state.AppliedVersion == target.ResourceVersion && !configEqual(state.Config, target.Config):
		op, detail = domain.OpDrift, "manual drift correction at v" + strconv.FormatInt(target.ResourceVersion, 10)
	default: // 版本更高（理论上不会出现）也按下发纠正
		op, detail = domain.OpApply, "roll forward apply"
	}

	if err := r.devices.Apply(target.DeviceID, target.ResourceVersion, target.Config); err != nil {
		return domain.Result{}, err
	}
	r.appendLedger(domain.LedgerEntry{
		DeviceID: target.DeviceID, Op: op,
		Version: target.ResourceVersion, Detail: detail,
	})
	r.clearTerminal(target.DeviceID)
	return domain.Result{EventID: eventID, DeviceID: target.DeviceID,
		Status: domain.StatusConverged, AppliedVersion: target.ResourceVersion}, nil
}

// Resync 对期望存储中的全部对象执行一轮再同步，纠正手动漂移、补做删除清理。
// 按设备加锁，因此与并发事件不会互相覆盖。
func (r *Reconciler) Resync(ctx context.Context) []domain.Result {
	results := make([]domain.Result, 0)
	for _, id := range r.desired.List() {
		target, ok := r.desired.Get(id)
		if !ok {
			continue
		}
		unlock := r.lockDevice(id)
		res := r.reconcileWithRetry(ctx, "resync:"+id+"#"+strconv.FormatInt(target.ResourceVersion, 10), target)
		unlock()
		results = append(results, res)
	}
	return results
}

// Run 从 events 消费事件并把结果写入返回的只读通道（每事件一个结果），
// 事件串行消费；同一事件内部已做单飞，调用方仍可并发调用 HandleEvent。
func (r *Reconciler) Run(ctx context.Context, events <-chan domain.Event) <-chan domain.Result {
	out := make(chan domain.Result)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-events:
				if !ok {
					return
				}
				select {
				case out <- r.HandleEvent(ctx, e):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Ledger 返回某设备实际设备动作的记账副本。
func (r *Reconciler) Ledger(deviceID string) []domain.LedgerEntry {
	r.ledgerMu.Lock()
	defer r.ledgerMu.Unlock()
	src := r.ledger[deviceID]
	out := make([]domain.LedgerEntry, len(src))
	copy(out, src)
	return out
}

func (r *Reconciler) appendLedger(e domain.LedgerEntry) {
	r.ledgerMu.Lock()
	defer r.ledgerMu.Unlock()
	r.ledger[e.DeviceID] = append(r.ledger[e.DeviceID], e)
}

func (r *Reconciler) saveTerminal(deviceID string, res domain.Result) {
	r.termMu.Lock()
	defer r.termMu.Unlock()
	r.term[deviceID] = terminalCache{version: res.AppliedVersion, result: res}
}

func (r *Reconciler) getTerminal(deviceID string, version int64) (domain.Result, bool) {
	r.termMu.Lock()
	defer r.termMu.Unlock()
	c, ok := r.term[deviceID]
	if !ok || c.version != version {
		return domain.Result{}, false
	}
	return c.result, true
}

func (r *Reconciler) clearTerminal(deviceID string) {
	r.termMu.Lock()
	defer r.termMu.Unlock()
	delete(r.term, deviceID)
}

func (r *Reconciler) clearTerminalIfStale(deviceID string, version int64) {
	r.termMu.Lock()
	defer r.termMu.Unlock()
	if c, ok := r.term[deviceID]; ok && c.version != version {
		delete(r.term, deviceID)
	}
}

func terminalResult(eventID, deviceID string, version int64, err error) domain.Result {
	re, _ := domain.AsReconcileError(err)
	reason := domain.ReasonInvalidParameter
	msg := err.Error()
	if re != nil {
		if re.Reason != "" {
			reason = re.Reason
		}
		msg = re.Message
	}
	return domain.Result{
		EventID: eventID, DeviceID: deviceID, Status: domain.StatusTerminal,
		AppliedVersion: version, TerminalReason: reason, TerminalMessage: msg,
	}
}

// configEqual 比较两份配置内容是否一致。
func configEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// validate 对事件参数做静态校验，违反规则属于永久失败。
// 规则：DeviceID 非空；Revision 为正；Config 非空；vlan（若提供）必须是 1..4094 的整数。
func validate(deviceID string, revision int64, cfg map[string]string) error {
	if deviceID == "" {
		return &domain.ReconcileError{Transient: false, Reason: domain.ReasonMissingField, Message: "deviceID is required"}
	}
	if revision <= 0 {
		return &domain.ReconcileError{Transient: false, Reason: domain.ReasonInvalidParameter,
			Message: "revision must be positive, got: " + strconv.FormatInt(revision, 10)}
	}
	if len(cfg) == 0 {
		return &domain.ReconcileError{Transient: false, Reason: domain.ReasonMissingField, Message: "config must not be empty"}
	}
	if raw, ok := cfg["vlan"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return &domain.ReconcileError{Transient: false, Reason: domain.ReasonInvalidField,
				Message: "vlan must be an integer, got: " + raw}
		}
		if n < 1 || n > 4094 {
			return &domain.ReconcileError{Transient: false, Reason: domain.ReasonOutOfRange,
				Message: "vlan out of range [1,4094]: " + raw}
		}
	}
	return nil
}
