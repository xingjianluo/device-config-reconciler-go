package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
	"github.com/xingjianluo/device-config-reconciler-go/internal/store"
)

// fakeSleeper 记录退避请求但不真正等待，让重试路径在测试中立即可跑完。
type fakeSleeper struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (f *fakeSleeper) Sleep(_ context.Context, d time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delays = append(f.delays, d)
	return nil
}

func (f *fakeSleeper) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delays)
}

func setup(t *testing.T, opts ...Option) (*store.DesiredStore, *store.DeviceStore, *Reconciler, *fakeSleeper) {
	t.Helper()
	d := store.NewDesiredStore()
	dev := store.NewDeviceStore()
	fs := &fakeSleeper{}
	all := append([]Option{WithSleeper(fs), WithBackoff(time.Millisecond, 10*time.Millisecond)}, opts...)
	return d, dev, New(d, dev, all...), fs
}

func upsert(eventID, dev string, revision int64, cfg map[string]string) domain.Event {
	return domain.Event{EventID: eventID, DeviceID: dev, Type: domain.EventUpsert, Revision: revision, Config: cfg}
}

func del(eventID, dev string, revision int64) domain.Event {
	return domain.Event{EventID: eventID, DeviceID: dev, Type: domain.EventDelete, Revision: revision}
}

func TestHandleEvent_CreateConverges(t *testing.T) {
	t.Logf("触发条件: 首次 Upsert。")
	t.Logf("判定依据: 结果 Converged、设备版本=期望版本、记账恰好 1 条 Apply。")
	_, dev, rec, _ := setup(t)
	res := rec.HandleEvent(context.Background(), upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	if res.Status != domain.StatusConverged || res.AppliedVersion != 1 || res.Attempts != 1 {
		t.Fatalf("judgment: first upsert must converge in 1 attempt, got %+v", res)
	}
	st, _ := dev.Get("d1")
	if !st.Present || st.AppliedVersion != 1 || st.Config["vlan"] != "10" {
		t.Fatalf("judgment: device state must equal desired v1, got %+v", st)
	}
	if l := rec.Ledger("d1"); len(l) != 1 || l[0].Op != domain.OpApply {
		t.Fatalf("judgment: exactly one Apply ledger entry, got %+v", l)
	}
}

func TestHandleEvent_DuplicateEventIsIdempotent(t *testing.T) {
	t.Logf("触发条件: Create 后重复投递同一事件 3 次（含相同 EventID）。")
	t.Logf("判定依据: 每次结果一致(Converged/v1)，设备 Apply 只发生 1 次，记账不重复。")
	_, dev, rec, _ := setup(t)
	ev := upsert("e-dup", "d1", 1, map[string]string{"vlan": "10"})
	first := rec.HandleEvent(context.Background(), ev)
	for i := 0; i < 3; i++ {
		r := rec.HandleEvent(context.Background(), ev)
		if r != first {
			t.Fatalf("judgment: duplicate event #%d must replay identical result %+v, got %+v", i, first, r)
		}
	}
	if calls := dev.ApplyCalls("d1"); calls != 1 {
		t.Fatalf("judgment: device Apply must happen exactly once across duplicate events, got %d", calls)
	}
	if l := rec.Ledger("d1"); len(l) != 1 {
		t.Fatalf("judgment: ledger must not double-count, got %d entries", len(l))
	}
}

func TestHandleEvent_UpdateConvergesToNewVersion(t *testing.T) {
	t.Logf("触发条件: v1 -> v2 两次 Upsert。")
	t.Logf("判定依据: 第二次产生新版本并下发；重复投递 v2 事件不再下发。")
	_, dev, rec, _ := setup(t)
	r1 := rec.HandleEvent(context.Background(), upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	r2 := rec.HandleEvent(context.Background(), upsert("e2", "d1", 2, map[string]string{"vlan": "20"}))
	if r2.AppliedVersion != 2 {
		t.Fatalf("judgment: update must apply v2, got %+v", r2)
	}
	st, _ := dev.Get("d1")
	if st.AppliedVersion != 2 || st.Config["vlan"] != "20" {
		t.Fatalf("judgment: device must reflect v2 content, got %+v", st)
	}
	dup := rec.HandleEvent(context.Background(), upsert("e2", "d1", 2, map[string]string{"vlan": "20"}))
	if dup.Status != domain.StatusConverged || dev.ApplyCalls("d1") != 2 {
		t.Fatalf("judgment: repeated v2 event must be a no-op apply, calls=%d res=%+v", dev.ApplyCalls("d1"), dup)
	}
	_ = r1
}

// 触发条件：v2 已收敛后手动篡改设备内容（版本不变、内容漂移），再执行 Resync。
// 判定依据：结果 Converged；设备内容恢复为期望；记账新增 1 条 Drift（不是 Apply）；
// 再来一轮 Resync 必须无动作（不重复记账）。
func TestResync_DriftCorrection(t *testing.T) {
	t.Logf("触发条件: v2 收敛后手动篡改设备内容(版本不变、内容漂移)，再执行 Resync")
	t.Logf("判定依据: 结果 Converged；设备恢复期望；新增 1 条 Drift 记账；再来一轮 Resync 无动作不重复记账")
	_, dev, rec, _ := setup(t)
	ctx := context.Background()
	rec.HandleEvent(ctx, upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	rec.HandleEvent(ctx, upsert("e2", "d1", 2, map[string]string{"vlan": "20"}))

	dev.Mutate("d1", map[string]string{"vlan": "999", "hostname": "tampered"})
	if st, _ := dev.Get("d1"); st.Config["vlan"] != "999" {
		t.Fatalf("precondition: device must be drifted, got %+v", st)
	}

	results := rec.Resync(ctx)
	if len(results) != 1 || results[0].Status != domain.StatusConverged || results[0].AppliedVersion != 2 {
		t.Fatalf("judgment: resync must converge drift back to v2, got %+v", results)
	}
	st, _ := dev.Get("d1")
	if st.AppliedVersion != 2 || st.Config["vlan"] != "20" {
		t.Fatalf("judgment: device content restored to desired v2, got %+v", st)
	}
	l := rec.Ledger("d1")
	if len(l) != 3 || l[2].Op != domain.OpDrift {
		t.Fatalf("judgment: correction must be booked once as Drift (apply,apply,drift), got %+v", l)
	}

	again := rec.Resync(ctx)
	if again[0].Status != domain.StatusConverged || len(rec.Ledger("d1")) != 3 {
		t.Fatalf("judgment: second resync on converged state must be a no-op, ledger=%+v", rec.Ledger("d1"))
	}
}

// 触发条件：删除一个已收敛的对象。
// 判定依据：删除成功前设备执行过 Cleanup；成功后设备 Present=false 且期望墓碑被 Purge；
// 结果 Status=Deleted；重复投递删除事件仍返回 Deleted 且不会再次 Cleanup。
func TestHandleEvent_DeleteWaitsForCleanup(t *testing.T) {
	t.Logf("触发条件: 删除一个已收敛的对象")
	t.Logf("判定依据: 设备先 Cleanup(1次) 成功后 Present=false 且墓碑 Purge；结果 Deleted；重复删除不再次 Cleanup")
	d, dev, rec, _ := setup(t)
	ctx := context.Background()
	rec.HandleEvent(ctx, upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))

	del := del("e-del", "d1", 2)
	res := rec.HandleEvent(ctx, del)
	if res.Status != domain.StatusDeleted {
		t.Fatalf("judgment: delete completes only after cleanup, got %+v", res)
	}
	if calls := dev.CleanupCalls("d1"); calls != 1 {
		t.Fatalf("judgment: exactly one cleanup call, got %d", calls)
	}
	if st, ok := dev.Get("d1"); !ok || st.Present {
		t.Fatalf("judgment: device config must be absent after cleanup, got %+v ok=%v", st, ok)
	}
	if _, ok := d.Get("d1"); ok {
		t.Fatalf("judgment: desired tombstone must be purged only after device cleanup")
	}
	l := rec.Ledger("d1")
	if len(l) != 2 || l[1].Op != domain.OpClean {
		t.Fatalf("judgment: ledger must end with one Clean entry, got %+v", l)
	}

	dup := rec.HandleEvent(ctx, del)
	if dup.Status != domain.StatusDeleted || dev.CleanupCalls("d1") != 1 {
		t.Fatalf("judgment: duplicate delete is idempotent and must not clean again, calls=%d", dev.CleanupCalls("d1"))
	}
}

func TestHandleEvent_DeleteMissingIsIdempotent(t *testing.T) {
	t.Logf("触发条件: 删除一个从不存在的对象。")
	t.Logf("判定依据: 直接 Deleted（幂等），不触设备。")
	_, dev, rec, _ := setup(t)
	res := rec.HandleEvent(context.Background(), del("x", "ghost", 1))
	if res.Status != domain.StatusDeleted || dev.CleanupCalls("ghost") != 0 {
		t.Fatalf("judgment: deleting missing object must be an idempotent no-op, got %+v", res)
	}
}

// 触发条件：Apply 注入 2 次瞬时锁定，第 3 次恢复；MaxAttempts 足够大。
// 判定依据：最终 Converged，Attempts=3，发生 2 次退避等待；设备 Apply 调用 3 次但记账只 1 条
// （失败不记账，成功只记一次），事件不丢。
func TestHandleEvent_TransientFailureRetriedWithBackoff(t *testing.T) {
	t.Logf("触发条件: Apply 注入 2 次瞬时锁定、第 3 次恢复，MaxAttempts=5")
	t.Logf("判定依据: 最终 Converged/Attempts=3/2 次退避；Apply 调用 3 次但成功只记账 1 条，事件不丢")
	_, dev, rec, fs := setup(t, WithMaxAttempts(5))
	dev.InjectApplyFault("d1", store.Fault{Remaining: 2, Err: domain.ErrLocked})

	res := rec.HandleEvent(context.Background(), upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	if res.Status != domain.StatusConverged || res.Attempts != 3 {
		t.Fatalf("judgment: transient faults must be retried until success, got %+v", res)
	}
	if fs.count() != 2 {
		t.Fatalf("judgment: exactly two backoff waits between three attempts, got %d", fs.count())
	}
	if dev.ApplyCalls("d1") != 3 {
		t.Fatalf("judgment: device must be called on every attempt (3), got %d", dev.ApplyCalls("d1"))
	}
	if l := rec.Ledger("d1"); len(l) != 1 || l[0].Op != domain.OpApply {
		t.Fatalf("judgment: failed attempts must not be booked; exactly one Apply on success, got %+v", l)
	}
}

// 触发条件：Apply 持续返回瞬时不可用，MaxAttempts=3。
// 判定依据：结果 Status=Transient（与 Terminal 可区分），Attempts=3，不缓存成功结论；
// 之后故障恢复，重新投递一个新事件即可收敛（事件不丢、可再推进）。
func TestHandleEvent_TransientFailureExhaustsAttempts(t *testing.T) {
	t.Logf("触发条件: Apply 持续瞬时不可用，MaxAttempts=3")
	t.Logf("判定依据: 结果 Transient(可与 Terminal 区分)/Attempts=3/不缓存成功；故障恢复后重新投递即收敛")
	_, dev, rec, fs := setup(t, WithMaxAttempts(3))
	dev.InjectApplyFault("d1", store.Fault{Remaining: -1, Err: domain.ErrUnavail})

	res := rec.HandleEvent(context.Background(), upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	if res.Status != domain.StatusTransient || res.Attempts != 3 {
		t.Fatalf("judgment: exhausted retries must report Transient, got %+v", res)
	}
	if res.Status == domain.StatusTerminal {
		t.Fatalf("judgment: transient exhaustion must be distinguishable from terminal failure")
	}
	if fs.count() != 2 {
		t.Fatalf("judgment: 3 attempts => 2 backoff waits, got %d", fs.count())
	}
	if len(rec.Ledger("d1")) != 0 {
		t.Fatalf("judgment: nothing succeeded, ledger must stay empty, got %+v", rec.Ledger("d1"))
	}

	// 恢复后用新事件再投递：必须能收敛。
	dev.InjectApplyFault("d1", store.Fault{Remaining: 0})
	res2 := rec.HandleEvent(context.Background(), upsert("e1-retry", "d1", 1, map[string]string{"vlan": "10"}))
	if res2.Status != domain.StatusConverged {
		t.Fatalf("judgment: redelivered event after recovery must converge, got %+v", res2)
	}
}

func TestHandleEvent_DeleteRetriesTransientCleanupFault(t *testing.T) {
	t.Logf("触发条件: 删除清理时注入 1 次瞬时锁定。")
	t.Logf("判定依据: 退避重试后 Deleted；Cleanup 调用 2 次；墓碑只在清理成功后 Purge。")
	d, dev, rec, _ := setup(t, WithMaxAttempts(3))
	ctx := context.Background()
	rec.HandleEvent(ctx, upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))
	dev.InjectCleanupFault("d1", store.Fault{Remaining: 1, Err: domain.ErrLocked})

	res := rec.HandleEvent(ctx, del("e-del", "d1", 2))
	if res.Status != domain.StatusDeleted || res.Attempts != 2 {
		t.Fatalf("judgment: delete must retry transient cleanup fault then succeed, got %+v", res)
	}
	if dev.CleanupCalls("d1") != 2 {
		t.Fatalf("judgment: cleanup attempted twice, got %d", dev.CleanupCalls("d1"))
	}
	if _, ok := d.Get("d1"); ok {
		t.Fatalf("judgment: tombstone purged only after successful cleanup")
	}
}

// 触发条件：Upsert 携带越界 vlan（永久失败）。
// 判定依据：立即 Terminal、Reason=OutOfRange、Attempts=1、无退避等待、不触设备；
// 期望存储中不落任何版本；重复投递同一事件结果完全一致且仍不触设备。
func TestHandleEvent_PermanentFailureStopsImmediately(t *testing.T) {
	t.Logf("触发条件: Upsert 携带越界 vlan(永久失败)")
	t.Logf("判定依据: 立即 Terminal/Reason=OutOfRange/Attempts=1/无退避/不触设备/不落版本；重复投递结果一致仍不触设备")
	d, dev, rec, fs := setup(t)
	ev := upsert("e-bad", "d1", 1, map[string]string{"vlan": "9999"})

	res := rec.HandleEvent(context.Background(), ev)
	if res.Status != domain.StatusTerminal {
		t.Fatalf("judgment: out-of-range must be Terminal, got %+v", res)
	}
	if res.TerminalReason != domain.ReasonOutOfRange {
		t.Fatalf("judgment: reason must be OutOfRange to distinguish failure class, got %q", res.TerminalReason)
	}
	if res.Attempts != 1 || fs.count() != 0 {
		t.Fatalf("judgment: permanent failure needs no retry/backoff, attempts=%d waits=%d", res.Attempts, fs.count())
	}
	if dev.ApplyCalls("d1") != 0 {
		t.Fatalf("judgment: permanent failure must never touch the device, calls=%d", dev.ApplyCalls("d1"))
	}
	if _, ok := d.Get("d1"); ok {
		t.Fatalf("judgment: invalid config must not be persisted as a desired version")
	}

	dup := rec.HandleEvent(context.Background(), ev)
	if dup != res || dev.ApplyCalls("d1") != 0 {
		t.Fatalf("judgment: duplicate permanent-failure event replays identical result, first=%+v dup=%+v", res, dup)
	}
}

func TestHandleEvent_NewVersionClearsTerminalConclusion(t *testing.T) {
	t.Logf("触发条件: 永久失败（vlan 非数字 => InvalidField）后，修正配置发新版本 Upsert。")
	t.Logf("判定依据: 新版本清除旧的永久失败结论并正常收敛。")
	_, dev, rec, _ := setup(t)
	ctx := context.Background()
	bad := rec.HandleEvent(ctx, upsert("e1", "d1", 1, map[string]string{"vlan": "abc"}))
	if bad.Status != domain.StatusTerminal || bad.TerminalReason != domain.ReasonInvalidField {
		t.Fatalf("judgment: non-integer vlan is InvalidField, got %+v", bad)
	}
	good := rec.HandleEvent(ctx, upsert("e2", "d1", 2, map[string]string{"vlan": "10"}))
	if good.Status != domain.StatusConverged || good.AppliedVersion != 2 {
		t.Fatalf("judgment: corrected upsert (rev2) must converge and not be blocked by stale terminal, got %+v", good)
	}
	if dev.ApplyCalls("d1") != 1 {
		t.Fatalf("judgment: only the valid event reaches the device, got %d", dev.ApplyCalls("d1"))
	}
}

func TestHandleEvent_ConcurrentSameEventSingleFlight(t *testing.T) {
	t.Logf("触发条件: 同一个事件被 N 个 goroutine 同时投递（相同 EventID）。")
	t.Logf("判定依据: 设备 Apply 只发生 1 次、记账 1 条；所有 goroutine 拿到完全相同的结果。")
	_, dev, rec, _ := setup(t)
	const n = 32
	var wg sync.WaitGroup
	results := make([]domain.Result, n)
	start := make(chan struct{})
	ev := upsert("e-conc", "d1", 1, map[string]string{"vlan": "10"})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = rec.HandleEvent(context.Background(), ev)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if r.EventID != "e-conc" || r.Status != domain.StatusConverged || r.AppliedVersion != 1 {
			t.Fatalf("judgment: waiter %d must share the converged result, got %+v", i, r)
		}
	}
	if calls := dev.ApplyCalls("d1"); calls != 1 {
		t.Fatalf("judgment: concurrent identical events must apply exactly once, got %d", calls)
	}
	if l := rec.Ledger("d1"); len(l) != 1 {
		t.Fatalf("judgment: concurrent identical events must book exactly once, got %d", len(l))
	}
}

// 触发条件：同一设备的 v1/v2/v3 三个不同事件由 N 个工作单元并发处理，
// 每个事件还各被重复投递若干次。
// 判定依据：无论调度顺序如何，最终设备状态等于最高版本 v3；实际下发的版本严格单调递增、
// 不重复、不回退（旧版本事件永不覆盖新版本，因此不会出现"高版本号+旧内容"）；-race 无竞争。
func TestHandleEvent_ConcurrentEventsSameDeviceNoDoubleApply(t *testing.T) {
	t.Logf("触发条件: 同一设备 v1/v2/v3 事件由多工作单元并发、各重复投递")
	t.Logf("判定依据: 无论调度如何最终为 v3；下发版本严格单调递增不重复不回退；设备调用与记账一一对应；-race 无竞争")
	_, dev, rec, _ := setup(t)
	ctx := context.Background()
	events := []domain.Event{
		upsert("e1", "d1", 1, map[string]string{"vlan": "10"}),
		upsert("e2", "d1", 2, map[string]string{"vlan": "20"}),
		upsert("e3", "d1", 3, map[string]string{"vlan": "30"}),
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for rep := 0; rep < 8; rep++ {
		for _, ev := range events {
			wg.Add(1)
			go func(ev domain.Event) {
				defer wg.Done()
				<-start
				rec.HandleEvent(ctx, ev)
			}(ev)
		}
	}
	close(start)
	wg.Wait()

	st, ok := dev.Get("d1")
	if !ok || !st.Present || st.AppliedVersion != 3 || st.Config["vlan"] != "30" {
		t.Fatalf("judgment: final state must be v3 vlan=30 regardless of scheduling, got %+v", st)
	}
	// 记账中的下发版本必须严格单调递增且不重复（乱序的旧版本直接跳过，不会重复/降级下发）。
	l := rec.Ledger("d1")
	var prev int64
	seen := map[int64]bool{}
	for _, e := range l {
		if e.Op != domain.OpApply {
			t.Fatalf("judgment: upsert stream must only book Apply, got %+v", l)
		}
		if e.Version <= prev || seen[e.Version] {
			t.Fatalf("judgment: applied versions must be strictly increasing and unique, got %+v", l)
		}
		prev, seen[e.Version] = e.Version, true
	}
	if prev != 3 {
		t.Fatalf("judgment: last applied version must be the highest v3, got ledger %+v", l)
	}
	if calls := dev.ApplyCalls("d1"); calls != len(l) {
		t.Fatalf("judgment: every device call must be booked exactly once, calls=%d ledger=%d", calls, len(l))
	}
}

func TestHandleEvent_ConcurrentDifferentDevices(t *testing.T) {
	t.Logf("触发条件: 不同设备的事件并发处理。")
	t.Logf("判定依据: 互不阻塞，最终各自收敛；Apply 每设备各 1 次。")
	_, dev, rec, _ := setup(t)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		devID := "d" + string(rune('a'+i%26)) + "-" + itoa(i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			r := rec.HandleEvent(context.Background(), upsert("e-"+id, id, 1, map[string]string{"vlan": "10"}))
			if r.Status != domain.StatusConverged {
				t.Errorf("judgment: device %s must converge, got %+v", id, r)
			}
		}(devID)
	}
	close(start)
	wg.Wait()
	if dev.ApplyCalls("da-0") != 1 {
		t.Fatalf("judgment: each independent device applied once")
	}
}

// 触发条件：删除事件与再同步并发执行。
// 判定依据：分片锁串行化两者，最终要么删除完成（设备缺席、墓碑移除），
// 要么收敛为最新期望；不能出现“墓碑还在但设备已清理”之外的中间悬挂态。
func TestHandleEvent_ConcurrentDeleteAndResync(t *testing.T) {
	t.Logf("触发条件: 删除事件与再同步并发执行")
	t.Logf("判定依据: 分片锁串行化：删除胜出则设备已清理且墓碑移除；再同步胜出则设备仍为期望配置，无悬挂中间态")
	d, dev, rec, _ := setup(t)
	ctx := context.Background()
	rec.HandleEvent(ctx, upsert("e1", "d1", 1, map[string]string{"vlan": "10"}))

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		rec.HandleEvent(ctx, del("e-del", "d1", 2))
	}()
	go func() {
		defer wg.Done()
		<-start
		rec.Resync(ctx)
	}()
	close(start)
	wg.Wait()

	_, desiredExists := d.Get("d1")
	st, deviceExists := dev.Get("d1")
	if desiredExists {
		// 若墓碑仍在，设备必须仍呈现期望配置（再同步胜出的自洽态）。
		if !deviceExists || !st.Present || st.Config["vlan"] != "10" {
			t.Fatalf("judgment: if desired remains, device must still hold desired config, got %+v", st)
		}
	} else {
		// 删除胜出：设备配置必须已清理。
		if deviceExists && st.Present {
			t.Fatalf("judgment: if tombstone purged, device must be cleaned, got %+v", st)
		}
	}
}

func TestRun_ProcessesEventStream(t *testing.T) {
	t.Logf("触发条件: 通过 Run 消费一串事件（含重复事件、删除事件）。")
	t.Logf("判定依据: 每个输入事件恰好产出一个结果，顺序一致，最终设备被清理。")
	_, dev, rec, _ := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	evs := make(chan domain.Event, 4)
	evs <- upsert("e1", "d1", 1, map[string]string{"vlan": "10"})
	evs <- upsert("e1", "d1", 1, map[string]string{"vlan": "10"})
	evs <- upsert("e2", "d1", 2, map[string]string{"vlan": "20"})
	evs <- del("e3", "d1", 3)
	close(evs)

	var got []domain.Result
	for r := range rec.Run(ctx, evs) {
		got = append(got, r)
	}
	if len(got) != 4 {
		t.Fatalf("judgment: one result per input event, got %d", len(got))
	}
	statuses := []domain.ReconcileStatus{got[0].Status, got[1].Status, got[2].Status, got[3].Status}
	want := []domain.ReconcileStatus{
		domain.StatusConverged, domain.StatusConverged, domain.StatusConverged, domain.StatusDeleted,
	}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("judgment: result[%d]=%s want %s (all=%v)", i, statuses[i], want[i], statuses)
		}
	}
	if st, _ := dev.Get("d1"); st.Present {
		t.Fatalf("judgment: stream ending in delete leaves device cleaned")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
