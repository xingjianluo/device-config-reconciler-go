package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
)

func TestDesiredStore_PutVersioningAndCopy(t *testing.T) {
	t.Logf("触发条件: 新建 + 更新同一设备。")
	t.Logf("判定依据: 资源版本单调递增，Get 返回内容的副本，对返回值的改写不能污染存储。")
	s := NewDesiredStore()

	d1, err, _ := s.Put("d1", 1, map[string]string{"vlan": "10"})
	if err != nil || d1.ResourceVersion != 1 || d1.Phase != domain.PhaseActive {
		t.Fatalf("first put: %+v err=%v", d1, err)
	}
	d1.Config["vlan"] = "999" // 篡改返回副本

	got, ok := s.Get("d1")
	if !ok {
		t.Fatalf("d1 should exist")
	}
	if got.Config["vlan"] != "10" {
		t.Fatalf("judgment: stored config must be isolated from returned copy, got %v", got.Config)
	}

	d2, _, _ := s.Put("d1", 2, map[string]string{"vlan": "20"})
	if d2.ResourceVersion != 2 {
		t.Fatalf("judgment: version must increment, got %d", d2.ResourceVersion)
	}
}

func TestDesiredStore_DeleteTombstoneLifecycle(t *testing.T) {
	t.Logf("触发条件: MarkDeleting -> Purge 的删除两段式流程。")
	t.Logf("判定依据: MarkDeleting 后墓碑仍在且 Phase=Deleting；Purge 后彻底不存在。")
	s := NewDesiredStore()
	if _, err, _ := s.MarkDeleting("missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("judgment: deleting a missing object must return ErrNotFound, got %v", err)
	}
	_, _, _ = s.Put("d1", 1, map[string]string{"vlan": "10"})

	dc, err, _ := s.MarkDeleting("d1", 1)
	if err != nil || dc.Phase != domain.PhaseDeleting {
		t.Fatalf("judgment: MarkDeleting keeps tombstone in Deleting, got %+v", dc)
	}
	if _, ok := s.Get("d1"); !ok {
		t.Fatalf("judgment: tombstone must remain readable during cleanup")
	}
	if err := s.Purge("d1"); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if _, ok := s.Get("d1"); ok {
		t.Fatalf("judgment: after Purge the object is gone")
	}
}

func TestDeviceStore_ApplyFaultConsumedAndCounted(t *testing.T) {
	t.Logf("触发条件: 给设备 Apply 注入 2 次锁定故障。")
	t.Logf("判定依据: 前 2 次调用报错且状态不变，第 3 次成功；调用计数为 3。")
	d := NewDeviceStore()
	d.InjectApplyFault("d1", Fault{Remaining: 2, Err: domain.ErrLocked})

	for i := 1; i <= 2; i++ {
		if err := d.Apply("d1", 1, map[string]string{"vlan": "1"}); !errors.Is(err, domain.ErrLocked) {
			t.Fatalf("judgment: attempt %d must hit injected lock fault, got %v", i, err)
		}
	}
	if _, ok := d.Get("d1"); ok {
		t.Fatalf("judgment: failed apply must not create device state")
	}
	if err := d.Apply("d1", 1, map[string]string{"vlan": "1"}); err != nil {
		t.Fatalf("judgment: third apply must succeed after fault budget exhausted, got %v", err)
	}
	if calls := d.ApplyCalls("d1"); calls != 3 {
		t.Fatalf("judgment: exactly 3 apply calls recorded, got %d", calls)
	}
}

// 触发条件：Cleanup 故障命中时不能清除设备配置。
func TestDeviceStore_CleanupFaultKeepsState(t *testing.T) {
	t.Logf("触发条件: 给 Cleanup 注入 1 次瞬时不可用故障")
	t.Logf("判定依据: 命中故障时返回错误且设备仍 Present；故障耗尽后 Cleanup 成功 Present=false")
	d := NewDeviceStore()
	if err := d.Apply("d1", 1, map[string]string{"vlan": "1"}); err != nil {
		t.Fatal(err)
	}
	d.InjectCleanupFault("d1", Fault{Remaining: 1, Err: domain.ErrUnavail})
	if err := d.Cleanup("d1"); !errors.Is(err, domain.ErrUnavail) {
		t.Fatalf("judgment: cleanup must surface transient fault, got %v", err)
	}
	st, ok := d.Get("d1")
	if !ok || !st.Present {
		t.Fatalf("judgment: failed cleanup must leave device present, got %+v ok=%v", st, ok)
	}
	if err := d.Cleanup("d1"); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
	if st, _ := d.Get("d1"); st.Present {
		t.Fatalf("judgment: successful cleanup must mark Present=false")
	}
}

func TestStores_ConcurrentAccess(t *testing.T) {
	t.Logf("触发条件: 并发 Put（携带单调版本）/Get。")
	t.Logf("判定依据: 不发生 data race（-race）；只接受最高版本，最终版本确定为 50。")
	s := NewDesiredStore()
	d := NewDeviceStore()
	var wg sync.WaitGroup
	for i := 1; i <= 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, _, _ = s.Put("d1", int64(i), map[string]string{"vlan": "10"})
			_ = d.Apply("d1", int64(i), map[string]string{"vlan": "10"})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = s.Get("d1")
			_, _ = d.Get("d1")
		}()
	}
	wg.Wait()

	dc, ok := s.Get("d1")
	if !ok || dc.ResourceVersion != 50 {
		t.Fatalf("judgment: highest revision 50 must win, got ok=%v v=%d", ok, dc.ResourceVersion)
	}
}

func TestDesiredStore_StaleRevisionRejected(t *testing.T) {
	t.Logf("触发条件: 先写 v3，再乱序到达 v2/相等 v3。")
	t.Logf("判定依据: 旧版本与相等版本都被拒绝(accepted=false, ErrStaleRevision)，存储保持 v3 内容不变。")
	s := NewDesiredStore()
	cur, err, ok := s.Put("d1", 3, map[string]string{"vlan": "30"})
	if err != nil || !ok || cur.ResourceVersion != 3 {
		t.Fatalf("precondition: v3 accepted, got %+v", cur)
	}
	for _, rev := range []int64{2, 3} {
		got, errOut, accepted := s.Put("d1", rev, map[string]string{"vlan": "999"})
		if accepted || !errors.Is(errOut, ErrStaleRevision) {
			t.Fatalf("judgment: revision %d must be rejected as stale, accepted=%v err=%v", rev, accepted, errOut)
		}
		if got.Config["vlan"] != "30" {
			t.Fatalf("judgment: stale write must not mutate stored content, got %v", got.Config)
		}
	}
	// 更高版本正常推进。
	v4, _, accepted := s.Put("d1", 4, map[string]string{"vlan": "40"})
	if !accepted || v4.ResourceVersion != 4 || v4.Config["vlan"] != "40" {
		t.Fatalf("judgment: higher revision must be accepted, got %+v accepted=%v", v4, accepted)
	}
}

func TestDesiredStore_UpsertRevivesTombstone(t *testing.T) {
	t.Logf("触发条件: 对 Deleting 墓碑到达更高版本的 Upsert（删除后重建）。")
	t.Logf("判定依据: 以新版本复活为 Active。")
	s := NewDesiredStore()
	_, _, _ = s.Put("d1", 1, map[string]string{"vlan": "10"})
	if _, err, ok := s.MarkDeleting("d1", 2); err != nil || !ok {
		t.Fatalf("mark deleting v2: err=%v ok=%v", err, ok)
	}
	dc, _, accepted := s.Put("d1", 3, map[string]string{"vlan": "30"})
	if !accepted || dc.Phase != domain.PhaseActive || dc.ResourceVersion != 3 {
		t.Fatalf("judgment: higher upsert revives tombstone as Active v3, got %+v accepted=%v", dc, accepted)
	}
}
