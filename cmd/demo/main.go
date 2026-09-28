// Command demo 在本地端到端演示配置创建、更新、漂移纠正、
// 瞬时失败退避重试、永久失败快速失败以及删除清理的协调语义。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
	"github.com/xingjianluo/device-config-reconciler-go/internal/reconcile"
	"github.com/xingjianluo/device-config-reconciler-go/internal/store"
)

func main() {
	ctx := context.Background()
	desired := store.NewDesiredStore()
	devices := store.NewDeviceStore()
	rec := reconcile.New(desired, devices)

	const dev = "device-01"

	// 1) 创建：期望首次下发。
	upsert1 := domain.Event{EventID: "evt-1", DeviceID: dev, Type: domain.EventUpsert, Revision: 1,
		Config: map[string]string{"hostname": "rtr-1", "vlan": "100"}}
	print(rec.HandleEvent(ctx, upsert1))

	// 2) 重复投递同一事件：结果幂等，设备不重复下发。
	print(rec.HandleEvent(ctx, upsert1))

	// 3) 更新：产生新版本 v2。
	upsert2 := domain.Event{EventID: "evt-2", DeviceID: dev, Type: domain.EventUpsert, Revision: 2,
		Config: map[string]string{"hostname": "rtr-1", "vlan": "200"}}
	print(rec.HandleEvent(ctx, upsert2))

	// 4) 手动篡改设备（漂移），再同步纠正回 v2。
	devices.Mutate(dev, map[string]string{"hostname": "tampered", "vlan": "999"})
	fmt.Println("after manual mutate, device is drifted; running Resync...")
	for _, r := range rec.Resync(ctx) {
		print(r)
	}

	// 5) 删除：先清理设备、再移除墓碑。
	del := domain.Event{EventID: "evt-3", DeviceID: dev, Type: domain.EventDelete, Revision: 3}
	print(rec.HandleEvent(ctx, del))
	if st, ok := devices.Get(dev); !ok || st.Present {
		log.Fatalf("expect device config removed after delete")
	}
	if _, ok := desired.Get(dev); ok {
		log.Fatalf("expect desired tombstone purged after cleanup")
	}

	// 6) 瞬时失败退避重试：设备锁定 2 次后恢复，事件最终收敛。
	const dev2 = "device-02"
	devices.InjectApplyFault(dev2, store.Fault{Remaining: 2, Err: domain.ErrLocked})
	ev := domain.Event{EventID: "evt-4", DeviceID: dev2, Type: domain.EventUpsert, Revision: 1,
		Config: map[string]string{"vlan": "10"}}
	print(rec.HandleEvent(ctx, ev))
	fmt.Printf("device-02 actual apply calls: %d\n", devices.ApplyCalls(dev2))

	// 7) 永久失败：vlan 越界，立即终止且不触设备。
	bad := domain.Event{EventID: "evt-5", DeviceID: "device-03", Type: domain.EventUpsert, Revision: 1,
		Config: map[string]string{"vlan": "9999"}}
	print(rec.HandleEvent(ctx, bad))
	fmt.Printf("device-03 actual apply calls (expect 0): %d\n", devices.ApplyCalls("device-03"))
}

func print(r domain.Result) {
	switch r.Status {
	case domain.StatusTerminal:
		fmt.Printf("[%s] device=%s status=%s reason=%s attempts=%d msg=%q\n",
			r.EventID, r.DeviceID, r.Status, r.TerminalReason, r.Attempts, r.TerminalMessage)
	default:
		fmt.Printf("[%s] device=%s status=%s version=%d attempts=%d\n",
			r.EventID, r.DeviceID, r.Status, r.AppliedVersion, r.Attempts)
	}
}
