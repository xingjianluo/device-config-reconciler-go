// Package store 提供协调组件使用的两类本地存储：
// DeviceStore 模拟设备端（实际状态 + 设备操作，支持故障注入与手动篡改钩子），
// DesiredStore 保存期望配置（含 Deleting 墓碑）。
package store

import (
	"sync"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
)

// Fault 表示对设备端注入的一次故障行为。
type Fault struct {
	// Remaining 剩余生效次数；<0 表示永久生效（直到重新注入），0 表示不注入。
	Remaining int
	Err       error
}

// consume 返回本次是否应返回故障：永久(Remaining<0)恒命中；否则命中一次减一。
func (f *Fault) consume() (error, bool) {
	if f == nil {
		return nil, false
	}
	if f.Remaining < 0 {
		return f.Err, true
	}
	if f.Remaining == 0 {
		return nil, false
	}
	f.Remaining--
	return f.Err, true
}

func copyConfig(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// DeviceStore 用本地 map 模拟设备端。所有方法均并发安全。
type DeviceStore struct {
	mu sync.Mutex

	states       map[string]domain.DeviceState
	applyFaults  map[string]*Fault
	cleanFaults map[string]*Fault
	applyCalls  map[string]int
	cleanCalls  map[string]int
}

// NewDeviceStore 创建模拟设备端存储。
func NewDeviceStore() *DeviceStore {
	return &DeviceStore{
		states:       make(map[string]domain.DeviceState),
		applyFaults:  make(map[string]*Fault),
		cleanFaults: make(map[string]*Fault),
		applyCalls:   make(map[string]int),
		cleanCalls:   make(map[string]int),
	}
}

// Get 读取设备实际状态（返回副本）；从未存在时 ok=false。
func (s *DeviceStore) Get(deviceID string) (domain.DeviceState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[deviceID]
	if !ok {
		return domain.DeviceState{}, false
	}
	st.Config = copyConfig(st.Config)
	return st, true
}

// Apply 下发/更新配置到设备。成功后 AppliedVersion 与 Config 更新为期望值，Present=true。
// 触发注入故障时返回该故障错误，且不改变状态。
func (s *DeviceStore) Apply(deviceID string, version int64, cfg map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyCalls[deviceID]++
	if err, hit := s.applyFaults[deviceID].consume(); hit {
		return err
	}
	s.states[deviceID] = domain.DeviceState{
		DeviceID:       deviceID,
		AppliedVersion: version,
		Config:         copyConfig(cfg),
		Present:        true,
	}
	return nil
}

// Cleanup 执行删除清理动作。成功后设备配置被移除(Present=false)。
func (s *DeviceStore) Cleanup(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanCalls[deviceID]++
	if err, hit := s.cleanFaults[deviceID].consume(); hit {
		return err
	}
	st := s.states[deviceID]
	st.Present = false
	st.Config = nil
	st.AppliedVersion = 0
	s.states[deviceID] = st
	return nil
}

// Mutate 模拟设备被手动改动（漂移），不影响 Present。
func (s *DeviceStore) Mutate(deviceID string, config map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.states[deviceID]
	st.Config = copyConfig(config)
	st.Present = true
	s.states[deviceID] = st
}

// InjectApplyFault / InjectCleanupFault 注入瞬时/永久故障（按次数生效）。
func (s *DeviceStore) InjectApplyFault(deviceID string, f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fc := f
	s.applyFaults[deviceID] = &fc
}

func (s *DeviceStore) InjectCleanupFault(deviceID string, f Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fc := f
	s.cleanFaults[deviceID] = &fc
}

// ApplyCalls / CleanupCalls 返回实际下发/清理的调用次数（并发/幂等断言用）。
func (s *DeviceStore) ApplyCalls(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyCalls[deviceID]
}

func (s *DeviceStore) CleanupCalls(deviceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleanCalls[deviceID]
}
