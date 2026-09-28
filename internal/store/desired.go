package store

import (
	"errors"
	"sort"
	"sync"

	"github.com/xingjianluo/device-config-reconciler-go/internal/domain"
)

// ErrNotFound 期望配置不存在。
var ErrNotFound = errors.New("desired config not found")

// DesiredStore 保存期望配置。版本号按设备单调递增；
// Delete 不立即移除，而是转为 Deleting 墓碑；Purge 在设备清理完成后移除墓碑。
type DesiredStore struct {
	mu     sync.Mutex
	items  map[string]domain.DesiredConfig
}

func NewDesiredStore() *DesiredStore {
	return &DesiredStore{items: make(map[string]domain.DesiredConfig)}
}

// ErrStaleRevision 事件携带的版本不高于期望存储中已有的最新版本（乱序/迟到事件）。
var ErrStaleRevision = errors.New("stale revision: older or equal revision cannot overwrite newer desired state")

// Put 按事件携带的 revision 写入期望配置。
// 只有 revision 高于当前最新版本时才接受（复活 Deleting 墓碑同样要求版本更高）；
// 旧/等版本（乱序、迟到、重复触发）返回 ErrStaleRevision，调用方应改为向当前最新期望收敛。
func (s *DesiredStore) Put(deviceID string, revision int64, config map[string]string) (domain.DesiredConfig, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.items[deviceID]; ok && revision <= cur.ResourceVersion {
		cur.Config = copyConfig(cur.Config)
		return cur, ErrStaleRevision, false
	}
	dc := domain.DesiredConfig{
		DeviceID:        deviceID,
		ResourceVersion: revision,
		Phase:           domain.PhaseActive,
		Config:          copyConfig(config),
	}
	s.items[deviceID] = dc
	// 返回独立副本，调用方对返回值的改写不能污染存储。
	dc.Config = copyConfig(dc.Config)
	return dc, nil, true
}

// Get 读取期望配置（返回副本）；墓碑也会返回（通过 Phase 区分）；不存在时 ok=false。
func (s *DesiredStore) Get(deviceID string) (domain.DesiredConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dc, ok := s.items[deviceID]
	if !ok {
		return domain.DesiredConfig{}, false
	}
	dc.Config = copyConfig(dc.Config)
	return dc, true
}

// MarkDeleting 按删除事件携带的 revision 把配置标记为 Deleting（保留墓碑）。
// revision 必须高于当前版本；对同一个删除版本的重复调用幂等返回当前墓碑；
// 低于当前版本的迟到删除返回 ErrStaleRevision（不能删除更新的配置）。
// 对象从不存在时报 ErrNotFound。
func (s *DesiredStore) MarkDeleting(deviceID string, revision int64) (domain.DesiredConfig, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dc, ok := s.items[deviceID]
	if !ok {
		return domain.DesiredConfig{}, ErrNotFound, false
	}
	if revision < dc.ResourceVersion {
		dc.Config = copyConfig(dc.Config)
		return dc, ErrStaleRevision, false
	}
	if dc.Phase == domain.PhaseDeleting {
		dc.Config = copyConfig(dc.Config)
		return dc, nil, false // 同一删除的重复触发：幂等
	}
	dc.Phase = domain.PhaseDeleting
	dc.ResourceVersion = revision
	dc.Config = nil
	s.items[deviceID] = dc
	return dc, nil, true
}

// Purge 彻底移除墓碑。仅允许在 PhaseDeleting 时调用。
func (s *DesiredStore) Purge(deviceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dc, ok := s.items[deviceID]
	if !ok {
		return ErrNotFound
	}
	if dc.Phase != domain.PhaseDeleting {
		return errors.New("Purge only allowed for Deleting phase")
	}
	delete(s.items, deviceID)
	return nil
}

// List 列出全部设备ID（含墓碑，按字典序，保证再同步顺序稳定）。
func (s *DesiredStore) List() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.items))
	for id := range s.items {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
