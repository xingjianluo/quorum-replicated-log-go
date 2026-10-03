package quorum

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// snapshotData 是一次快照的完整内容：状态机数据、客户端去重表、
// 以及快照边界（最后包含的日志索引与任期）。
// 去重表必须随快照持久化，否则客户端跨快照重试同一请求会被重复应用。
type snapshotData struct {
	LastIndex uint64
	LastTerm  uint64
	Data      map[string]string
	Dedup     map[string]DedupRecord
}

// snapshotStore 负责快照的原子落盘与加载。
// 写入采用 临时文件 + fsync + rename + 目录 fsync：
// 崩溃最多留下一个未被 rename 的临时文件，旧快照始终完整可用，
// 快照动作失败或被打断都不会破坏已提交数据。
type snapshotStore struct {
	mu     sync.Mutex
	path   string
	fsync  bool
	data   snapshotData
	loaded bool
}

// openSnapshot 加载已有快照；文件不存在表示从未做过快照。
// 临时文件（上次快照中途崩溃的残留）直接忽略并清理。
func openSnapshot(path string, fsync bool) (*snapshotStore, error) {
	s := &snapshotStore{path: path, fsync: fsync}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	var d snapshotData
	if err := json.Unmarshal(raw, &d); err != nil {
		// rename 保证原子，理论上不会损坏；防御性按“无快照”处理，
		// 已提交数据仍可从 WAL 完整重放恢复。
		return s, nil
	}
	if d.Data == nil {
		d.Data = map[string]string{}
	}
	if d.Dedup == nil {
		d.Dedup = map[string]DedupRecord{}
	}
	s.data, s.loaded = d, true
	// 清理可能残留的旧临时文件。
	_ = os.Remove(path + ".tmp")
	return s, nil
}

func (s *snapshotStore) load() (snapshotData, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data, s.loaded
}

// save 原子写：临时文件 + fsync + rename + 目录 fsync。
// 失败时旧快照不受影响，调用方不得继续清理日志。
func (s *snapshotStore) save(d snapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if s.fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if s.fsync {
		syncDir(filepath.Dir(s.path))
	}
	s.data, s.loaded = d, true
	return nil
}
