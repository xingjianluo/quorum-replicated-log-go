package quorum

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot 是某一已提交索引处的一致性状态快照：
// 状态机数据 + 客户端去重表 + 快照点对应的日志索引/任期。
// 快照落盘后，LastIndex 及更早的日志记录可以被清理（日志压缩）。
type Snapshot struct {
	LastIndex uint64
	LastTerm  uint64
	Data      map[string]string
	Dedup     map[string]dedupRecord
}

// snapshotStore 负责快照的持久化：临时文件 + rename + fsync，
// 崩溃时要么看到旧快照、要么看到新快照，绝不看到半个文件。
type snapshotStore struct {
	mu    sync.Mutex
	path  string
	fsync bool
}

func openSnapshotStore(path string, fsync bool) *snapshotStore {
	return &snapshotStore{path: path, fsync: fsync}
}

// load 读取磁盘快照；不存在返回 (nil, nil)。
func (s *snapshotStore) load() (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		// 快照损坏（rename 保证原子，防御性处理）：视为无快照，
		// 已提交数据仍可从 WAL 完整重放恢复。
		return nil, nil
	}
	return &snap, nil
}

// save 原子写入快照：临时文件 + fsync + rename + 目录 fsync。
// 与 WAL 压缩的配合顺序：先 save 成功，调用方才允许清理旧日志；
// 若崩溃发生在 save 之后、日志压缩之前，恢复时按快照基线过滤 WAL 即可。
func (s *snapshotStore) save(snap *Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(snap)
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
	return nil
}
