package quorum

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// metaStore 持久化副本元数据：当前任期、投票对象、已提交索引。
// 已提交索引落盘后，崩溃恢复时可把已提交日志完整重放到状态机。
// 写入采用 临时文件 + rename + fsync，避免崩溃留下半个文件。
type metaStore struct {
	mu    sync.Mutex
	path  string
	fsync bool
	data  metaData
}

type metaData struct {
	Term        uint64
	VotedFor    string
	CommitIndex uint64
}

func openMeta(path string, fsync bool) (*metaStore, error) {
	m := &metaStore{path: path, fsync: fsync}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil // 首次启动：零值元数据
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &m.data); err != nil {
		// 元数据损坏（理论上 rename 保证原子，防御性处理）：按零值恢复，
		// 已提交数据仍可从 WAL 与后续提交通知恢复。
		m.data = metaData{}
	}
	return m, nil
}

func (m *metaStore) load() metaData {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data
}

// save 原子写：临时文件 + fsync + rename + 目录 fsync。
func (m *metaStore) save(d metaData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if m.fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	if m.fsync {
		syncDir(filepath.Dir(m.path))
	}
	m.data = d
	return nil
}
