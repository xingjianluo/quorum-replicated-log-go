package quorum

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Config 是集群配置。
type Config struct {
	ReplicaIDs []string
	Dir        string // 数据根目录，每个副本一个子目录
	Fsync      bool   // 是否每次写盘都 fsync（测试可置 true 验证崩溃恢复语义）
}

// Cluster 管理同一进程内的多个副本，并提供故障注入能力
// （崩溃、重启、网络分区），用于在单测中模拟真实集群故障。
type Cluster struct {
	cfg      Config
	mu       sync.Mutex
	replicas map[string]*Replica
	isolated map[string]bool
}

// NewCluster 创建（或从磁盘恢复）一个集群。
func NewCluster(cfg Config) (*Cluster, error) {
	if len(cfg.ReplicaIDs) == 0 {
		return nil, fmt.Errorf("quorum: empty replica list")
	}
	c := &Cluster{
		cfg:      cfg,
		replicas: map[string]*Replica{},
		isolated: map[string]bool{},
	}
	for _, id := range cfg.ReplicaIDs {
		r, err := openReplica(id, filepath.Join(cfg.Dir, id), cfg.Fsync)
		if err != nil {
			return nil, err
		}
		r.cluster = c
		c.replicas[id] = r
	}
	return c, nil
}

// quorum 返回多数派票数。
func (c *Cluster) quorum() int { return len(c.cfg.ReplicaIDs)/2 + 1 }

// Replica 返回指定副本。
func (c *Cluster) Replica(id string) *Replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replicas[id]
}

// reachablePeers 返回对 id 而言可达的其他副本
// （未崩溃、未分区；若 id 自身被分区则无人可达）。
func (c *Cluster) reachablePeers(id string) []*Replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isolated[id] {
		return nil
	}
	var out []*Replica
	for pid, r := range c.replicas {
		if pid == id || c.isolated[pid] || r.crashedNow() {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Elect 让指定副本以更高任期发起选主（多数派存活时成功）。
func (c *Cluster) Elect(id string) (*Replica, error) {
	c.mu.Lock()
	r := c.replicas[id]
	if r == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("quorum: unknown replica %q", id)
	}
	if c.isolated[id] || r.crashedNow() {
		c.mu.Unlock()
		return nil, fmt.Errorf("quorum: replica %q unreachable", id)
	}
	// 新任期高于所有现存任期，保证旧领导者的写入失效。
	var maxTerm uint64
	for _, other := range c.replicas {
		if t := other.Term(); t > maxTerm {
			maxTerm = t
		}
	}
	c.mu.Unlock()
	if !r.campaign(maxTerm + 1) {
		return nil, fmt.Errorf("quorum: replica %q failed to win election", id)
	}
	return r, nil
}

// Crash 模拟副本进程崩溃：内存状态丢失，已持久化数据保留在磁盘。
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	r := c.replicas[id]
	c.mu.Unlock()
	if r != nil {
		r.crash()
	}
}

// Restart 用磁盘上的持久化状态重启副本（模拟进程重启）：
// 截断 WAL 残缺尾部、重放已提交日志，并恢复通信。
func (c *Cluster) Restart(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, err := openReplica(id, filepath.Join(c.cfg.Dir, id), c.cfg.Fsync)
	if err != nil {
		return err
	}
	r.cluster = c
	c.replicas[id] = r
	delete(c.isolated, id)
	return nil
}

// Isolate 模拟网络分区：副本无法与其他副本通信（但不崩溃）。
func (c *Cluster) Isolate(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isolated[id] = true
}

// Heal 恢复副本的通信。
func (c *Cluster) Heal(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.isolated, id)
}

// DataDir 返回副本的数据目录（测试用于构造残缺日志等场景）。
func (c *Cluster) DataDir(id string) string {
	return filepath.Join(c.cfg.Dir, id)
}

// MkdirAll 确保数据根目录存在（NewCluster 前置步骤，便于测试显式调用）。
func (c *Cluster) MkdirAll() error { return os.MkdirAll(c.cfg.Dir, 0o755) }
