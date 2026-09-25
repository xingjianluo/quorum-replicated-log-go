package quorum

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Role 标识副本角色。
type Role int

const (
	Follower Role = iota
	Leader
)

func (r Role) String() string {
	if r == Leader {
		return "leader"
	}
	return "follower"
}

// Replica 是一个副本节点。同一进程内由 Cluster 直连方法调用模拟网络。
//
// 锁约定：r.mu 保护角色/任期/提交索引等内存状态；
// 调用其他副本的方法（HandleAppendEntries/HandleRequestVote）时绝不持有 r.mu，
// 避免选主与复制并发时的锁循环。
type Replica struct {
	mu      sync.Mutex
	id      string
	dir     string
	fsync   bool
	cluster *Cluster

	role    Role
	crashed bool

	term      uint64
	votedFor  string
	commitIdx uint64

	// 领导者状态：是否已在当前任期提交过日志（线性一致读的前提）。
	termCommitted bool
	match         map[string]uint64 // 各跟随者已确认匹配的最大索引

	w    *wal
	meta *metaStore
	kv   *kvStore
}

// openReplica 从磁盘目录打开（或恢复）一个副本。
func openReplica(id, dir string, fsync bool) (*Replica, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	r := &Replica{id: id, dir: dir, fsync: fsync, role: Follower, match: map[string]uint64{}}
	if err := r.recover(); err != nil {
		return nil, err
	}
	return r, nil
}

// recover 加载 WAL 与元数据，截断残缺尾部，并把已提交日志重放到状态机。
func (r *Replica) recover() error {
	w, err := openWAL(filepath.Join(r.dir, "wal.log"), r.fsync)
	if err != nil {
		return err
	}
	m, err := openMeta(filepath.Join(r.dir, "meta.json"), r.fsync)
	if err != nil {
		w.close()
		return err
	}
	r.w, r.meta = w, m
	md := m.load()
	r.term, r.votedFor, r.commitIdx = md.Term, md.VotedFor, md.CommitIndex
	r.kv = newKVStore()
	r.applyCommittedLocked()
	return nil
}

// applyCommittedLocked 把 (applied, commitIdx] 区间的已提交日志应用到状态机。
// 重复回放安全：kvStore 按索引单调推进，已应用的记录自动跳过。
func (r *Replica) applyCommittedLocked() {
	entries := r.w.entries()
	for _, e := range entries {
		if e.Index > r.commitIdx {
			break
		}
		r.kv.apply(e.Index, e)
	}
}

// persistMetaLocked 落盘任期/投票/提交索引。
func (r *Replica) persistMetaLocked() {
	_ = r.meta.save(metaData{Term: r.term, VotedFor: r.votedFor, CommitIndex: r.commitIdx})
}

// ID 返回副本标识。
func (r *Replica) ID() string { return r.id }

// Term 返回当前任期（测试/调试用）。
func (r *Replica) Term() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.term
}

// Role 返回当前角色（测试/调试用）。
func (r *Replica) Role() Role {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.role
}

// CommitIndex 返回已提交索引（测试/调试用）。
func (r *Replica) CommitIndex() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIdx
}

// KVSnapshot 返回状态机数据副本（测试比对收敛用）。
func (r *Replica) KVSnapshot() map[string]string { return r.kv.snapshot() }

// LogEntries 返回 WAL 完整记录（测试比对日志用）。
func (r *Replica) LogEntries() []Entry { return r.w.entries() }

// crashedNow 报告副本当前是否处于崩溃状态。
func (r *Replica) crashedNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.crashed
}

// crash 模拟进程崩溃：内存状态丢失，仅磁盘数据保留。
func (r *Replica) crash() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return
	}
	r.crashed = true
	r.role = Follower
	r.termCommitted = false
	_ = r.w.close() // 模拟进程退出：关闭文件句柄，已 fsync 的数据保留
}

// becomeFollowerLocked 切换到跟随者（发现更高任期或选主失败时）。
func (r *Replica) becomeFollowerLocked(term uint64) {
	r.role = Follower
	r.termCommitted = false
	if term > r.term {
		r.term = term
		r.votedFor = ""
	}
	r.persistMetaLocked()
}

// ---------- 客户端接口 ----------

// Propose 由客户端调用：仅领导者接受。
// 流程：本地持久化 -> 复制到多数副本 -> 推进提交索引 -> 应用状态机。
// 失败原因可区分：ErrNotLeader / ErrNoQuorum / ErrCrashed。
func (r *Replica) Propose(cmd Command) (string, error) {
	r.mu.Lock()
	if r.crashed {
		r.mu.Unlock()
		return "", ErrCrashed
	}
	if r.role != Leader {
		r.mu.Unlock()
		return "", fmt.Errorf("%w (replica %s is %s, term %d)", ErrNotLeader, r.id, r.role, r.term)
	}
	// 1. 本地持久化（提交顺序与持久化顺序一致：先落盘后复制）。
	lastIdx, _ := r.w.lastIndexTerm()
	entry := Entry{Term: r.term, Index: lastIdx + 1, Command: cmd}
	if err := r.w.append(entry); err != nil {
		r.mu.Unlock()
		return "", err
	}
	r.match[r.id] = entry.Index
	r.mu.Unlock()

	// 2. 复制到所有可达副本（不含自身）。
	acks := 1 // 自己已持久化
	for _, p := range r.cluster.reachablePeers(r.id) {
		if r.syncPeer(p) {
			acks++
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return "", ErrCrashed
	}
	if r.role != Leader {
		// 复制期间发现更高任期已下台：旧领导者的写入失效。
		return "", fmt.Errorf("%w: stepped down during replication (term %d)", ErrNotLeader, r.term)
	}
	// 3. 多数派判定。
	if acks < r.cluster.quorum() {
		return "", fmt.Errorf("%w: only %d/%d replicas persisted entry %d",
			ErrNoQuorum, acks, len(r.cluster.cfg.ReplicaIDs), entry.Index)
	}
	// 4. 提交：推进提交索引、落盘、应用。
	r.commitIdx = entry.Index
	if entry.Term == r.term {
		r.termCommitted = true
	}
	r.persistMetaLocked()
	r.applyCommittedLocked()
	r.mu.Unlock()

	// 5. 把新的提交索引通知跟随者（尽力而为，失败不影响已提交结果）。
	for _, p := range r.cluster.reachablePeers(r.id) {
		r.syncPeer(p)
	}
	r.mu.Lock()

	res, _ := r.kv.apply(entry.Index, entry) // 已应用，取缓存结果
	return res, nil
}

// Read 线性一致读：仅领导者可服务，且必须已在当前任期提交过日志，
// 否则无法确认自己仍掌握最新提交状态，返回 ErrUnconfirmedRead。
func (r *Replica) Read(key string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return "", ErrCrashed
	}
	if r.role != Leader {
		return "", fmt.Errorf("%w (replica %s is %s)", ErrNotLeader, r.id, r.role)
	}
	if !r.termCommitted {
		return "", fmt.Errorf("%w: leader has not committed an entry in term %d", ErrUnconfirmedRead, r.term)
	}
	v, ok := r.kv.get(key)
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

// ---------- 复制（领导者侧） ----------

// syncPeer 把本副本日志中 peer 缺失的部分同步给它，并携带最新提交索引。
// 返回 peer 是否确认到当前日志尾部。发现更高任期时会自动下台并返回 false。
func (r *Replica) syncPeer(p *Replica) bool {
	for {
		r.mu.Lock()
		if r.crashed || r.role != Leader {
			r.mu.Unlock()
			return false
		}
		prev := r.match[p.id]
		lastIdx, _ := r.w.lastIndexTerm()
		if prev > lastIdx {
			prev = lastIdx
		}
		var prevTerm uint64
		if prev > 0 {
			if e, ok := r.w.at(prev); ok {
				prevTerm = e.Term
			}
		}
		var batch []Entry
		for _, e := range r.w.entries() {
			if e.Index > prev {
				batch = append(batch, e)
			}
		}
		req := AppendEntriesRequest{
			Term: r.term, LeaderID: r.id,
			PrevIndex: prev, PrevTerm: prevTerm,
			Entries: batch, LeaderCommit: r.commitIdx,
		}
		r.mu.Unlock()

		resp := p.HandleAppendEntries(req)

		r.mu.Lock()
		if resp.Term > r.term {
			r.becomeFollowerLocked(resp.Term)
			r.mu.Unlock()
			return false
		}
		if resp.Success {
			r.match[p.id] = resp.Match
			r.mu.Unlock()
			return resp.Match >= lastIdx
		}
		if resp.Err == ErrLogMismatch {
			// 回退 prev 后重试（跟随者日志短于预期或任期冲突）。
			r.match[p.id] = resp.Match
			r.mu.Unlock()
			continue
		}
		r.mu.Unlock()
		return false // 崩溃/分区等：不可达
	}
}

// ---------- 复制（跟随者侧） ----------

// HandleAppendEntries 处理领导者的复制请求。
func (r *Replica) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return AppendEntriesResponse{Err: ErrCrashed}
	}
	if req.Term < r.term {
		return AppendEntriesResponse{Term: r.term, Err: fmt.Errorf(
			"%w: req term %d < my term %d", ErrStaleTerm, req.Term, r.term)}
	}
	if req.Term > r.term {
		r.becomeFollowerLocked(req.Term)
	}
	r.role = Follower // 收到当前任期领导者的请求，确认跟随者身份

	// 前缀一致性检查。
	lastIdx, _ := r.w.lastIndexTerm()
	if req.PrevIndex > 0 {
		e, ok := r.w.at(req.PrevIndex)
		if !ok || e.Term != req.PrevTerm {
			return AppendEntriesResponse{Term: r.term, Match: lastIdx, Err: fmt.Errorf(
				"%w at prev index %d", ErrLogMismatch, req.PrevIndex)}
		}
	}
	// 追加新日志：冲突处截断后写入（覆盖旧任期未提交的失效记录）。
	for _, e := range req.Entries {
		if existing, ok := r.w.at(e.Index); ok {
			if existing.Term != e.Term {
				if err := r.w.truncateFrom(e.Index); err != nil {
					return AppendEntriesResponse{Term: r.term, Err: err}
				}
				if err := r.w.append(e); err != nil {
					return AppendEntriesResponse{Term: r.term, Err: err}
				}
			}
			continue
		}
		if err := r.w.append(e); err != nil {
			return AppendEntriesResponse{Term: r.term, Err: err}
		}
	}
	// 推进提交索引并应用（不超过本地日志尾部）。
	lastIdx, _ = r.w.lastIndexTerm()
	if req.LeaderCommit > r.commitIdx {
		r.commitIdx = min(req.LeaderCommit, lastIdx)
		r.persistMetaLocked()
		r.applyCommittedLocked()
	}
	lastIdx, _ = r.w.lastIndexTerm()
	return AppendEntriesResponse{Term: r.term, Success: true, Match: lastIdx}
}

// ---------- 选主 ----------

// HandleRequestVote 处理投票请求：任期更高且日志不落后才投票。
func (r *Replica) HandleRequestVote(term uint64, candidateID string, lastIndex, lastTerm uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return r.term, false
	}
	if term < r.term {
		return r.term, false
	}
	myLastIdx, myLastTerm := r.w.lastIndexTerm()
	upToDate := lastTerm > myLastTerm || (lastTerm == myLastTerm && lastIndex >= myLastIdx)
	if !upToDate {
		return r.term, false
	}
	if term == r.term && r.votedFor != "" && r.votedFor != candidateID {
		return r.term, false
	}
	r.becomeFollowerLocked(term)
	r.votedFor = candidateID
	r.persistMetaLocked()
	return r.term, true
}

// campaign 以指定任期发起选主：获得多数票后成为领导者，
// 并立即提交一条当前任期的 noop 日志（确认领导权、开启线性一致读）。
func (r *Replica) campaign(term uint64) bool {
	r.mu.Lock()
	if r.crashed {
		r.mu.Unlock()
		return false
	}
	r.term = term
	r.votedFor = r.id
	r.role = Follower
	r.termCommitted = false
	r.persistMetaLocked()
	lastIdx, lastTerm := r.w.lastIndexTerm()
	r.mu.Unlock()

	votes := 1
	for _, p := range r.cluster.reachablePeers(r.id) {
		respTerm, granted := p.HandleRequestVote(term, r.id, lastIdx, lastTerm)
		if granted {
			votes++
			continue
		}
		r.mu.Lock()
		if respTerm > r.term {
			r.becomeFollowerLocked(respTerm)
			r.mu.Unlock()
			return false
		}
		r.mu.Unlock()
	}
	if votes < r.cluster.quorum() {
		return false
	}
	r.mu.Lock()
	if r.crashed {
		r.mu.Unlock()
		return false
	}
	r.role = Leader
	r.termCommitted = false
	r.match = map[string]uint64{r.id: 0}
	for _, p := range r.cluster.reachablePeers(r.id) {
		r.match[p.id] = 0
	}
	r.mu.Unlock()
	// 提交当前任期的 noop，确认领导权（同时把历史未提交日志一并提交）。
	_, _ = r.Propose(Command{Op: OpNoop, ClientID: "", Seq: 0})
	r.mu.Lock()
	ok := r.role == Leader
	r.mu.Unlock()
	return ok
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
