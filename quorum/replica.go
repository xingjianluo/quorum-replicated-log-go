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
	snap *snapshotStore
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

// recover 加载 WAL、元数据与快照，截断残缺尾部，并把已提交日志重放到状态机。
// 恢复顺序：先恢复快照（状态机直接对齐到快照点），再重放快照点之后的已提交日志。
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
	ss := openSnapshotStore(filepath.Join(r.dir, "snapshot.json"), r.fsync)
	snap, err := ss.load()
	if err != nil {
		w.close()
		return err
	}
	r.w, r.meta, r.snap = w, m, ss
	md := m.load()
	r.term, r.votedFor, r.commitIdx = md.Term, md.VotedFor, md.CommitIndex
	r.kv = newKVStore()
	if snap != nil {
		// 快照已落盘：WAL 中被覆盖的旧记录（可能因崩溃来不及压缩）按基线清理。
		if err := w.setBase(snap.LastIndex, snap.LastTerm); err != nil {
			return err
		}
		r.kv.restore(snap)
		if r.commitIdx < snap.LastIndex {
			// 防御：提交索引不应低于快照点（快照只含已提交状态）。
			r.commitIdx = snap.LastIndex
			r.persistMetaLocked()
		}
	}
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
	// 并发 Propose 下，索引更大的写入可能先完成提交；
	// 提交索引必须单调推进，否则后完成的较小索引会把 commitIdx 回退，
	// 导致尾部日志永远不在跟随者提交（跟随者状态机丢写）。
	if entry.Index > r.commitIdx {
		r.commitIdx = entry.Index
	}
	if entry.Term == r.term {
		r.termCommitted = true
	}
	r.persistMetaLocked()
	r.applyCommittedLocked()
	r.maybeAutoSnapshotLocked()
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
// 若 peer 落后的起点早于已压缩清理的日志，则改为发送快照对齐状态。
func (r *Replica) syncPeer(p *Replica) bool {
	for {
		r.mu.Lock()
		if r.crashed || r.role != Leader {
			r.mu.Unlock()
			return false
		}
		prev := r.match[p.id]
		base := r.w.baseIndex()
		if prev < base {
			// 跟随者落后的起点已被快照清理：发送快照让其直接对齐到快照点。
			snap, err := r.snap.load()
			if err != nil || snap == nil {
				r.mu.Unlock()
				return false
			}
			req := InstallSnapshotRequest{
				Term: r.term, LeaderID: r.id,
				Snapshot: *snap, LeaderCommit: r.commitIdx,
			}
			r.mu.Unlock()

			resp := p.HandleInstallSnapshot(req)

			r.mu.Lock()
			if resp.Term > r.term {
				r.becomeFollowerLocked(resp.Term)
				r.mu.Unlock()
				return false
			}
			if resp.Success {
				r.match[p.id] = resp.Match
				r.mu.Unlock()
				continue // 快照对齐后继续同步快照点之后的日志
			}
			r.mu.Unlock()
			return false // 崩溃/分区等：不可达
		}
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
	base := r.w.baseIndex()
	for _, e := range req.Entries {
		if e.Index <= base {
			continue // 已被本地快照覆盖的记录：跳过，不重复追加
		}
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
		r.maybeAutoSnapshotLocked()
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

// ---------- 快照 ----------

// Snapshot 对已提交状态做快照，并清理快照点及更早的日志记录。
// 顺序保证：快照先原子落盘，成功后才压缩 WAL；
// 快照失败或中途崩溃都不会影响已提交数据（旧日志原样保留，
// 恢复时按快照基线过滤 WAL，重复恢复同一快照是幂等的）。
func (r *Replica) Snapshot() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return ErrCrashed
	}
	return r.snapshotLocked()
}

// snapshotLocked 在已持有 r.mu 时执行快照（调用方需保证未崩溃）。
func (r *Replica) snapshotLocked() error {
	applied := r.kv.appliedIndex()
	base := r.w.baseIndex()
	if applied == 0 || applied <= base {
		return nil // 没有新的已提交状态可快照
	}
	e, ok := r.w.at(applied)
	if !ok {
		return fmt.Errorf("quorum: snapshot point %d not in log (base %d)", applied, base)
	}
	snap := r.kv.capture(applied, e.Term)
	if err := r.snap.save(snap); err != nil {
		return err // 快照落盘失败：不触碰 WAL，已提交数据不受影响
	}
	// 快照已持久化，清理被覆盖的日志；此步失败同样安全（恢复时按基线过滤）。
	return r.w.compactThrough(snap.LastIndex, snap.LastTerm)
}

// maybeAutoSnapshotLocked 在 WAL 长度超过配置的阈值时自动快照（0 表示关闭）。
func (r *Replica) maybeAutoSnapshotLocked() {
	th := r.cluster.cfg.SnapshotThreshold
	if th <= 0 {
		return
	}
	lastIdx, _ := r.w.lastIndexTerm()
	if lastIdx-r.w.baseIndex() >= uint64(th) {
		_ = r.snapshotLocked()
	}
}

// HandleInstallSnapshot 处理领导者的快照传输：用于跟随者落后的起点
// 早于领导者已清理日志的场景。整体流程幂等：同一快照重复发送、
// 或恢复过程中崩溃重发，状态都保持一致，不会重复应用。
func (r *Replica) HandleInstallSnapshot(req InstallSnapshotRequest) InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.crashed {
		return InstallSnapshotResponse{Err: ErrCrashed}
	}
	if req.Term < r.term {
		return InstallSnapshotResponse{Term: r.term, Err: fmt.Errorf(
			"%w: req term %d < my term %d", ErrStaleTerm, req.Term, r.term)}
	}
	if req.Term > r.term {
		r.becomeFollowerLocked(req.Term)
	}
	r.role = Follower

	snap := req.Snapshot
	if snap.LastIndex <= r.w.baseIndex() {
		// 本地快照已覆盖该点：重复发送，直接确认（幂等）。
		return InstallSnapshotResponse{Term: r.term, Success: true, Match: r.w.baseIndex()}
	}
	// 快照点与本地日志冲突（同索引不同任期）：快照点之后的本地后缀全部失效。
	if existing, ok := r.w.at(snap.LastIndex); ok && existing.Term != snap.LastTerm {
		if err := r.w.truncateFrom(snap.LastIndex); err != nil {
			return InstallSnapshotResponse{Term: r.term, Err: err}
		}
	}
	// 1. 快照原子落盘：先持久化，再改内存、再清理日志。
	if err := r.snap.save(&snap); err != nil {
		return InstallSnapshotResponse{Term: r.term, Err: err}
	}
	// 2. 清理被快照覆盖的日志，保留快照点之后仍一致的后缀。
	if err := r.w.compactThrough(snap.LastIndex, snap.LastTerm); err != nil {
		return InstallSnapshotResponse{Term: r.term, Err: err}
	}
	// 3. 状态机整体替换为快照内容（含客户端去重表，重试不会重复应用）。
	r.kv.restore(&snap)
	// 4. 提交索引至少推进到快照点，并按领导者提交位继续推进、应用后缀。
	if r.commitIdx < snap.LastIndex {
		r.commitIdx = snap.LastIndex
	}
	lastIdx, _ := r.w.lastIndexTerm()
	if req.LeaderCommit > r.commitIdx {
		r.commitIdx = min(req.LeaderCommit, lastIdx)
	}
	r.persistMetaLocked()
	r.applyCommittedLocked()
	return InstallSnapshotResponse{Term: r.term, Success: true, Match: snap.LastIndex}
}
