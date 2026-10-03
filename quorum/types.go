// Package quorum 实现一个本地可运行的多数派复制式键值存储。
// 多个副本运行在同一进程内，通过内存消息模拟网络，
// 通过文件 WAL 模拟持久化，通过“崩溃/重启”接口模拟故障。
package quorum

import "errors"

// Op 是状态机操作类型。
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "del"
	OpNoop   Op = "noop"
)

// Command 是一条客户端命令。ClientID+Seq 构成幂等键：
// 客户端超时重试同一请求时携带相同的 (ClientID, Seq)，
// 副本据此去重，保证同一请求不会被重复应用。
type Command struct {
	Op       Op
	Key      string
	Value    string
	ClientID string
	Seq      uint64
}

// Entry 是复制日志中的一条记录。
type Entry struct {
	Term    uint64
	Index   uint64
	Command Command
}

// 可区分的拒绝/失败原因。调用方可用 errors.Is 判定。
var (
	// ErrNotLeader：该副本不是当前领导者（或已下台），无法接受写入/线性一致读。
	ErrNotLeader = errors.New("quorum: not leader")
	// ErrNoQuorum：领导者未能在多数副本上持久化该写入，写入被拒绝。
	ErrNoQuorum = errors.New("quorum: no quorum")
	// ErrStaleTerm：请求携带的任期过期（旧领导者的失效写入）。
	ErrStaleTerm = errors.New("quorum: stale term")
	// ErrLogMismatch：日志前缀不一致，跟随者拒绝追加。
	ErrLogMismatch = errors.New("quorum: log mismatch")
	// ErrCrashed：副本处于崩溃状态，不可访问。
	ErrCrashed = errors.New("quorum: replica crashed")
	// ErrKeyNotFound：读取的键不存在。
	ErrKeyNotFound = errors.New("quorum: key not found")
	// ErrUnconfirmedRead：领导者无法确认自己仍掌握最新已提交状态，拒绝读取。
	ErrUnconfirmedRead = errors.New("quorum: unconfirmed read")
)

// AppendEntriesRequest 是领导者发给跟随者的复制请求（简化版 Raft）。
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevIndex    uint64
	PrevTerm     uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesResponse 是跟随者对复制请求的响应。
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	Match   uint64 // 已确认匹配的最大日志索引
	Err     error  // 可区分的拒绝原因
}

// InstallSnapshotRequest 是领导者发给严重落后跟随者的快照传输请求：
// 跟随者落后的起点早于领导者已压缩清理的日志时，用快照直接对齐状态。
type InstallSnapshotRequest struct {
	Term         uint64
	LeaderID     string
	Snapshot     Snapshot
	LeaderCommit uint64
}

// InstallSnapshotResponse 是跟随者对快照传输的响应。
type InstallSnapshotResponse struct {
	Term    uint64
	Success bool
	Match   uint64 // 快照覆盖的最大日志索引
	Err     error
}
