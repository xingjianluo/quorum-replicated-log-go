package quorum

import "sync"

// kvStore 是应用层状态机：按日志索引顺序应用已提交记录。
// 幂等保证：
//  1. 索引必须严格单调推进（index == applied+1），重复回放同一条
//     已提交记录会被跳过，状态不变、不产生计数膨胀；
//  2. 同一 (ClientID, Seq) 的重复请求直接返回缓存结果，不重复应用，
//     客户端超时重试安全。
type kvStore struct {
	mu      sync.Mutex
	data    map[string]string
	applied uint64
	// 每个客户端已处理的最大 Seq 及其结果，用于重试去重。
	dedup map[string]dedupRecord
}

type dedupRecord struct {
	Seq    uint64
	Result string
}

func newKVStore() *kvStore {
	return &kvStore{data: map[string]string{}, dedup: map[string]dedupRecord{}}
}

// apply 应用一条已提交日志，返回 (结果值, 是否实际应用)。
// 重复回放（index <= applied）或重复请求（ClientID+Seq 已见）返回 false。
func (k *kvStore) apply(index uint64, e Entry) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if index != k.applied+1 {
		// 重复回放或乱序：跳过，状态不变；
		// 若该请求此前已应用，返回缓存结果（客户端重试可见同一结果）。
		if index <= k.applied && e.Command.ClientID != "" {
			if rec, ok := k.dedup[e.Command.ClientID]; ok && e.Command.Seq <= rec.Seq {
				return rec.Result, false
			}
		}
		return "", false
	}
	k.applied = index
	cmd := e.Command
	if cmd.Op == OpNoop {
		return "", true
	}
	// 客户端请求去重：同一 (ClientID, Seq) 只应用一次。
	if cmd.ClientID != "" {
		if rec, ok := k.dedup[cmd.ClientID]; ok && cmd.Seq <= rec.Seq {
			return rec.Result, false
		}
	}
	old := k.data[cmd.Key]
	switch cmd.Op {
	case OpSet:
		k.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(k.data, cmd.Key)
	}
	if cmd.ClientID != "" {
		k.dedup[cmd.ClientID] = dedupRecord{Seq: cmd.Seq, Result: old}
	}
	return old, true
}

func (k *kvStore) get(key string) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.data[key]
	return v, ok
}

// appliedIndex 返回已应用的最大日志索引。
func (k *kvStore) appliedIndex() uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.applied
}

// snapshot 返回数据副本（用于测试比对各副本收敛）。
func (k *kvStore) snapshot() map[string]string {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make(map[string]string, len(k.data))
	for key, v := range k.data {
		out[key] = v
	}
	return out
}
