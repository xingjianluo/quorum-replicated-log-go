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

// capture 导出当前已提交状态的完整快照（数据 + 去重表 + 已应用索引）。
// 去重表必须随快照一起保存：否则跨快照的客户端重试会被重复应用。
func (k *kvStore) capture(lastIndex, lastTerm uint64) *Snapshot {
	k.mu.Lock()
	defer k.mu.Unlock()
	data := make(map[string]string, len(k.data))
	for key, v := range k.data {
		data[key] = v
	}
	dedup := make(map[string]dedupRecord, len(k.dedup))
	for id, rec := range k.dedup {
		dedup[id] = rec
	}
	return &Snapshot{LastIndex: lastIndex, LastTerm: lastTerm, Data: data, Dedup: dedup}
}

// restore 用快照整体替换状态机（数据、去重表、已应用索引）。
// 重复恢复同一快照是幂等的：状态被替换为相同内容，applied 不回退也不膨胀。
func (k *kvStore) restore(snap *Snapshot) {
	k.mu.Lock()
	defer k.mu.Unlock()
	data := make(map[string]string, len(snap.Data))
	for key, v := range snap.Data {
		data[key] = v
	}
	dedup := make(map[string]dedupRecord, len(snap.Dedup))
	for id, rec := range snap.Dedup {
		dedup[id] = rec
	}
	k.data = data
	k.dedup = dedup
	k.applied = snap.LastIndex
}
