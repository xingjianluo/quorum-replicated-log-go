// Package kvserver wires the raft replicated log to per-replica key/value
// state machines and provides a small client-style KV API with automatic
// leader redirection, idempotent retries and linearizable reads.
package kvserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/quorum-replicated-log-go/cluster"
	"github.com/xingjianluo/quorum-replicated-log-go/raft"
	"github.com/xingjianluo/quorum-replicated-log-go/store"
)

// Req is a client key/value request.
type Req struct {
	Op        store.Op
	Key       string
	Value     string
	Delta     int64
	ClientID  string
	RequestID string
}

// Resp is the outcome of a completed write.
type Resp struct {
	Outcome raft.ProposeOutcome
	Apply   store.ApplyResult
}

// KVCluster is a simulated replicated KV store: one KVStore per replica.
type KVCluster struct {
	cl     *cluster.Cluster
	mu     sync.Mutex
	stores map[uint64]*store.KVStore
}

// New creates a KVCluster of n replicas rooted at baseDir.
func New(baseDir string, n int) (*KVCluster, error) {
	kv := &KVCluster{stores: make(map[uint64]*store.KVStore)}
	cl, err := cluster.NewCluster(baseDir, n, kv.applyCommitted)
	if err != nil {
		return nil, err
	}
	kv.cl = cl
	for _, id := range cl.Peers() {
		kv.stores[id] = store.New()
	}
	return kv, nil
}

// Start launches the cluster tick driver.
func (k *KVCluster) Start() { k.cl.Start() }

// Stop shuts the cluster down.
func (k *KVCluster) Stop() error { return k.cl.Stop() }

// Cluster exposes the underlying simulation cluster.
func (k *KVCluster) Cluster() *cluster.Cluster { return k.cl }

func (k *KVCluster) applyCommitted(id, index, term uint64, data []byte) {
	var cmd store.Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		panic(fmt.Sprintf("kvserver: bad committed command on replica %d: %v", id, err))
	}
	k.mu.Lock()
	s := k.stores[id]
	k.mu.Unlock()
	if _, err := s.Apply(index, cmd); err != nil {
		panic(fmt.Sprintf("kvserver: apply on replica %d: %v", id, err))
	}
}

// storeFor returns a live replica's state machine.
func (k *KVCluster) storeFor(id uint64) *store.KVStore {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.stores[id]
}

func encode(req Req) ([]byte, error) {
	cmd := store.Command{
		Op:        req.Op,
		Key:       req.Key,
		Value:     req.Value,
		Delta:     req.Delta,
		ClientID:  req.ClientID,
		RequestID: req.RequestID,
	}
	return json.Marshal(cmd)
}

// Write submits a write, transparently redirecting from a non-leader to
// the hinted leader. A timeout-driven retry with the same
// ClientID/RequestID is never applied twice (deduplicated by the FSM).
func (k *KVCluster) Write(ctx context.Context, req Req) (Resp, error) {
	data, err := encode(req)
	if err != nil {
		return Resp{}, err
	}
	// Start at the current leader if known, else at replica 1.
	target, err := k.cl.WaitLeader(2 * time.Second)
	if err != nil {
		target = k.cl.Peers()[0]
	}
	var lastErr error
	for {
		// Re-discover the leader on every iteration: after an election a
		// previously hinted node may already have stepped down.
		lid, lerr := k.cl.WaitLeader(300 * time.Millisecond)
		if lerr == nil {
			target = lid
		}
		out, perr := k.cl.Propose(target, ctx, data)
		if perr == nil {
			ar, _ := k.storeFor(target).ResultFor(req.RequestID)
			ar.Index = out.Index
			return Resp{Outcome: out, Apply: ar}, nil
		}
		var re *raft.Error
		if errors.As(perr, &re) {
			switch re.Code {
			case raft.RejectNotLeader:
				if re.LeaderHint != 0 && re.LeaderHint != target {
					target = re.LeaderHint
					lastErr = perr
					continue
				}
			case raft.RejectStaleTerm, raft.RejectNoQuorum:
				lastErr = perr
			default:
				return Resp{}, perr
			}
		} else {
			return Resp{}, perr
		}
		// Election churn / quorum temporarily unavailable: back off and
		// retry the SAME idempotent request (FSM deduplicates it).
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return Resp{}, lastErr
			}
			return Resp{}, perr
		case <-time.After(40 * time.Millisecond):
		}
	}
}

// LinearizableRead returns a committed value. It first runs a read
// barrier (fresh quorum confirmation of leadership) so no old value can
// be returned; if leadership cannot be confirmed, the read is refused
// with raft.RejectStaleRead rather than serving possibly stale data.
func (k *KVCluster) LinearizableRead(ctx context.Context, key string) (string, bool, error) {
	leader, err := k.cl.WaitLeader(2 * time.Second)
	if err != nil {
		return "", false, err
	}
	if err := k.cl.ReadBarrier(leader, ctx); err != nil {
		return "", false, err
	}
	v, ok := k.storeFor(leader).Get(key)
	return v, ok, nil
}

// StaleRead deliberately reads a specific replica's LOCAL state without
// any quorum confirmation. The boolean stale is always true and every
// returned value is explicitly marked as potentially stale.
func (k *KVCluster) StaleRead(id uint64, key string) (value string, ok, stale bool) {
	s := k.storeFor(id)
	if s == nil {
		return "", false, true
	}
	v, exists := s.Get(key)
	return v, exists, true
}

// State returns a deep copy of one replica's committed key/value state.
func (k *KVCluster) State(id uint64) map[string]string {
	s := k.storeFor(id)
	if s == nil {
		return nil
	}
	return s.Clone()
}

// Crash hard-stops a replica (no graceful shutdown).
func (k *KVCluster) Crash(id uint64) { k.cl.Crash(id) }

// Restart rebuilds a crashed replica; its FSM starts empty and is
// rebuilt by committed-entry catch-up/replay.
func (k *KVCluster) Restart(id uint64) error {
	k.mu.Lock()
	k.stores[id] = store.New()
	k.mu.Unlock()
	return k.cl.Restart(id)
}

// IsDown reports crash state.
func (k *KVCluster) IsDown(id uint64) bool { return k.cl.IsDown(id) }

// Partition isolates the listed replicas from the others.
func (k *KVCluster) Partition(ids ...uint64) { k.cl.Partition(ids...) }

// Heal removes partitions.
func (k *KVCluster) Heal() { k.cl.Heal() }

// Majority returns the quorum size.
func (k *KVCluster) Majority() int { return k.cl.Majority() }

// Peers returns replica IDs.
func (k *KVCluster) Peers() []uint64 { return k.cl.Peers() }
