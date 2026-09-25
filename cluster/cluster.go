// Package cluster runs several raft.Replica instances in one process to
// simulate a local cluster: an in-process transport, hard crashes/restarts
// and network partitions, all without real networks or external services.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/xingjianluo/quorum-replicated-log-go/raft"
)

// ApplyFunc applies one committed log entry to a replica's state machine.
type ApplyFunc func(id uint64, index, term uint64, data []byte)

// Cluster is a simulated in-process set of replicas.
type Cluster struct {
	mu sync.Mutex

	n        int
	baseDir  string
	peers    []uint64
	apply    ApplyFunc
	tickStep time.Duration

	replicas map[uint64]*raft.Replica
	down     map[uint64]bool
	partOf   map[uint64]int // partition index; -1 = fully isolated
	paused   map[uint64]bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Option customises a Cluster.
type Option func(*Cluster)

// WithTickStep sets the driver tick interval.
func WithTickStep(d time.Duration) Option {
	return func(c *Cluster) { c.tickStep = d }
}

// inprocTransport routes RPCs to live replicas, dropping cross-partition
// and crashed-replica traffic.
type inprocTransport struct {
	c *Cluster
}

func (t *inprocTransport) RequestVote(from, to uint64, req raft.VoteRequest) (raft.VoteResponse, error) {
	if !t.c.canTalk(from, to) {
		return raft.VoteResponse{}, fmt.Errorf("partition: %d -/-> %d", from, to)
	}
	r := t.c.Replica(to)
	if r == nil {
		return raft.VoteResponse{}, fmt.Errorf("replica %d is down", to)
	}
	return r.RequestVote(req), nil
}

func (t *inprocTransport) AppendEntries(from, to uint64, req raft.AppendRequest) (raft.AppendResponse, error) {
	if !t.c.canTalk(from, to) {
		return raft.AppendResponse{}, fmt.Errorf("partition: %d -/-> %d", from, to)
	}
	r := t.c.Replica(to)
	if r == nil {
		return raft.AppendResponse{}, fmt.Errorf("replica %d is down", to)
	}
	return r.AppendEntries(req), nil
}

// NewCluster creates n replicas with independent durable directories.
// apply receives every committed command (also during recovery replay).
func NewCluster(baseDir string, n int, apply ApplyFunc, opts ...Option) (*Cluster, error) {
	if n < 1 {
		return nil, fmt.Errorf("cluster: n must be >= 1")
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	c := &Cluster{
		n:        n,
		baseDir:  baseDir,
		peers:    ids,
		apply:    apply,
		tickStep: 5 * time.Millisecond,
		replicas: make(map[uint64]*raft.Replica),
		down:     make(map[uint64]bool),
		partOf:   make(map[uint64]int),
		paused:   make(map[uint64]bool),
		stopCh:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	for _, id := range ids {
		if err := c.spawn(id); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// spawn creates a replica instance from its durable directory.
func (c *Cluster) spawn(id uint64) error {
	tr := &inprocTransport{c: c}
	cfg := raft.Config{
		ID:        id,
		Peers:     append([]uint64{}, c.peers...),
		Dir:       filepath.Join(c.baseDir, fmt.Sprintf("replica-%d", id)),
		Transport: tr,
		Apply: func(index, term uint64, data []byte) {
			if c.apply != nil {
				c.apply(id, index, term, data)
			}
		},
	}
	r, err := raft.NewReplica(cfg)
	if err != nil {
		return fmt.Errorf("cluster: start replica %d: %w", id, err)
	}
	c.replicas[id] = r
	c.down[id] = false
	return nil
}

// Start launches the tick driver.
func (c *Cluster) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.tickStep)
		defer t.Stop()
		for {
			select {
			case <-c.stopCh:
				return
			case <-t.C:
				c.mu.Lock()
				live := make([]*raft.Replica, 0, c.n)
				for _, id := range c.peers {
					if !c.down[id] && !c.paused[id] {
						live = append(live, c.replicas[id])
					}
				}
				c.mu.Unlock()
				for _, r := range live {
					r.Tick(c.tickStep)
				}
			}
		}
	}()
}

// Stop shuts every replica down gracefully and stops the driver.
func (c *Cluster) Stop() error {
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	c.wg.Wait()
	var firstErr error
	c.mu.Lock()
	var live []*raft.Replica
	for _, id := range c.peers {
		if r := c.replicas[id]; r != nil && !c.down[id] {
			live = append(live, r)
		}
	}
	c.mu.Unlock()
	for _, r := range live {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// WaitLeader blocks until a stable leader exists and returns its ID.
func (c *Cluster) WaitLeader(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range c.peers {
			r := c.Replica(id)
			if r != nil && r.Role() == raft.Leader {
				return id, nil
			}
		}
		time.Sleep(3 * time.Millisecond)
	}
	return 0, errors.New("cluster: no leader within timeout")
}

// Replica returns one live replica (nil if currently crashed).
func (c *Cluster) Replica(id uint64) *raft.Replica {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down[id] {
		return nil
	}
	return c.replicas[id]
}

// Propose submits data through replica id.
func (c *Cluster) Propose(id uint64, ctx context.Context, data []byte) (raft.ProposeOutcome, error) {
	r := c.Replica(id)
	if r == nil {
		return raft.ProposeOutcome{}, &raft.Error{Code: raft.RejectClosed, Msg: fmt.Sprintf("replica %d is down", id)}
	}
	return r.Propose(ctx, data)
}

// ReadBarrier performs a leadership confirmation round through replica id.
func (c *Cluster) ReadBarrier(id uint64, ctx context.Context) error {
	r := c.Replica(id)
	if r == nil {
		return &raft.Error{Code: raft.RejectClosed, Msg: fmt.Sprintf("replica %d is down", id)}
	}
	return r.ReadBarrier(ctx)
}

// Crash hard-stops replica id WITHOUT graceful shutdown, abandoning all
// in-memory state. Its durable directory is untouched.
func (c *Cluster) Crash(id uint64) {
	c.mu.Lock()
	if c.down[id] {
		c.mu.Unlock()
		return
	}
	c.down[id] = true
	r := c.replicas[id]
	c.mu.Unlock()
	// Release the durable file as a real process exit would; no shutdown
	// handshake is performed. Synchronous so a subsequent Restart never
	// races an old file handle.
	_ = r.Close()
}

// Restart rebuilds a crashed replica from its durable directory. Its
// state machine is rebuilt by replaying committed entries after it
// catches up (Apply is invoked again, which an idempotent FSM tolerates).
func (c *Cluster) Restart(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.down[id] {
		return nil
	}
	if err := c.spawn(id); err != nil {
		return err
	}
	return nil
}

// IsDown reports whether replica id is currently crashed.
func (c *Cluster) IsDown(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.down[id]
}

// Partition isolates replicas: members listed in ids can only talk to
// each other; all other replicas form the complement group.
func (c *Cluster) Partition(ids ...uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	iso := make(map[uint64]bool)
	for _, id := range ids {
		iso[id] = true
	}
	for _, id := range c.peers {
		if iso[id] {
			c.partOf[id] = 1
		} else {
			c.partOf[id] = 2
		}
	}
}

// Heal removes all partitions.
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partOf = make(map[uint64]int)
}

// canTalk reports whether the simulated network permits from->to.
func (c *Cluster) canTalk(from, to uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down[from] || c.down[to] {
		return false
	}
	gf, fok := c.partOf[from]
	gt, tok := c.partOf[to]
	if !fok || !tok {
		return true
	}
	return gf == gt
}

// Peers returns the replica IDs.
func (c *Cluster) Peers() []uint64 { return append([]uint64{}, c.peers...) }

// Majority returns the majority quorum size.
func (c *Cluster) Majority() int { return c.n/2 + 1 }
