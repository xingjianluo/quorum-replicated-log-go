// Package raft implements a compact, single-process Raft-style consensus
// replica: leader election, majority quorum replication, durable log,
// idempotent crash recovery and linearizable-read barriers.
//
// Concurrency model: every Replica runs one serial event-loop goroutine.
// All state mutations happen on that loop, so concurrent Propose calls
// obtain a total (deterministic arrival) order without data races. Incoming
// RPCs are enqueued as tasks and answered on the loop.
package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/xingjianluo/quorum-replicated-log-go/wal"
)

// Role of a replica.
type Role int

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Replica roles.
const (
	Follower Role = iota
	Candidate
	Leader
)

// EntryType distinguishes client commands from internal no-op entries.
type EntryType uint8

const (
	// EntryCommand carries raw client command bytes.
	EntryCommand EntryType = 0
	// EntryNoop is appended by a fresh leader; committing it lets the
	// leader commit entries left over from previous terms (Raft §8).
	EntryNoop EntryType = 1
)

// Entry is one log record.
type Entry struct {
	Index uint64    `json:"i"`
	Term  uint64    `json:"t"`
	Type  EntryType `json:"y"`
	Data  []byte    `json:"d,omitempty"`
}

// VoteRequest is the RequestVote RPC payload.
type VoteRequest struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

// VoteResponse is the RequestVote RPC reply.
type VoteResponse struct {
	Term      uint64
	Granted   bool
	ReplicaID uint64
}

// AppendRequest is the AppendEntries RPC payload (heartbeat when Entries empty).
type AppendRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendResponse is the AppendEntries RPC reply.
type AppendResponse struct {
	Term          uint64
	Success       bool
	ReplicaID     uint64
	ConflictIndex uint64
}

// Transport is the network abstraction. The in-process cluster implements it.
type Transport interface {
	RequestVote(from, to uint64, req VoteRequest) (VoteResponse, error)
	AppendEntries(from, to uint64, req AppendRequest) (AppendResponse, error)
}

// Config configures a Replica.
type Config struct {
	ID                 uint64
	Peers              []uint64
	Dir                string
	Transport          Transport
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	ProposeTimeout     time.Duration
	RPCTimeout         time.Duration
	// Apply is invoked in commit order for each newly committed command.
	// No-op entries are skipped. Callers must make Apply idempotent with
	// regard to index for crash-recovery replay.
	Apply func(index, term uint64, data []byte)
}

// RejectCode distinguishes rejection reasons for clients and tests.
type RejectCode string

// Distinguishable rejection reasons.
const (
	RejectNotLeader    RejectCode = "NOT_LEADER"
	RejectStaleTerm    RejectCode = "STALE_TERM"
	RejectNoQuorum     RejectCode = "NO_QUORUM"
	RejectStaleRead    RejectCode = "STALE_READ"
	RejectClosed       RejectCode = "REPLICA_CLOSED"
	RejectEmptyRequest RejectCode = "EMPTY_REQUEST"
)

// Error is a typed consensus rejection so callers can tell failure causes apart.
type Error struct {
	Code       RejectCode
	Msg        string
	Term       uint64
	LeaderHint uint64
}

func (e *Error) Error() string {
	return fmt.Sprintf("raft[%s] term=%d hint=%d: %s", e.Code, e.Term, e.LeaderHint, e.Msg)
}

func (r *Replica) err(code RejectCode, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...), Term: r.term, LeaderHint: r.leaderHint}
}

// ProposeOutcome is committed proposal information returned to the client.
type ProposeOutcome struct {
	Index uint64
	Term  uint64
}

type proposalWait struct {
	index uint64
	term  uint64
	ch    chan error
}

type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor uint64 `json:"voted_for"`
}

// Replica is one consensus replica with its durable log.
type Replica struct {
	cfg Config

	loop   chan func()
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup
	w      *wal.WAL

	// Persistent state.
	term     uint64
	votedFor uint64
	log      []Entry // dummy Entry{} at index 0

	// Volatile state.
	role        Role
	leaderHint  uint64
	commitIndex uint64
	// appliedIndex counts entries handed to Apply (no-ops included as
	// positions); recovery always replays from 1.
	appliedIndex uint64

	// Timers.
	elapsed         time.Duration
	electionTimeout time.Duration
	lastHeartbeat   time.Duration
	lastContact     time.Duration

	// Leader-only bookkeeping.
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
	proposals  map[uint64][]*proposalWait
	granted    map[uint64]bool
	votes      int

	repairNote wal.RepairReport
}

// RepairReport exposes what the durable log recovered/truncated at startup.
func (r *Replica) RepairReport() wal.RepairReport { return r.repairNote }

// NewReplica creates a replica and synchronously recovers durable state.
func NewReplica(cfg Config) (*Replica, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("raft: replica ID must be > 0")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("raft: transport is required")
	}
	if len(cfg.Peers) == 0 {
		cfg.Peers = []uint64{cfg.ID}
	}
	if cfg.ElectionTimeoutMin == 0 {
		cfg.ElectionTimeoutMin = 120 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax == 0 || cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Millisecond
	}
	if cfg.ProposeTimeout == 0 {
		cfg.ProposeTimeout = time.Second
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 60 * time.Millisecond
	}

	w, rep, err := wal.Open(cfg.Dir)
	if err != nil {
		return nil, err
	}
	records, hs, err := w.ScanHardState()
	if err != nil {
		return nil, err
	}
	r := &Replica{
		cfg:        cfg,
		loop:       make(chan func(), 2048),
		closed:     make(chan struct{}),
		w:          w,
		log:        make([]Entry, 1),
		nextIndex:  make(map[uint64]uint64),
		matchIndex: make(map[uint64]uint64),
		proposals:  make(map[uint64][]*proposalWait),
		granted:    make(map[uint64]bool),
		repairNote: rep,
	}
	for _, rec := range records {
		var e Entry
		if err := json.Unmarshal(rec, &e); err != nil {
			return nil, fmt.Errorf("raft: decode durable entry: %w", err)
		}
		if e.Index != uint64(len(r.log)) {
			return nil, fmt.Errorf("raft: non-contiguous durable log: entry index %d, expected %d", e.Index, len(r.log))
		}
		r.log = append(r.log, e)
	}
	if hs != nil {
		var h hardState
		if err := json.Unmarshal(hs, &h); err != nil {
			return nil, fmt.Errorf("raft: decode hardstate: %w", err)
		}
		r.term = h.Term
		r.votedFor = h.VotedFor
	}
	r.resetElectionTimeout()

	r.wg.Add(1)
	go r.run()
	return r, nil
}

func (r *Replica) run() {
	defer r.wg.Done()
	for {
		select {
		case <-r.closed:
			return
		case task := <-r.loop:
			task()
		}
	}
}

func (r *Replica) submit(task func()) bool {
	select {
	case <-r.closed:
		return false
	default:
	}
	select {
	case r.loop <- task:
		return true
	case <-r.closed:
		return false
	}
}

func call[T any](r *Replica, fn func() T) (T, bool) {
	var zero T
	done := make(chan T, 1)
	if !r.submit(func() { done <- fn() }) {
		return zero, false
	}
	select {
	case v := <-done:
		return v, true
	case <-r.closed:
		return zero, false
	}
}

// ---- persistence (event loop only) ----

func (r *Replica) persistHardState() error {
	b, err := json.Marshal(hardState{Term: r.term, VotedFor: r.votedFor})
	if err != nil {
		return err
	}
	return r.w.AppendHardState(b)
}

func encodeEntries(entries []Entry) ([][]byte, error) {
	out := make([][]byte, 0, len(entries))
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

func (r *Replica) appendDurable(entries ...Entry) error {
	recs, err := encodeEntries(entries)
	if err != nil {
		return err
	}
	if err := r.w.AppendBatch(recs); err != nil {
		return err
	}
	r.log = append(r.log, entries...)
	return nil
}

// rewriteSuffix drops data entries after keep and writes replacements,
// keeping the current term/vote hardstate frame.
func (r *Replica) rewriteSuffix(keep int, entries ...Entry) error {
	all := append(append([]Entry{}, r.log[1:keep+1]...), entries...)
	recs, err := encodeEntries(all)
	if err != nil {
		return err
	}
	hs, err := json.Marshal(hardState{Term: r.term, VotedFor: r.votedFor})
	if err != nil {
		return err
	}
	if err := r.w.Rewrite(recs, hs); err != nil {
		return err
	}
	newLog := make([]Entry, 0, keep+1+len(entries))
	newLog = append(newLog, Entry{})
	newLog = append(newLog, all...)
	r.log = newLog
	return nil
}

// ---- log helpers (event loop only) ----

func (r *Replica) lastIndex() uint64 { return uint64(len(r.log) - 1) }

func (r *Replica) lastTerm() uint64 {
	if len(r.log) <= 1 {
		return 0
	}
	return r.log[len(r.log)-1].Term
}

func (r *Replica) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if int(index) >= len(r.log) {
		return 0
	}
	return r.log[index].Term
}

func (r *Replica) isLogUpToDate(lastIdx, lastTerm uint64) bool {
	if lastTerm != r.lastTerm() {
		return lastTerm > r.lastTerm()
	}
	return lastIdx >= r.lastIndex()
}

func (r *Replica) majority() int { return len(r.cfg.Peers)/2 + 1 }

func (r *Replica) resetElectionTimeout() {
	span := int64(r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin)
	if span < 0 {
		span = 0
	}
	src := rand.NewSource(time.Now().UnixNano() ^ int64(r.cfg.ID)*104729 ^ int64(r.term)*7919)
	r.electionTimeout = r.cfg.ElectionTimeoutMin + time.Duration(rand.New(src).Int63n(span+1))
}

// ---- role transitions (event loop only) ----

func (r *Replica) becomeFollower(term uint64, leaderHint uint64) {
	persisted := false
	if term > r.term {
		r.term = term
		r.votedFor = 0
		if err := r.persistHardState(); err != nil {
			// Durable-state failure is unrecoverable locally.
			panic(fmt.Errorf("raft: persist term: %w", err))
		}
		persisted = true
	}
	if r.role != Follower {
		r.failProposals(r.err(RejectStaleTerm, "leadership changed in term %d", r.term))
	}
	r.role = Follower
	if leaderHint != 0 {
		r.leaderHint = leaderHint
	}
	r.elapsed = 0
	r.lastContact = 0
	_ = persisted
	r.resetElectionTimeout()
}

func (r *Replica) startElection() {
	r.role = Candidate
	r.term++
	r.votedFor = r.cfg.ID
	r.leaderHint = 0
	r.elapsed = 0
	if err := r.persistHardState(); err != nil {
		panic(fmt.Errorf("raft: persist vote: %w", err))
	}
	r.granted = map[uint64]bool{r.cfg.ID: true}
	r.votes = 1
	if r.votes >= r.majority() {
		// Single-node cluster (or quorum of one): self-vote elects now;
		// there are no peer responses that would trigger collectVote.
		r.becomeLeader()
		return
	}
	req := VoteRequest{Term: r.term, CandidateID: r.cfg.ID, LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm()}
	for _, p := range r.cfg.Peers {
		if p == r.cfg.ID {
			continue
		}
		peer := p
		go func() {
			resp, err := r.cfg.Transport.RequestVote(r.cfg.ID, peer, req)
			if err != nil {
				return
			}
			r.submit(func() { r.collectVote(resp) })
		}()
	}
}

func (r *Replica) collectVote(resp VoteResponse) {
	if resp.Term > r.term {
		r.becomeFollower(resp.Term, 0)
		return
	}
	if r.role != Candidate || resp.Term != r.term {
		return
	}
	if resp.Granted && !r.granted[resp.ReplicaID] {
		r.granted[resp.ReplicaID] = true
		r.votes++
	}
	if r.votes >= r.majority() {
		r.becomeLeader()
	}
}

func (r *Replica) becomeLeader() {
	r.role = Leader
	r.leaderHint = r.cfg.ID
	r.lastHeartbeat = 0
	r.lastContact = 0
	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = r.lastIndex() + 1
		r.matchIndex[p] = 0
	}
	r.matchIndex[r.cfg.ID] = r.lastIndex()
	noop := Entry{Index: r.lastIndex() + 1, Term: r.term, Type: EntryNoop}
	if err := r.appendDurable(noop); err != nil {
		panic(fmt.Errorf("raft: persist noop: %w", err))
	}
	r.matchIndex[r.cfg.ID] = r.lastIndex()
	r.replicate()
}

// ---- proposals ----

func (r *Replica) failProposals(err error) {
	for idx, waits := range r.proposals {
		for _, w := range waits {
			select {
			case w.ch <- err:
			default:
			}
		}
		delete(r.proposals, idx)
	}
}

// commitThrough applies entries strictly after appliedIndex up to index,
// in order, invoking the state-machine callback exactly once per command.
func (r *Replica) commitThrough(index uint64) {
	if index <= r.commitIndex {
		return
	}
	r.commitIndex = index
	for r.appliedIndex < r.commitIndex {
		r.appliedIndex++
		e := r.log[r.appliedIndex]
		if waits, ok := r.proposals[e.Index]; ok {
			for _, w := range waits {
				select {
				case w.ch <- nil:
				default:
				}
			}
			delete(r.proposals, e.Index)
		}
		if e.Type == EntryCommand && r.cfg.Apply != nil {
			r.cfg.Apply(e.Index, e.Term, e.Data)
		}
	}
}

// Propose appends and replicates data; returns after commit or a typed error.
func (r *Replica) Propose(ctx context.Context, data []byte) (ProposeOutcome, error) {
	if len(data) == 0 {
		return ProposeOutcome{}, &Error{Code: RejectEmptyRequest, Msg: "proposal payload must not be empty"}
	}
	type resT struct {
		out ProposeOutcome
		err error
	}
	resCh := make(chan resT, 1)
	ok := r.submit(func() {
		if r.role != Leader {
			resCh <- resT{err: r.err(RejectNotLeader, "replica %d is %s in term %d; redirect to leader %d", r.cfg.ID, r.role, r.term, r.leaderHint)}
			return
		}
		e := Entry{Index: r.lastIndex() + 1, Term: r.term, Type: EntryCommand, Data: data}
		if err := r.appendDurable(e); err != nil {
			resCh <- resT{err: err}
			return
		}
		r.matchIndex[r.cfg.ID] = e.Index
		wait := &proposalWait{index: e.Index, term: e.Term, ch: make(chan error, 1)}
		r.proposals[e.Index] = append(r.proposals[e.Index], wait)
		r.replicate()
		go func() {
			timer := time.NewTimer(r.cfg.ProposeTimeout)
			defer timer.Stop()
			select {
			case err := <-wait.ch:
				r.submit(func() {
					if err != nil {
						resCh <- resT{err: err}
						return
					}
					resCh <- resT{out: ProposeOutcome{Index: wait.index, Term: wait.term}}
				})
			case <-timer.C:
				r.submit(func() {
					if r.role == Leader && r.term == wait.term && r.commitIndex >= wait.index {
						resCh <- resT{out: ProposeOutcome{Index: wait.index, Term: wait.term}}
						return
					}
					if r.role != Leader || r.term != wait.term {
						resCh <- resT{err: r.err(RejectStaleTerm, "term %d ended (leadership change) before quorum; the stale write is not accepted", wait.term)}
						return
					}
					resCh <- resT{err: r.err(RejectNoQuorum, "only %d of %d replicas reachable; majority is %d", r.reachableFor(wait.index), len(r.cfg.Peers), r.majority())}
				})
			case <-ctx.Done():
				r.submit(func() {
					if r.commitIndex >= wait.index {
						resCh <- resT{out: ProposeOutcome{Index: wait.index, Term: wait.term}}
						return
					}
					resCh <- resT{err: r.err(RejectNoQuorum, "client gave up before quorum: %v", ctx.Err())}
				})
			case <-r.closed:
				select {
				case resCh <- resT{err: &Error{Code: RejectClosed, Msg: "replica closed"}}:
				default:
				}
			}
		}()
	})
	if !ok {
		return ProposeOutcome{}, &Error{Code: RejectClosed, Msg: "replica closed"}
	}
	select {
	case res := <-resCh:
		return res.out, res.err
	case <-r.closed:
		return ProposeOutcome{}, &Error{Code: RejectClosed, Msg: "replica closed"}
	}
}

// reachableFor returns 1 (self) plus followers matching at least index.
func (r *Replica) reachableFor(index uint64) int {
	n := 1
	for _, p := range r.cfg.Peers {
		if p == r.cfg.ID {
			continue
		}
		if r.matchIndex[p] >= index {
			n++
		}
	}
	return n
}

// ---- replication (event loop only) ----

type appendOutcome struct {
	peer       uint64
	ok         bool
	term       uint64
	conflict   uint64
	sentPrev   uint64
	sentNumber int
	reachable  bool
}

// replicate performs one parallel AppendEntries round. It runs on the
// leader event loop; RPCs execute on goroutines and slow/dead peers
// count as "not reachable" after RPCTimeout.
func (r *Replica) replicate() int {
	if r.role != Leader {
		return 0
	}
	outcomes := make(chan appendOutcome, len(r.cfg.Peers))
	pending := 0
	for _, p := range r.cfg.Peers {
		if p == r.cfg.ID {
			continue
		}
		peer := p
		ni := r.nextIndex[peer]
		var entries []Entry
		if ni <= r.lastIndex() {
			entries = append([]Entry{}, r.log[ni:]...)
		}
		req := AppendRequest{
			Term:         r.term,
			LeaderID:     r.cfg.ID,
			PrevLogIndex: ni - 1,
			PrevLogTerm:  r.termAt(ni - 1),
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		pending++
		go func() {
			oc := appendOutcome{peer: peer, sentPrev: ni - 1, sentNumber: len(entries)}
			resp, err := r.cfg.Transport.AppendEntries(r.cfg.ID, peer, req)
			if err != nil {
				outcomes <- oc
				return
			}
			oc.reachable = true
			oc.ok = resp.Success
			oc.term = resp.Term
			oc.conflict = resp.ConflictIndex
			outcomes <- oc
		}()
	}

	acks := 1 // self
	deadline := time.After(r.cfg.RPCTimeout)
	finishEarly := false
	for pending > 0 && !finishEarly {
		select {
		case oc := <-outcomes:
			pending--
			if r.role != Leader {
				finishEarly = true
				continue
			}
			if oc.term > r.term {
				r.becomeFollower(oc.term, 0)
				finishEarly = true
				continue
			}
			if oc.reachable && oc.term == r.term {
				if oc.ok {
					acks++
					match := oc.sentPrev + uint64(oc.sentNumber)
					if match > r.matchIndex[oc.peer] {
						r.matchIndex[oc.peer] = match
					}
					r.nextIndex[oc.peer] = match + 1
				} else {
					ni := oc.conflict
					if ni == 0 || ni > r.lastIndex()+1 {
						ni = r.lastIndex() + 1
					}
					if cur := r.nextIndex[oc.peer]; ni >= cur && cur > 1 {
						ni = cur - 1
					}
					r.nextIndex[oc.peer] = ni
				}
			}
			// Quorum already reached: commit what we have; stragglers
			// are repaired on later rounds. Or quorum is already
			// impossible: stop waiting for dead/slow peers.
			if acks >= r.majority() || acks+pending < r.majority() {
				finishEarly = true
			}
		case <-deadline:
			pending = 0
		}
	}

	if acks >= r.majority() {
		r.lastContact = 0
	}
	// Commit rule (Raft §5.4.2, safety): a leader advances commitIndex
	// only on entries of ITS CURRENT term that a quorum replicated. An
	// entry appended in an earlier term - notably one appended by a
	// leader that had lost majority contact - can NEVER be committed by
	// this replica directly: a newer leader overwrites it, or it is
	// committed transitively only after a current-term entry (the new
	// leader's no-op) precedes it. Thus rejected/out-of-quorum writes can
	// never become visible.
	if r.role != Leader {
		return acks
	}
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.log[n].Term != r.term {
			continue
		}
		cnt := 1
		for _, p := range r.cfg.Peers {
			if p != r.cfg.ID && r.matchIndex[p] >= n {
				cnt++
			}
		}
		if cnt >= r.majority() {
			r.commitThrough(n)
			break
		}
	}
	return acks
}

// ReadBarrier confirms leadership with a fresh quorum heartbeat round
// before serving a read. Failure to reach the majority means the replica
// cannot prove it is still leader; the read is refused, never served stale.
func (r *Replica) ReadBarrier(ctx context.Context) error {
	done := make(chan error, 1)
	ok := r.submit(func() {
		if r.role != Leader {
			done <- r.err(RejectNotLeader, "reads are served by the leader only; hint=%d", r.leaderHint)
			return
		}
		acks := r.replicate()
		if acks < r.majority() {
			done <- r.err(RejectStaleRead, "only %d/%d confirmed current leadership; majority is %d, refusing potentially stale read", acks, len(r.cfg.Peers), r.majority())
			return
		}
		done <- nil
	})
	if !ok {
		return &Error{Code: RejectClosed, Msg: "replica closed"}
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return &Error{Code: RejectStaleRead, Msg: "read barrier timed out: " + ctx.Err().Error()}
	case <-r.closed:
		return &Error{Code: RejectClosed, Msg: "replica closed"}
	}
}

// ---- RPC entrypoints (called by transports; executed on the loop) ----

// RequestVote handles a RequestVote RPC.
func (r *Replica) RequestVote(req VoteRequest) VoteResponse {
	v, ok := call(r, func() VoteResponse {
		resp := VoteResponse{ReplicaID: r.cfg.ID, Term: r.term}
		if req.Term < r.term {
			return resp
		}
		if req.Term > r.term {
			r.becomeFollower(req.Term, 0)
		}
		resp.Term = r.term
		upToDate := r.isLogUpToDate(req.LastLogIndex, req.LastLogTerm)
		if (r.votedFor == 0 || r.votedFor == req.CandidateID) && upToDate {
			r.votedFor = req.CandidateID
			if err := r.persistHardState(); err != nil {
				panic(fmt.Errorf("raft: persist vote: %w", err))
			}
			r.elapsed = 0
			resp.Granted = true
		}
		return resp
	})
	if !ok {
		return VoteResponse{ReplicaID: r.cfg.ID, Term: r.term}
	}
	return v
}

// AppendEntries handles an AppendEntries RPC.
func (r *Replica) AppendEntries(req AppendRequest) AppendResponse {
	v, ok := call(r, func() AppendResponse {
		resp := AppendResponse{ReplicaID: r.cfg.ID, Term: r.term}
		if req.Term < r.term {
			return resp
		}
		if req.Term > r.term || r.role != Follower {
			r.becomeFollower(req.Term, req.LeaderID)
		} else {
			r.leaderHint = req.LeaderID
		}
		resp.Term = r.term
		r.elapsed = 0

		// Log consistency check at PrevLogIndex.
		if req.PrevLogIndex > r.lastIndex() {
			resp.ConflictIndex = r.lastIndex() + 1
			return resp
		}
		if r.termAt(req.PrevLogIndex) != req.PrevLogTerm {
			conflictTerm := r.termAt(req.PrevLogIndex)
			idx := req.PrevLogIndex
			for idx > 1 && r.termAt(idx-1) == conflictTerm {
				idx--
			}
			resp.ConflictIndex = idx
			return resp
		}

		// Merge entries, dropping any conflicting suffix.
		start := req.PrevLogIndex + 1
		i := 0
		for i < len(req.Entries) && start+uint64(i) <= r.lastIndex() &&
			r.log[start+uint64(i)].Term == req.Entries[i].Term {
			i++
		}
		if i < len(req.Entries) {
			keep := int(start) + i - 1
			if start+uint64(i) <= r.lastIndex() {
				if err := r.rewriteSuffix(keep, req.Entries[i:]...); err != nil {
					panic(fmt.Errorf("raft: rewrite suffix: %w", err))
				}
			} else {
				if err := r.appendDurable(req.Entries[i:]...); err != nil {
					panic(fmt.Errorf("raft: append entries: %w", err))
				}
			}
		}

		if req.LeaderCommit > r.commitIndex {
			hc := req.LeaderCommit
			if hc > r.lastIndex() {
				hc = r.lastIndex()
			}
			r.commitThrough(hc)
		}
		resp.Success = true
		return resp
	})
	if !ok {
		return AppendResponse{ReplicaID: r.cfg.ID, Term: r.term}
	}
	return v
}

// Tick advances the replica's logical timer by d. Safe to call on a
// non-running replica (the call is dropped).
func (r *Replica) Tick(d time.Duration) {
	r.submit(func() {
		switch r.role {
		case Leader:
			r.lastHeartbeat += d
			r.lastContact += d
			if r.lastContact >= r.cfg.ElectionTimeoutMax {
				// Cannot contact a quorum for a full election timeout:
				// stop acting as leader without blaming a term change.
				r.stepDownNoQuorum()
				return
			}
			if r.lastHeartbeat >= r.cfg.HeartbeatInterval {
				r.lastHeartbeat = 0
				r.replicate()
			}
		default:
			r.elapsed += d
			if r.elapsed >= r.electionTimeout {
				if r.role == Candidate {
					// The current election failed: randomise a fresh
					// timeout before trying again so an isolated replica
					// does not burn a new term on every single tick.
					r.resetElectionTimeout()
				}
				r.startElection()
			}
		}
	})
}

// stepDownNoQuorum is used when a leader has not contacted a quorum for a
// full election timeout. No higher term has been observed, so the term is
// NOT known to have ended: pending writes are rejected as NO_QUORUM (the
// client may retry). STALE_TERM is reserved for a genuine term change
// caused by discovering a newer leader.
func (r *Replica) stepDownNoQuorum() {
	r.role = Follower
	r.leaderHint = 0
	r.elapsed = 0
	r.lastContact = 0
	r.failProposals(r.err(RejectNoQuorum, "leader lost majority contact; write cannot be committed in term %d", r.term))
	r.resetElectionTimeout()
}

// ---- accessors ----

// ID returns the replica ID.
func (r *Replica) ID() uint64 { return r.cfg.ID }

// Term returns the current term.
func (r *Replica) Term() uint64 {
	v, ok := call(r, func() uint64 { return r.term })
	if !ok {
		return 0
	}
	return v
}

// Role returns the current role.
func (r *Replica) Role() Role {
	v, ok := call(r, func() Role { return r.role })
	if !ok {
		return Follower
	}
	return v
}

// LeaderHint returns the last known leader ID (0 if unknown).
func (r *Replica) LeaderHint() uint64 {
	v, ok := call(r, func() uint64 { return r.leaderHint })
	if !ok {
		return 0
	}
	return v
}

// CommitIndex returns the highest known committed index.
func (r *Replica) CommitIndex() uint64 {
	v, ok := call(r, func() uint64 { return r.commitIndex })
	if !ok {
		return 0
	}
	return v
}

// AppliedIndex returns the highest applied log index.
func (r *Replica) AppliedIndex() uint64 {
	v, ok := call(r, func() uint64 { return r.appliedIndex })
	if !ok {
		return 0
	}
	return v
}

// LastLogIndex returns the highest durable log index.
func (r *Replica) LastLogIndex() uint64 {
	v, ok := call(r, func() uint64 { return r.lastIndex() })
	if !ok {
		return 0
	}
	return v
}

// Close persists and releases resources. It does NOT simulate a crash;
// Cluster.Crash abandons the replica without Close.
func (r *Replica) Close() error {
	var err error
	r.once.Do(func() {
		// Best-effort: flush pending tasks state, then stop the loop.
		close(r.closed)
		r.wg.Wait()
		err = r.w.Close()
	})
	return err
}
