// Package store is the replicated key/value state machine. It is
// deterministic: the same log prefix always produces the same state.
package store

import (
	"fmt"
	"strconv"
	"sync"
)

// Op is the kind of a replicated command.
type Op string

// Supported operations.
const (
	OpSet    Op = "Set"
	OpDelete Op = "Delete"
	OpIncr   Op = "Incr"
)

// Command is one client request, encoded into a raft log entry.
// ClientID/RequestID make retries idempotent.
type Command struct {
	Op        Op     `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	Delta     int64  `json:"delta,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// ApplyResult is the deterministic outcome of applying one Command.
type ApplyResult struct {
	Index   uint64 `json:"index"`
	Value   string `json:"value"`
	Counter int64  `json:"counter"`
	Existed bool   `json:"existed"`
	// Duplicate is true when this Apply call changed nothing because the
	// request had already been applied (replay or client retry).
	Duplicate bool `json:"duplicate"`
}

// KVStore is the deterministic key/value state machine.
type KVStore struct {
	mu          sync.Mutex
	data        map[string]string
	counters    map[string]int64
	lastApplied uint64
	// byRequest caches the original result of each client request so a
	// retry returns the same outcome without re-executing it.
	byRequest map[string]ApplyResult
	// byIndex caches results by log index so recovery replay is a no-op
	// for already-applied entries.
	byIndex map[uint64]ApplyResult
}

// New creates an empty state machine.
func New() *KVStore {
	return &KVStore{
		data:      make(map[string]string),
		counters:  make(map[string]int64),
		byRequest: make(map[string]ApplyResult),
		byIndex:   make(map[uint64]ApplyResult),
	}
}

// Apply applies cmd at log index. It is idempotent in two ways:
//
//   - index <= lastApplied (e.g. recovery replay): the cached result for
//     that index is returned and state is not touched;
//   - a non-empty RequestID seen before (client retry): the original
//     result is returned and counters are not incremented again.
func (s *KVStore) Apply(index uint64, cmd Command) (ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return ApplyResult{}, fmt.Errorf("store: log index starts at 1, got 0")
	}

	// Guard 1: never apply an index more than once. Raft indices may skip
	// values at the FSM layer (internal no-op entries are not delivered),
	// so the invariant is strictly increasing indices - never a gap-free
	// stream. Replaying an index <= lastApplied returns the cached result
	// and changes nothing.
	if index <= s.lastApplied {
		if r, ok := s.byIndex[index]; ok {
			r.Duplicate = true
			return r, nil
		}
		return ApplyResult{Index: index, Duplicate: true}, nil
	}

	// Guard 2: a retried client request returns its first result verbatim.
	if cmd.RequestID != "" {
		if r, ok := s.byRequest[cmd.RequestID]; ok {
			r.Duplicate = true
			// The retry occupies this log index in the stream even
			// though it changed nothing.
			s.lastApplied = index
			s.byIndex[index] = r
			return r, nil
		}
	}

	res := ApplyResult{Index: index}
	switch cmd.Op {
	case OpSet:
		if cmd.Key == "" {
			return ApplyResult{}, fmt.Errorf("store: Set requires a key")
		}
		_, res.Existed = s.data[cmd.Key]
		s.data[cmd.Key] = cmd.Value
		res.Value = cmd.Value
		res.Counter = s.counters[cmd.Key]
	case OpDelete:
		if cmd.Key == "" {
			return ApplyResult{}, fmt.Errorf("store: Delete requires a key")
		}
		res.Value, res.Existed = s.data[cmd.Key]
		delete(s.data, cmd.Key)
		delete(s.counters, cmd.Key)
	case OpIncr:
		if cmd.Key == "" {
			return ApplyResult{}, fmt.Errorf("store: Incr requires a key")
		}
		delta := cmd.Delta
		if delta == 0 {
			delta = 1
		}
		v := s.counters[cmd.Key] + delta
		s.counters[cmd.Key] = v
		res.Counter = v
		if sv, ok := s.data[cmd.Key]; ok {
			res.Value = sv
		} else {
			res.Value = strconv.FormatInt(v, 10)
		}
		_, res.Existed = s.data[cmd.Key]
	default:
		return ApplyResult{}, fmt.Errorf("store: unknown op %q", cmd.Op)
	}

	s.lastApplied = index
	s.byIndex[index] = res
	if cmd.RequestID != "" {
		s.byRequest[cmd.RequestID] = res
	}
	return res, nil
}

// Get returns the committed value of key. Counters are rendered as
// decimal strings for keys without an explicit Set value.
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.data[key]; ok {
		return v, true
	}
	if c, ok := s.counters[key]; ok {
		return strconv.FormatInt(c, 10), true
	}
	return "", false
}

// LastApplied returns the highest applied log index.
func (s *KVStore) LastApplied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastApplied
}

// ResultFor returns the cached result of a previously applied request.
func (s *KVStore) ResultFor(requestID string) (ApplyResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byRequest[requestID]
	return r, ok
}

// Clone returns a deep copy of the committed key/value pairs, including
// counter-only keys rendered as decimal strings.
func (s *KVStore) Clone() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(s.data)+len(s.counters))
	for k, v := range s.counters {
		out[k] = strconv.FormatInt(v, 10)
	}
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
