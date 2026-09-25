package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// deadTransport is never used in a single-node cluster (majority is the
// node itself, so no RPC is needed to elect or commit).
type deadTransport struct{}

func (deadTransport) RequestVote(from, to uint64, _ VoteRequest) (VoteResponse, error) {
	return VoteResponse{}, errors.New("no network in single-node test")
}
func (deadTransport) AppendEntries(from, to uint64, _ AppendRequest) (AppendResponse, error) {
	return AppendResponse{}, errors.New("no network in single-node test")
}

func newSingle(t *testing.T, dir string) *Replica {
	t.Helper()
	r, err := NewReplica(Config{
		ID:                 1,
		Peers:              []uint64{1},
		Dir:                dir,
		Transport:          deadTransport{},
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		ProposeTimeout:     500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewReplica: %v", err)
	}
	return r
}

func drive(r *Replica, d time.Duration, n int) {
	for i := 0; i < n; i++ {
		r.Tick(d)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSingleNodeCommitAndRecover(t *testing.T) {
	dir := t.TempDir()
	r := newSingle(t, dir)
	drive(r, 5*time.Millisecond, 12)
	if r.Role() != Leader {
		t.Fatalf("single node must elect itself, role=%s", r.Role())
	}
	t.Logf("input: single-node cluster; role=%s term=%d judgement: self-vote forms majority of 1", r.Role(), r.Term())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	out, err := r.Propose(ctx, []byte("hello"))
	cancel()
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if out.Index != 2 { // index 1 is the leader's no-op
		t.Fatalf("first command index=%d want 2 (index 1 is no-op)", out.Index)
	}
	t.Logf("input: Propose(hello); committed index=%d term=%d judgement: quorum-of-self commits durably", out.Index, out.Term)

	// Graceful restart: durable log + hardstate are recovered.
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r2 := newSingle(t, dir)
	defer r2.Close()
	if r2.LastLogIndex() != 2 || r2.Term() != 1 {
		t.Fatalf("recovery: lastIndex=%d term=%d want 2,1", r2.LastLogIndex(), r2.Term())
	}
	if rep := r2.RepairReport(); rep.TruncatedBytes != 0 {
		t.Fatalf("clean log must need no repair, got %+v", rep)
	}
	t.Logf("judgement: after restart lastLogIndex=%d term=%d, no repair needed", r2.LastLogIndex(), r2.Term())
}

func TestSingleNodeTornTailReported(t *testing.T) {
	dir := t.TempDir()
	r := newSingle(t, dir)
	drive(r, 5*time.Millisecond, 12)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if _, err := r.Propose(ctx, []byte("durable")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "wal.log")
	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-1); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: chop 1 byte off wal.log tail (%d bytes); restart must detect partial-frame", st.Size())

	r2 := newSingle(t, dir)
	defer r2.Close()
	rep := r2.RepairReport()
	t.Logf("repair=%+v lastLogIndex=%d judgement: torn tail truncated at open, never applied", rep, r2.LastLogIndex())
	if rep.TruncatedBytes == 0 || rep.Reason != "partial-frame" {
		t.Fatalf("torn tail must be reported and truncated, got %+v", rep)
	}
	// The intact prefix (noop + command frames minus the torn final frame)
	// is all that remains.
	if r2.LastLogIndex() > 2 {
		t.Fatalf("unexpected entries after repair: %d", r2.LastLogIndex())
	}
}

func TestSingleNodeReadBarrierAndRejectCodes(t *testing.T) {
	dir := t.TempDir()
	// A fresh replica before any ticks is follower: propose must be
	// rejected as NOT_LEADER with a typed error.
	r := newSingle(t, dir)
	defer r.Close()
	if _, err := r.Propose(context.Background(), []byte("x")); err == nil {
		t.Fatal("propose before election must fail")
	} else {
		var re *Error
		if !errors.As(err, &re) || re.Code != RejectNotLeader {
			t.Fatalf("want NOT_LEADER, got %v", err)
		}
		t.Logf("input: propose before election; judgement: rejected code=%s", re.Code)
	}
	if _, err := r.Propose(context.Background(), nil); err == nil {
		t.Fatal("empty proposal must fail")
	} else {
		var re *Error
		if !errors.As(err, &re) || re.Code != RejectEmptyRequest {
			t.Fatalf("want EMPTY_REQUEST, got %v", err)
		}
	}

	drive(r, 5*time.Millisecond, 12)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.ReadBarrier(ctx); err != nil {
		t.Fatalf("leader read barrier: %v", err)
	}
	t.Log("judgement: elected leader passes ReadBarrier with quorum-of-self")
}
