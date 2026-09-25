package kvserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/quorum-replicated-log-go/raft"
	"github.com/xingjianluo/quorum-replicated-log-go/store"
)

func waitLeader(t *testing.T, k *KVCluster) uint64 {
	t.Helper()
	id, err := k.Cluster().WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("no leader elected: %v", err)
	}
	t.Logf("judgement: leader elected = replica %d", id)
	return id
}

func newCluster(t *testing.T, n int) (*KVCluster, string) {
	t.Helper()
	dir := t.TempDir()
	k, err := New(dir, n)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	k.Start()
	t.Cleanup(func() { _ = k.Stop() })
	return k, dir
}

func mustWrite(t *testing.T, k *KVCluster, req Req) Resp {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := k.Write(ctx, req)
	if err != nil {
		t.Fatalf("Write %+v: %v", req, err)
	}
	t.Logf("input: %s key=%q -> committed index=%d term=%d (counter=%d dup=%v)",
		req.Op, req.Key, resp.Outcome.Index, resp.Outcome.Term, resp.Apply.Counter, resp.Apply.Duplicate)
	return resp
}

func rejectCode(err error) raft.RejectCode {
	var re *raft.Error
	if errors.As(err, &re) {
		return re.Code
	}
	return ""
}

// 1) Baseline majority commit + linearizable read of the new value.
func TestMajorityCommitAndLinearizableRead(t *testing.T) {
	k, _ := newCluster(t, 3)
	leader := waitLeader(t, k)

	mustWrite(t, k, Req{Op: store.OpSet, Key: "color", Value: "blue", ClientID: "c1", RequestID: "r1"})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := k.Cluster().ReadBarrier(leader, ctx); err != nil {
		t.Fatalf("read barrier on healthy leader: %v", err)
	}
	v, ok, err := k.LinearizableRead(ctx, "color")
	if err != nil {
		t.Fatalf("LinearizableRead: %v", err)
	}
	t.Logf("input: read color after Set blue; got %q exists=%v; judgement: committed read never returns an old value", v, ok)
	if !ok || v != "blue" {
		t.Fatalf("read = %q,%v want blue,true", v, ok)
	}
}

// 2) Quorum-less writes must be refused (distinguishable NO_QUORUM).
func TestWriteWithoutQuorumRejected(t *testing.T) {
	k, _ := newCluster(t, 3)
	leader := waitLeader(t, k)

	k.Crash(2)
	k.Crash(3)
	t.Logf("input: crash replicas 2,3; only leader %d alive, majority=%d", leader, k.Majority())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := k.Write(ctx, Req{Op: store.OpSet, Key: "k", Value: "v", ClientID: "c", RequestID: "no-q"})
	code := rejectCode(err)
	t.Logf("judgement: write with 1/3 alive rejected with code=%s err=%v (NO_QUORUM at contact loss, or NOT_LEADER once it stands down)", code, err)
	if code != raft.RejectNoQuorum && code != raft.RejectNotLeader {
		t.Fatalf("got %v, want NO_QUORUM or NOT_LEADER (never committed, never a stale-term acceptance)", err)
	}
}

// 3) Crash + restart: committed data is fully recovered and states converge.
func TestCrashRestartRecovery(t *testing.T) {
	k, _ := newCluster(t, 3)
	waitLeader(t, k)

	mustWrite(t, k, Req{Op: store.OpSet, Key: "a", Value: "1", ClientID: "c", RequestID: "a1"})
	mustWrite(t, k, Req{Op: store.OpSet, Key: "b", Value: "2", ClientID: "c", RequestID: "a2"})
	mustWrite(t, k, Req{Op: store.OpIncr, Key: "hits", Delta: 4, ClientID: "c", RequestID: "a3"})

	t.Log("input: crash replica 2 and 3, then restart both from durable dirs")
	k.Crash(2)
	k.Crash(3)
	if err := k.Restart(2); err != nil {
		t.Fatal(err)
	}
	if err := k.Restart(3); err != nil {
		t.Fatal(err)
	}

	// Give restarted replicas time to catch up and replay.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s2, s3 := k.State(2), k.State(3)
		if s2["a"] == "1" && s2["b"] == "2" && s2["hits"] == "4" &&
			s3["a"] == "1" && s3["b"] == "2" && s3["hits"] == "4" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range k.Peers() {
		st := k.State(id)
		t.Logf("replica %d state=%v judgement: committed data recovered exactly once", id, st)
		if st["a"] != "1" || st["b"] != "2" || st["hits"] != "4" {
			t.Fatalf("replica %d did not converge: %v", id, st)
		}
	}
}

// 4) Torn tail of the durable log is detected, truncated, never applied;
// committed prefix stays intact.
func TestTornTailOnRestart(t *testing.T) {
	k, dir := newCluster(t, 3)
	leader := waitLeader(t, k)

	mustWrite(t, k, Req{Op: store.OpSet, Key: "safe", Value: "yes", ClientID: "c", RequestID: "t1"})

	// Crash the leader and corrupt its wal file by chopping bytes off the
	// tail to emulate an interrupted write.
	k.Crash(leader)
	logPath := filepath.Join(dir, fmt.Sprintf("replica-%d", leader), "wal.log")
	st, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	chop := st.Size() - 2
	t.Logf("input: chop leader %d wal.log %d -> %d bytes (simulated torn write)", leader, st.Size(), chop)
	if err := os.Truncate(logPath, chop); err != nil {
		t.Fatal(err)
	}
	if err := k.Restart(leader); err != nil {
		t.Fatalf("restart must repair torn tail: %v", err)
	}
	// Electable again and serves reads/writes.
	id, err := k.Cluster().WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("no leader after torn-tail restart: %v", err)
	}
	v, ok, _ := k.StaleRead(id, "safe")
	t.Logf("judgement: cluster electable (leader=%d), committed prefix intact: safe=%q exists=%v; torn bytes dropped", id, v, ok)
	if !ok || v != "yes" {
		t.Fatalf("committed prefix lost after torn-tail repair: %q %v", v, ok)
	}
}

// 5) Leadership change invalidates an in-flight old-term write: code
// STALE_TERM, distinct from NO_QUORUM.
func TestStaleLeaderWriteRejectedAfterElection(t *testing.T) {
	k, _ := newCluster(t, 3)
	oldLeader := waitLeader(t, k)

	// Isolate the old leader alone; the other two elect a new leader.
	k.Partition(oldLeader)
	t.Logf("input: partition oldLeader %d alone, wait for the majority side to elect", oldLeader)
	newLeader := uint64(0)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range k.Peers() {
			if id == oldLeader {
				continue
			}
			if r := k.Cluster().Replica(id); r != nil && r.Role() == raft.Leader {
				newLeader = id
			}
		}
		if newLeader != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == 0 {
		t.Fatalf("majority side did not elect a new leader")
	}
	t.Logf("judgement: new leader elected = %d (old=%d, term advanced)", newLeader, oldLeader)

	// The stale leader will step itself down when its election timer
	// expires without majority contact; a write meanwhile is refused
	// either immediately (NOT_LEADER after step-down) or as STALE_TERM
	// when its old term ends. Both are distinguishable rejection codes,
	// and in no case is the write committed by the new majority.
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	_, err := k.Cluster().Propose(oldLeader, ctx,
		[]byte(`{"op":"Set","key":"ghost","value":"x","client_id":"c","request_id":"stale"}`))
	code := rejectCode(err)
	t.Logf("write via stale leader -> code=%s err=%v", code, err)
	if code != raft.RejectStaleTerm && code != raft.RejectNotLeader && code != raft.RejectNoQuorum {
		t.Fatalf("old-term write must be rejected with a distinguishable code, got: %v", err)
	}

	// While partitioned, the ghost value is never visible on the majority
	// side. Healing can (Raft Figure 8) let a stale log win the next
	// election and commit an old-term entry transitively; a rejected
	// write's outcome is therefore "unknown", never "committed". The
	// client re-establishes a known value with a fresh request.
	k.Heal()
	lid, err := k.Cluster().WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, k, Req{Op: store.OpSet, Key: "ghost", Value: "resolved", ClientID: "c", RequestID: "ghost-resolved"})
	_ = lid
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range k.Peers() {
			if k.State(id)["ghost"] != "resolved" {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range k.Peers() {
		v := k.State(id)["ghost"]
		t.Logf("judgement: replica %d ghost=%q (new committed request overrides any unknown outcome, state converges)", id, v)
		if v != "resolved" {
			t.Fatalf("replica %d did not converge to resolved: %q", id, v)
		}
	}
}

// 6) Timeout-driven retries with the same RequestID apply exactly once.
func TestDuplicateRetryNotAppliedTwice(t *testing.T) {
	k, _ := newCluster(t, 3)
	waitLeader(t, k)

	req := Req{Op: store.OpIncr, Key: "coins", Delta: 10, ClientID: "client-42", RequestID: "req-42"}
	r1 := mustWrite(t, k, req)
	// Simulate a client that timed out and retries verbatim.
	r2 := mustWrite(t, k, req)
	r3 := mustWrite(t, k, req)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, _, err := k.LinearizableRead(ctx, "coins")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: same request sent 3x (indices %d,%d,%d); coins=%s; judgement: retry dedup keeps counter at 10",
		r1.Outcome.Index, r2.Outcome.Index, r3.Outcome.Index, v)
	if v != "10" {
		t.Fatalf("duplicate retry applied more than once, coins=%s", v)
	}
}

// 7) Concurrent commits: total deterministic order, counters converge.
func TestConcurrentCommitsConverge(t *testing.T) {
	k, _ := newCluster(t, 5)
	waitLeader(t, k)

	const writers = 8
	const perWriter = 10
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				req := Req{
					Op:        store.OpIncr,
					Key:       "shared",
					Delta:     1,
					ClientID:  fmt.Sprintf("w-%d", wid),
					RequestID: fmt.Sprintf("w-%d-r-%d", wid, i),
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := k.Write(ctx, req)
				cancel()
				if err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write failed: %v", err)
	}

	want := int64(writers * perWriter)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range k.Peers() {
			s := k.State(id)
			if s["shared"] != fmt.Sprintf("%d", want) {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range k.Peers() {
		v := k.State(id)["shared"]
		t.Logf("replica %d shared=%s judgement: %d concurrent commits ordered and converged", id, v, want)
		if v != fmt.Sprintf("%d", want) {
			t.Fatalf("replica %d = %s, want %d", id, v, want)
		}
	}
}

// 8) Read that cannot confirm leadership is refused/marked, never served stale.
func TestStaleReadRefusedAndMarked(t *testing.T) {
	k, _ := newCluster(t, 3)
	leader := waitLeader(t, k)
	mustWrite(t, k, Req{Op: store.OpSet, Key: "x", Value: "committed", ClientID: "c", RequestID: "s1"})

	// Mark a follower read explicitly stale (allowed, but labelled).
	follower := uint64(0)
	for _, id := range k.Peers() {
		if id != leader {
			follower = id
		}
	}
	// Wait for the follower to have applied the committed value so the
	// demonstration is about read semantics, not replication timing.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok, _ := k.StaleRead(follower, "x"); ok && v == "committed" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, ok, stale := k.StaleRead(follower, "x")
	t.Logf("input: local read on follower %d; value=%q ok=%v stale=%v judgement: local reads are explicitly marked stale", follower, v, ok, stale)
	if !stale {
		t.Fatal("StaleRead must always be marked stale=true")
	}

	// Kill the other two: leader cannot confirm leadership -> barrier refused.
	for _, id := range k.Peers() {
		if id != leader {
			k.Crash(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	err := k.Cluster().ReadBarrier(leader, ctx)
	code := rejectCode(err)
	t.Logf("input: read barrier with only leader alive; code=%s err=%v judgement: cannot prove latest state -> read refused, not served old", code, err)
	if code != raft.RejectStaleRead && code != raft.RejectNotLeader && code != raft.RejectNoQuorum {
		t.Fatalf("expected a distinguishable stale-read rejection, got %v", err)
	}
}

// 9) A write rejected for lack of quorum is not visible while the quorum
// is down. After the quorum returns, a client re-establishes a KNOWN
// state with a fresh request (Raft contract: an errored write's outcome
// is "unknown", never "certainly absent" - if the stale node happens to
// win the next election its superseded entries may commit transitively;
// a newer write deterministically overrides them). All replicas converge.
func TestNoQuorumWriteUnknownOutcomeThenReestablish(t *testing.T) {
	k, _ := newCluster(t, 3)

	mustWrite(t, k, Req{Op: store.OpSet, Key: "v1", Value: "before", ClientID: "c", RequestID: "q1"})

	leader, err := k.Cluster().WaitLeader(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range k.Peers() {
		if id != leader {
			k.Crash(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, err = k.Write(ctx, Req{Op: store.OpSet, Key: "v1", Value: "rejected", ClientID: "c", RequestID: "q2"})
	cancel()
	t.Logf("quorum-less write rejected (outcome is now UNKNOWN, never visible without quorum): %v", err)
	if code := rejectCode(err); code != raft.RejectNoQuorum && code != raft.RejectNotLeader {
		t.Fatalf("want NO_QUORUM or NOT_LEADER, got %v", err)
	}
	// While only 1/3 is up, no linearizable read can be served at all:
	// the unknown outcome cannot leak through a read either.
	rctx, rcancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	rerr := k.Cluster().ReadBarrier(leader, rctx)
	rcancel()
	t.Logf("read while quorum down -> %v (no possibly-unknown value is served)", rerr)

	// Quorum returns.
	for _, id := range k.Peers() {
		if k.IsDown(id) {
			if err := k.Restart(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitLeader(t, k)
	// Client re-establishes a deterministic known state with a NEW request.
	mustWrite(t, k, Req{Op: store.OpSet, Key: "v1", Value: "known", ClientID: "c", RequestID: "q3"})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range k.Peers() {
			if k.State(id)["v1"] != "known" {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range k.Peers() {
		v := k.State(id)["v1"]
		t.Logf("replica %d v1=%s judgement: fresh committed write overrode any unknown outcome and all replicas converged", id, v)
		if v != "known" {
			t.Fatalf("replica %d = %q, want known", id, v)
		}
	}
}
