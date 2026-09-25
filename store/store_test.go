package store

import "testing"

func TestSetGetDelete(t *testing.T) {
	s := New()
	if _, err := s.Apply(1, Command{Op: OpSet, Key: "k1", Value: "v1"}); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Get("k1"); !ok || v != "v1" {
		t.Fatalf("Get(k1) = %q,%v want v1,true", v, ok)
	}
	if _, err := s.Apply(2, Command{Op: OpDelete, Key: "k1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("k1"); ok {
		t.Fatal("k1 must be gone after Delete")
	}
	t.Logf("input=Set k1=v1 then Delete k1; judgement=Get reports absent after delete, lastApplied=%d", s.LastApplied())
}

func TestReplayDoesNotInflateCounters(t *testing.T) {
	s := New()
	// First pass: simulate applying entries 1..3.
	for i, d := range []int64{1, 1, 5} {
		if _, err := s.Apply(uint64(i+1), Command{Op: OpIncr, Key: "hits", Delta: d}); err != nil {
			t.Fatal(err)
		}
	}
	if c, ok := s.Get("hits"); !ok || c != "7" {
		t.Fatalf("hits = %q want 7", c)
	}
	// Simulate recovery replay of the same already-applied indices.
	for i, d := range []int64{1, 1, 5} {
		r, err := s.Apply(uint64(i+1), Command{Op: OpIncr, Key: "hits", Delta: d})
		if err != nil {
			t.Fatal(err)
		}
		if !r.Duplicate {
			t.Fatalf("replay of index %d must be marked Duplicate", i+1)
		}
	}
	c, _ := s.Get("hits")
	t.Logf("input=apply Incr(1,1,5) twice at same indices; value=%s judgement=replayed committed records must not change state or inflate counters", c)
	if c != "7" {
		t.Fatalf("counter inflated by replay: %s", c)
	}
}

func TestClientRetryIdempotent(t *testing.T) {
	s := New()
	cmd := Command{Op: OpIncr, Key: "balance", Delta: 10, ClientID: "client-7", RequestID: "req-9"}
	r1, err := s.Apply(1, cmd)
	if err != nil {
		t.Fatal(err)
	}
	// Timeout-driven retry arrives as a new entry (index 2) with the same RequestID.
	r2, err := s.Apply(2, cmd)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := s.Get("balance")
	t.Logf("input=same ClientID/RequestID Incr +10 at indices 1 and 2; r1.counter=%d r2.counter=%d dup=%v value=%s; judgement=retry must not be applied twice",
		r1.Counter, r2.Counter, r2.Duplicate, c)
	if c != "10" {
		t.Fatalf("retry applied twice, balance=%s", c)
	}
	if !r2.Duplicate || r2.Counter != 10 {
		t.Fatalf("retry must return the original result, got %+v", r2)
	}
}

func TestReplayOldIndexIsDuplicate(t *testing.T) {
	s := New()
	if _, err := s.Apply(2, Command{Op: OpSet, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	// An older index arriving later (recovery replay) must be a no-op
	// rather than re-executing; the newer value stays in place.
	r, err := s.Apply(1, Command{Op: OpSet, Key: "a", Value: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Duplicate {
		t.Fatal("replay of an already-passed index must be Duplicate")
	}
	v, _ := s.Get("a")
	t.Logf("input=apply index 2 then replay index 1; value=%s judgement=replay never moves state backwards", v)
	if v != "1" {
		t.Fatalf("stale replay moved state backwards: %s", v)
	}
}

func TestCloneConverges(t *testing.T) {
	s := New()
	_, _ = s.Apply(1, Command{Op: OpSet, Key: "name", Value: "xj"})
	_, _ = s.Apply(2, Command{Op: OpIncr, Key: "n", Delta: 2})
	c := s.Clone()
	if c["name"] != "xj" || c["n"] != "2" {
		t.Fatalf("clone wrong: %v", c)
	}
	t.Logf("input=Set name=xj, Incr n+2; clone=%v judgement=full committed state is recoverable", c)
}
