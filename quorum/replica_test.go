package quorum

import (
	"errors"
	"testing"
)

func newTestCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	ids := []string{"r1", "r2", "r3", "r4", "r5"}[:n]
	c, err := NewCluster(Config{ReplicaIDs: ids, Dir: t.TempDir(), Fsync: true})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMajorityCommitVisible(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, err := c.Elect("r1")
	if err != nil {
		t.Fatal(err)
	}
	cmd := Command{Op: OpSet, Key: "foo", Value: "bar", ClientID: "cli", Seq: 1}
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	t.Logf("输入: 3 副本集群，领导者 r1 提交 set foo=bar")
	t.Logf("判定依据: 多数派确认后写入应对外可见，Read(foo)=%q", mustRead(t, leader, "foo"))
	if v := mustRead(t, leader, "foo"); v != "bar" {
		t.Fatalf("read = %q, want bar", v)
	}
	// 提交索引应通知到跟随者。
	for _, id := range []string{"r2", "r3"} {
		f := c.Replica(id)
		if f.CommitIndex() < leader.CommitIndex() {
			t.Fatalf("follower %s commitIndex=%d < leader %d", id, f.CommitIndex(), leader.CommitIndex())
		}
		if v, ok := f.KVSnapshot()["foo"]; !ok || v != "bar" {
			t.Fatalf("follower %s did not apply committed entry: %v", id, f.KVSnapshot())
		}
	}
}

func TestProposeToFollowerRejected(t *testing.T) {
	c := newTestCluster(t, 3)
	if _, err := c.Elect("r1"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Replica("r2").Propose(Command{Op: OpSet, Key: "k", Value: "v", ClientID: "c", Seq: 1})
	t.Logf("输入: 客户端向跟随者 r2 发起写入")
	t.Logf("判定依据: 应返回 ErrNotLeader，实际 err=%v", err)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("want ErrNotLeader, got %v", err)
	}
}

func TestWriteOutsideMajorityRejected(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, err := c.Elect("r1")
	if err != nil {
		t.Fatal(err)
	}
	// 隔离两个跟随者：领导者只剩自己，凑不齐 2/3 多数派。
	c.Isolate("r2")
	c.Isolate("r3")
	_, err = leader.Propose(Command{Op: OpSet, Key: "k", Value: "v", ClientID: "c", Seq: 1})
	t.Logf("输入: 隔离 r2、r3 后，领导者 r1 尝试写入（可达副本 1/3）")
	t.Logf("判定依据: 多数派之外的写入必须被拒绝，应返回 ErrNoQuorum，实际 err=%v", err)
	if !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("want ErrNoQuorum, got %v", err)
	}
	if v := leader.KVSnapshot()["k"]; v != "" {
		t.Fatalf("rejected write must not be applied, got %q", v)
	}
}

func TestStaleLeaderWriteInvalidated(t *testing.T) {
	c := newTestCluster(t, 3)
	old, err := c.Elect("r1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Propose(Command{Op: OpSet, Key: "k", Value: "v1", ClientID: "c", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	// 旧领导者被分区，新领导者在多数派中选出。
	c.Isolate("r1")
	newLeader, err := c.Elect("r2")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: r1 分区后 r2 以更高任期 %d 当选（旧任期 %d）", newLeader.Term(), old.Term())

	// 旧领导者的写入：无法获得多数派，必须被拒绝。
	_, err = old.Propose(Command{Op: OpSet, Key: "k", Value: "stale", ClientID: "c", Seq: 2})
	t.Logf("判定依据: 旧领导者写入应被拒绝（ErrNoQuorum 或 ErrNotLeader），实际 err=%v", err)
	if !errors.Is(err, ErrNoQuorum) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale leader write accepted: %v", err)
	}

	// 分区恢复后，旧领导者向新任期跟随者复制应收到 ErrStaleTerm 并下台。
	c.Heal("r1")
	resp := c.Replica("r3").HandleAppendEntries(AppendEntriesRequest{
		Term: old.Term(), LeaderID: "r1", Entries: []Entry{{Term: old.Term(), Index: 99}},
	})
	t.Logf("判定依据: 旧领导者(任期 %d)的复制请求应被 ErrStaleTerm 拒绝，实际 resp.Err=%v", old.Term(), resp.Err)
	if !errors.Is(resp.Err, ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", resp.Err)
	}

	// 新领导者写入生效，旧值不被覆盖。
	if _, err := newLeader.Propose(Command{Op: OpSet, Key: "k", Value: "v2", ClientID: "c", Seq: 3}); err != nil {
		t.Fatal(err)
	}
	if v := mustRead(t, newLeader, "k"); v != "v2" {
		t.Fatalf("read = %q, want v2", v)
	}
}

func TestReadConsistency(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	leader.Propose(Command{Op: OpSet, Key: "a", Value: "1", ClientID: "c", Seq: 1})

	// 跟随者读：拒绝。
	if _, err := c.Replica("r2").Read("a"); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("follower read: want ErrNotLeader, got %v", err)
	}
	t.Logf("输入: 向跟随者 r2 读取已提交键 a")
	t.Logf("判定依据: 跟随者无法保证最新，应返回 ErrNotLeader")

	// 领导者读：返回已提交的最新值。
	leader.Propose(Command{Op: OpSet, Key: "a", Value: "2", ClientID: "c", Seq: 2})
	if v := mustRead(t, leader, "a"); v != "2" {
		t.Fatalf("read = %q, want 2 (committed reads must not return stale values)", v)
	}
	t.Logf("判定依据: 已提交数据的读取不得返回旧值，Read(a)=%q", "2")
}

func TestDuplicateRetryNotReapplied(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	cmd := Command{Op: OpSet, Key: "n", Value: "first", ClientID: "cli", Seq: 7}
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	// 其他客户端把键改掉。
	if _, err := leader.Propose(Command{Op: OpSet, Key: "n", Value: "second", ClientID: "other", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	// 客户端因超时重试同一请求（相同 ClientID+Seq）。
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	v := mustRead(t, leader, "n")
	t.Logf("输入: (cli,7) 提交 set n=first；(other,1) 提交 set n=second；随后重试 (cli,7)")
	t.Logf("判定依据: 重试不得重复应用，n 应保持 second，实际=%q", v)
	if v != "second" {
		t.Fatalf("duplicate retry reapplied: n=%q, want second", v)
	}
}

func mustRead(t *testing.T, r *Replica, key string) string {
	t.Helper()
	v, err := r.Read(key)
	if err != nil {
		t.Fatalf("read %q failed: %v", key, err)
	}
	return v
}
