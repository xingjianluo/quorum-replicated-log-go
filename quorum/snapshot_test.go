package quorum

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// proposeN 连续提交 n 条 set 写入（客户端 id 前缀 + 递增 Seq）。
func proposeN(t *testing.T, leader *Replica, client string, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		cmd := Command{Op: OpSet, Key: fmt.Sprintf("%s-k%d", client, i),
			Value: fmt.Sprintf("v%d", i), ClientID: client, Seq: uint64(i)}
		if _, err := leader.Propose(cmd); err != nil {
			t.Fatalf("propose %d failed: %v", i, err)
		}
	}
}

func walSize(t *testing.T, c *Cluster, id string) int64 {
	t.Helper()
	fi, err := os.Stat(filepath.Join(c.DataDir(id), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// 快照后崩溃重启：已提交数据完整恢复，日志被清理，重复重启状态不变。
func TestSnapshotCrashRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 10)

	snapIdx, err := leader.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := leader.KVSnapshot()
	committed := leader.CommitIndex()
	t.Logf("输入: 提交 10 条写入后在索引 %d 做快照，随后领导者崩溃重启", snapIdx)
	t.Logf("判定依据: 快照边界=%d 应等于提交索引=%d，且 WAL 中 <= %d 的记录已清理",
		snapIdx, committed, snapIdx)
	if snapIdx != committed {
		t.Fatalf("snapshot boundary %d != commitIndex %d", snapIdx, committed)
	}
	for _, e := range leader.LogEntries() {
		if e.Index <= snapIdx {
			t.Fatalf("compacted entry %d still in WAL", e.Index)
		}
	}

	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	got := c.Replica("r1").KVSnapshot()
	t.Logf("判定依据: 重启后已提交数据必须完整恢复，数据=%v", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch:\n got %v\nwant %v", got, want)
	}
	if ci := c.Replica("r1").CommitIndex(); ci != committed {
		t.Fatalf("commitIndex not recovered: got %d want %d", ci, committed)
	}

	// 再次崩溃重启：快照+日志的重复恢复不得改变状态。
	c.Crash("r1")
	c.Restart("r1")
	got2 := c.Replica("r1").KVSnapshot()
	t.Logf("判定依据: 二次重启重复恢复快照与日志，状态不得改变，数据=%v", got2)
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("repeated recovery changed state:\n got %v\nwant %v", got2, want)
	}
}

// 快照落盘与日志清理之间崩溃：重启后 WAL 残留前缀必须被跳过，
// 已提交数据不丢、不重放。
func TestSnapshotInterruptedBeforeDiscard(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 6)
	want := leader.KVSnapshot()

	// 模拟“快照已落盘、日志尚未清理”时崩溃：直接保存快照但不清理 WAL。
	r := c.Replica("r1")
	data, dedup, applied := r.kv.snapshotState()
	e, _ := r.w.at(applied)
	if err := r.snap.save(snapshotData{LastIndex: applied, LastTerm: e.Term, Data: data, Dedup: dedup}); err != nil {
		t.Fatal(err)
	}
	walBefore := walSize(t, c, "r1")
	t.Logf("输入: 快照(边界=%d)落盘后、日志清理前崩溃（WAL 仍含全部记录，大小=%d 字节）", applied, walBefore)

	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	got := c.Replica("r1").KVSnapshot()
	snapIdx, _ := c.Replica("r1").SnapshotBoundary()
	t.Logf("判定依据: 恢复后快照边界=%d，WAL 中 <= %d 的残留记录被跳过，状态=%v 必须与崩溃前一致", snapIdx, applied, got)
	if snapIdx != applied {
		t.Fatalf("snapshot boundary = %d, want %d", snapIdx, applied)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state changed by interrupted snapshot:\n got %v\nwant %v", got, want)
	}
	// 重复恢复不膨胀：已应用索引不得越过提交索引。
	if ai, ci := c.Replica("r1").kv.appliedIndex(), c.Replica("r1").CommitIndex(); ai != ci {
		t.Fatalf("appliedIndex=%d != commitIndex=%d (replay inflation)", ai, ci)
	}
}

// 历史被清理后，长期离线的副本通过快照传输追赶并收敛。
func TestLaggingFollowerCatchUpViaSnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 3)

	// r3 长期离线，期间领导者继续提交并做快照清理历史。
	c.Isolate("r3")
	proposeN(t, leader, "c", 4, 9)
	snapIdx, err := leader.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: r3 离线期间领导者提交到索引 %d 并快照清理历史，随后 r3 恢复通信", leader.CommitIndex())
	t.Logf("判定依据: r3 落后的起点(3) < 快照边界(%d)，日志补齐已不可能，必须经快照追平", snapIdx)

	c.Heal("r3")
	// 触发一次新写入，带动 r3 的追赶（快照 + 增量）。
	proposeN(t, leader, "c", 13, 1)

	got := c.Replica("r3").KVSnapshot()
	want := leader.KVSnapshot()
	r3Snap, _ := c.Replica("r3").SnapshotBoundary()
	t.Logf("判定依据: r3 快照边界推进到 %d，状态收敛为 %v", r3Snap, want)
	if r3Snap < snapIdx {
		t.Fatalf("r3 snapshot boundary = %d, want >= %d (snapshot not installed)", r3Snap, snapIdx)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lagging replica did not converge:\n got %v\nwant %v", got, want)
	}
	// 落后副本不得对外提供已清理的旧值：读必须走领导者且是最新值。
	if _, err := c.Replica("r3").Read("c-k1"); err == nil {
		t.Fatal("follower read should be rejected, not served from stale state")
	}
	if v := mustRead(t, leader, "c-k1"); v != "v1" {
		t.Fatalf("leader read = %q, want v1", v)
	}
}

// 快照与新写入的衔接处：同一段数据被反复恢复、反复发送，状态不变样。
func TestSnapshotBoundaryDuplicateRecovery(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 5)

	f := c.Replica("r3")
	if _, err := f.Snapshot(); err != nil {
		t.Fatal(err)
	}
	snapIdx, snapTerm := f.SnapshotBoundary()
	want := f.KVSnapshot()
	t.Logf("输入: r3 在索引 %d 做快照后，反复收到覆盖快照边界的复制请求与重复快照", snapIdx)

	// 领导者再提交 2 条，构造跨越快照边界的 AppendEntries。
	proposeN(t, leader, "c", 6, 2)
	entries := leader.LogEntries() // 领导者未快照，持有全部记录
	// 同一段跨界数据反复发送 3 次。
	for round := 1; round <= 3; round++ {
		resp := f.HandleAppendEntries(AppendEntriesRequest{
			Term: leader.Term(), LeaderID: "r1",
			PrevIndex: 0, PrevTerm: 0,
			Entries: entries, LeaderCommit: leader.CommitIndex(),
		})
		if !resp.Success {
			t.Fatalf("round %d: append rejected: %v", round, resp.Err)
		}
	}
	// 重复安装同一快照两次（追赶中快照被反复发送）。
	sd := snapshotData{LastIndex: snapIdx, LastTerm: snapTerm, Data: want, Dedup: map[string]DedupRecord{}}
	for round := 1; round <= 2; round++ {
		resp := f.HandleInstallSnapshot(InstallSnapshotRequest{
			Term: leader.Term(), LeaderID: "r1",
			LastIndex: sd.LastIndex, LastTerm: sd.LastTerm,
			Data: sd.Data, Dedup: sd.Dedup,
		})
		if resp.Err != nil {
			t.Fatalf("round %d: snapshot install failed: %v", round, resp.Err)
		}
	}
	// 崩溃重启，把衔接处再恢复一遍。
	c.Crash("r3")
	if err := c.Restart("r3"); err != nil {
		t.Fatal(err)
	}

	got := c.Replica("r3").KVSnapshot()
	wantFinal := leader.KVSnapshot()
	ai := c.Replica("r3").kv.appliedIndex()
	ci := c.Replica("r3").CommitIndex()
	t.Logf("判定依据: 状态=%v 与领导者一致；appliedIndex=%d 等于 commitIndex=%d，无重复应用膨胀", got, ai, ci)
	if !reflect.DeepEqual(got, wantFinal) {
		t.Fatalf("state diverged after duplicate recovery:\n got %v\nwant %v", got, wantFinal)
	}
	if ai != ci {
		t.Fatalf("appliedIndex=%d != commitIndex=%d (inflation at snapshot boundary)", ai, ci)
	}
}

// 客户端超时重试：跨快照、跨重启后同一请求不得被重复应用。
func TestClientRetryAcrossSnapshotAndRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	cmd := Command{Op: OpSet, Key: "n", Value: "first", ClientID: "cli", Seq: 7}
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Propose(Command{Op: OpSet, Key: "n", Value: "second", ClientID: "other", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: (cli,7) set n=first；(other,1) set n=second；随后快照、领导者崩溃重启、重新当选")

	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	leader, err := c.Elect("r1")
	if err != nil {
		t.Fatal(err)
	}
	// 客户端因超时重试同一请求（相同 ClientID+Seq），跨过快照与重启。
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	v := mustRead(t, leader, "n")
	t.Logf("判定依据: 去重表随快照持久化，重试不得重复应用，n 应保持 second，实际=%q", v)
	if v != "second" {
		t.Fatalf("retry reapplied across snapshot+restart: n=%q, want second", v)
	}
}

// 长时间持续写入 + 周期性快照：磁盘占用趋于稳定而非无限增长。
func TestSnapshotDiskUsageStable(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")

	var firstSize, lastSize int64
	const rounds = 10
	const perRound = 30
	for round := 1; round <= rounds; round++ {
		proposeN(t, leader, "c", (round-1)*perRound+1, perRound)
		// 快照前测量：稳态下 WAL 只含“距上次快照新写入的部分”，大小应恒定。
		size := walSize(t, c, "r1")
		if round == 1 {
			firstSize = size
		}
		lastSize = size
		if _, err := leader.Snapshot(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("输入: 连续提交 %d 条写入，每 %d 条做一次快照", rounds*perRound, perRound)
	t.Logf("判定依据: 第 1 轮快照前 WAL=%d 字节，第 %d 轮快照前 WAL=%d 字节，应同量级而非随写入线性增长",
		firstSize, rounds, lastSize)
	// 每轮写入量相同，稳态下快照间隔内积累的 WAL 应大致恒定。
	// 允许 4 倍裕量吸收编码长度差异（索引/值变长）。
	if lastSize > firstSize*4 {
		t.Fatalf("WAL keeps growing: first round %d bytes, last round %d bytes", firstSize, lastSize)
	}

	// 崩溃重启后全部已提交数据仍可恢复。
	want := leader.KVSnapshot()
	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	got := c.Replica("r1").KVSnapshot()
	t.Logf("判定依据: 重启后 %d 个键全部恢复", len(want))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state lost after sustained writes + snapshots:\n got %d keys\nwant %d keys", len(got), len(want))
	}
}
