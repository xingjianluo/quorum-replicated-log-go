package quorum

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// newSnapshotCluster 创建开启自动快照阈值的测试集群。
func newSnapshotCluster(t *testing.T, n int, threshold int) *Cluster {
	t.Helper()
	ids := []string{"r1", "r2", "r3", "r4", "r5"}[:n]
	c, err := NewCluster(Config{ReplicaIDs: ids, Dir: t.TempDir(), Fsync: true, SnapshotThreshold: threshold})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

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

// 快照之后的崩溃重启：已提交数据与提交索引完整恢复，日志已被压缩。
func TestSnapshotThenCrashRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 10)
	want := leader.KVSnapshot()
	committed := leader.CommitIndex()
	logBefore := len(leader.LogEntries())

	if err := leader.Snapshot(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	logAfter := len(leader.LogEntries())
	t.Logf("输入: 提交 10 条写入后手动快照，随后领导者崩溃重启")
	t.Logf("判定依据: 快照压缩日志（%d 条 -> %d 条），重启后已提交数据完整", logBefore, logAfter)
	if logAfter >= logBefore {
		t.Fatalf("log not compacted: before %d after %d", logBefore, logAfter)
	}

	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	r1 := c.Replica("r1")
	got := r1.KVSnapshot()
	t.Logf("判定依据: 重启后状态=%v，commitIdx=%d（期望 %d）", got, r1.CommitIndex(), committed)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state lost after snapshot+restart:\n got %v\nwant %v", got, want)
	}
	if r1.CommitIndex() != committed {
		t.Fatalf("commitIndex not recovered: got %d want %d", r1.CommitIndex(), committed)
	}

	// 快照点之后继续写入：新旧日志无缝衔接，索引单调递增。
	if _, err := c.Elect("r1"); err != nil {
		t.Fatal(err)
	}
	proposeN(t, c.Replica("r1"), "c", 11, 3)
	for _, id := range []string{"r2", "r3"} {
		if got := c.Replica(id).KVSnapshot(); !reflect.DeepEqual(got, c.Replica("r1").KVSnapshot()) {
			t.Fatalf("replica %s diverged after snapshot boundary writes", id)
		}
	}
	t.Logf("判定依据: 快照边界之后再写 3 条，三副本状态一致=%v", c.Replica("r1").KVSnapshot())
}

// 快照落盘与日志压缩之间崩溃：恢复时按快照基线过滤 WAL，重复恢复幂等。
func TestSnapshotCrashBetweenSaveAndCompact(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 8)
	want := c.Replica("r3").KVSnapshot()
	committed := c.Replica("r3").CommitIndex()

	// 在 r3 上模拟“快照已落盘、WAL 尚未压缩”的崩溃点：
	// 只写快照文件，不压缩 WAL，然后崩溃重启。
	r3 := c.Replica("r3")
	r3.mu.Lock()
	snap := r3.kv.capture(r3.commitIdx, 1)
	if err := r3.snap.save(snap); err != nil {
		r3.mu.Unlock()
		t.Fatal(err)
	}
	r3.mu.Unlock()
	walBefore := walSize(t, c, "r3")

	c.Crash("r3")
	if err := c.Restart("r3"); err != nil {
		t.Fatal(err)
	}
	got := c.Replica("r3").KVSnapshot()
	t.Logf("输入: r3 快照已落盘但 WAL 未压缩（%d 字节）时崩溃重启", walBefore)
	t.Logf("判定依据: 恢复按快照基线过滤 WAL，状态=%v 不重复应用、不丢失", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state changed by overlapping snapshot+WAL recovery:\n got %v\nwant %v", got, want)
	}
	if c.Replica("r3").CommitIndex() != committed {
		t.Fatalf("commitIndex mismatch: got %d want %d", c.Replica("r3").CommitIndex(), committed)
	}

	// 再次崩溃重启：同一快照重复恢复，状态不变、applied 不膨胀。
	c.Crash("r3")
	c.Restart("r3")
	got2 := c.Replica("r3").KVSnapshot()
	t.Logf("判定依据: 二次重启重复恢复快照，状态不变=%v", got2)
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("repeated snapshot restore changed state:\n got %v\nwant %v", got2, want)
	}
	// 恢复后仍能跟上领导者的新写入。
	proposeN(t, leader, "c", 9, 2)
	if got := c.Replica("r3").KVSnapshot(); !reflect.DeepEqual(got, leader.KVSnapshot()) {
		t.Fatalf("r3 failed to catch up after snapshot-boundary recovery")
	}
}

// 快照动作本身失败：已提交数据与日志不受影响，重启后一切正常。
func TestSnapshotFailurePreservesData(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 6)
	want := leader.KVSnapshot()
	logBefore := len(leader.LogEntries())

	// 注入故障：snapshot.json.tmp 是目录，快照写临时文件必然失败。
	if err := os.Mkdir(filepath.Join(c.DataDir("r1"), "snapshot.json.tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := leader.Snapshot()
	t.Logf("输入: 快照写盘被注入失败（临时文件路径被目录占用）")
	t.Logf("判定依据: Snapshot 返回错误=%v，日志不压缩（仍 %d 条），数据不变", err, logBefore)
	if err == nil {
		t.Fatal("snapshot should fail when temp path is blocked")
	}
	if got := len(leader.LogEntries()); got != logBefore {
		t.Fatalf("log compacted despite snapshot failure: %d -> %d", logBefore, got)
	}
	if got := leader.KVSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed data changed by failed snapshot")
	}

	// 故障下崩溃重启：已提交数据完整恢复。
	c.Crash("r1")
	c.Restart("r1")
	if got := c.Replica("r1").KVSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("committed data lost after failed snapshot + restart")
	}
	t.Logf("判定依据: 快照失败后崩溃重启，已提交数据完整=%v", c.Replica("r1").KVSnapshot())
}

// 历史被清理后，长期离线且反复崩溃的落后副本通过快照追赶收敛。
func TestLaggingReplicaCatchUpViaSnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 5)

	// r3 离线，期间还崩溃重启了一次（模拟反复崩溃的落后副本）。
	c.Isolate("r3")
	c.Crash("r3")
	if err := c.Restart("r3"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("r3")

	// 领导者继续提交并做快照，r3 落后的那段历史被清理。
	proposeN(t, leader, "c", 6, 10)
	if err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}
	proposeN(t, leader, "c", 16, 3)
	base := leader.LogEntries()[0].Index
	t.Logf("输入: r3 离线（期间崩溃重启一次），领导者再提交 13 条并快照，日志基线推进到 %d", base)

	// 分区恢复，下一次写入触发领导者向 r3 发送快照。
	c.Heal("r3")
	proposeN(t, leader, "c", 19, 1)

	got := c.Replica("r3").KVSnapshot()
	want := leader.KVSnapshot()
	t.Logf("判定依据: r3 通过快照追赶到最新状态，键数=%d，与领导者一致", len(got))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lagging replica did not converge via snapshot:\n got %v\nwant %v", got, want)
	}
	// r3 的日志同样被压缩到快照点之后，不会无限保留旧历史。
	r3First := c.Replica("r3").LogEntries()[0].Index
	t.Logf("判定依据: r3 日志基线=%d（快照清理生效），领导者基线=%d", r3First, base)
	if r3First < base {
		t.Fatalf("r3 kept discarded history: first index %d < leader base %d", r3First, base)
	}

	// 落后副本在追赶前不能提供读取（不会返回已清理的旧值）；
	// 追赶并当选领导者后，读到的是最新值而不是旧值。
	if _, err := c.Replica("r3").Read("c-k6"); err == nil {
		t.Fatal("follower read should be rejected while lagging")
	}
	newLeader, err := c.Elect("r3")
	if err != nil {
		t.Fatal(err)
	}
	if v := mustRead(t, newLeader, "c-k6"); v != "v6" {
		t.Fatalf("read after catch-up returned stale/discarded value: %q", v)
	}
	t.Logf("判定依据: r3 追赶后当选领导者，Read(c-k6)=%q 为最新值", "v6")
}

// 同一快照被反复安装：状态不变、applied 不膨胀；跨快照的客户端重试不重复应用。
func TestRepeatedSnapshotInstallIdempotent(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	proposeN(t, leader, "c", 1, 6)
	if err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}

	// 隔离 r3 并取领导者的快照，手动重复安装两次。
	c.Isolate("r3")
	snap, err := leader.snap.load()
	if err != nil || snap == nil {
		t.Fatalf("leader snapshot missing: %v", err)
	}
	req := InstallSnapshotRequest{Term: leader.Term(), LeaderID: "r1", Snapshot: *snap, LeaderCommit: leader.CommitIndex()}
	r3 := c.Replica("r3")
	resp1 := r3.HandleInstallSnapshot(req)
	state1 := r3.KVSnapshot()
	applied1 := r3.kv.appliedIndex()
	resp2 := r3.HandleInstallSnapshot(req)
	state2 := r3.KVSnapshot()
	applied2 := r3.kv.appliedIndex()
	t.Logf("输入: 对 r3 重复安装同一快照（LastIndex=%d）两次", snap.LastIndex)
	t.Logf("判定依据: 两次都成功，状态不变，applied=%d 不膨胀", applied2)
	if !resp1.Success || !resp2.Success {
		t.Fatalf("snapshot install rejected: %v / %v", resp1.Err, resp2.Err)
	}
	if !reflect.DeepEqual(state1, state2) {
		t.Fatalf("repeated install changed state:\n first %v\nsecond %v", state1, state2)
	}
	if applied1 != applied2 || applied2 != snap.LastIndex {
		t.Fatalf("appliedIndex inflated: %d -> %d (snapshot at %d)", applied1, applied2, snap.LastIndex)
	}

	// 快照点之后的新写入与快照无缝衔接：r3 恢复通信后收敛。
	c.Heal("r3")
	proposeN(t, leader, "c", 7, 2)
	if got := r3.KVSnapshot(); !reflect.DeepEqual(got, leader.KVSnapshot()) {
		t.Fatalf("r3 diverged after snapshot install + new writes")
	}
	t.Logf("判定依据: 快照后再写 2 条，r3 与领导者收敛一致=%v", leader.KVSnapshot())
}

// 客户端超时重试跨快照、跨重启：去重表随快照持久化，重试不重复应用。
func TestClientRetryAcrossSnapshotAndRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	cmd := Command{Op: OpSet, Key: "n", Value: "first", ClientID: "cli", Seq: 7}
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	// 其他客户端把键改掉，随后快照并压缩历史。
	if _, err := leader.Propose(Command{Op: OpSet, Key: "n", Value: "second", ClientID: "other", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}

	// 全部副本崩溃重启（去重表只能从快照恢复）。
	for _, id := range []string{"r1", "r2", "r3"} {
		c.Crash(id)
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		if err := c.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	leader, err := c.Elect("r1")
	if err != nil {
		t.Fatal(err)
	}
	// 客户端因超时重试同一请求（相同 ClientID+Seq）。
	if _, err := leader.Propose(cmd); err != nil {
		t.Fatal(err)
	}
	v := mustRead(t, leader, "n")
	t.Logf("输入: (cli,7) 提交后被 (other,1) 覆盖；快照+全集群重启后重试 (cli,7)")
	t.Logf("判定依据: 去重表随快照恢复，重试不重复应用，n 应保持 second，实际=%q", v)
	if v != "second" {
		t.Fatalf("retry reapplied across snapshot+restart: n=%q, want second", v)
	}
}

// 长时间持续写入 + 自动快照：磁盘占用趋于稳定，各副本最终一致。
func TestAutoSnapshotBoundsDiskUsage(t *testing.T) {
	c := newSnapshotCluster(t, 3, 20)
	leader, _ := c.Elect("r1")

	proposeN(t, leader, "c", 1, 100)
	size100 := walSize(t, c, "r1")
	entries100 := len(leader.LogEntries())
	proposeN(t, leader, "c", 101, 100)
	size200 := walSize(t, c, "r1")
	entries200 := len(leader.LogEntries())

	t.Logf("输入: 阈值=20 的自动快照下持续写入 200 条")
	t.Logf("判定依据: WAL 条数 100 条时=%d、200 条时=%d（应远小于总量且有界）",
		entries100, entries200)
	t.Logf("判定依据: WAL 文件 100 条时=%d 字节、200 条时=%d 字节（应趋于稳定而非翻倍）",
		size100, size200)
	if entries200 > 40 {
		t.Fatalf("log keeps growing despite auto snapshot: %d entries after 200 writes", entries200)
	}
	if size200 > size100*2 {
		t.Fatalf("wal size not stable: %d -> %d bytes", size100, size200)
	}

	// 快照不丢数据：领导者状态包含全部 200 个键，跟随者收敛。
	if got := len(leader.KVSnapshot()); got != 200 {
		t.Fatalf("snapshot lost committed data: %d keys, want 200", got)
	}
	for _, id := range []string{"r2", "r3"} {
		if got := c.Replica(id).KVSnapshot(); !reflect.DeepEqual(got, leader.KVSnapshot()) {
			t.Fatalf("replica %s diverged under auto snapshot", id)
		}
	}
	t.Logf("判定依据: 200 个键全部保留，三副本状态一致")
}
