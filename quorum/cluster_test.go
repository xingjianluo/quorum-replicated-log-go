package quorum

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestCrashRestartRecovery(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	for i := 1; i <= 5; i++ {
		if _, err := leader.Propose(Command{Op: OpSet, Key: fmt.Sprintf("k%d", i),
			Value: fmt.Sprintf("v%d", i), ClientID: "c", Seq: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	want := leader.KVSnapshot()
	committed := leader.CommitIndex()

	// 领导者崩溃并重启：已提交数据必须完整恢复。
	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		t.Fatal(err)
	}
	got := c.Replica("r1").KVSnapshot()
	t.Logf("输入: 提交 5 条写入后崩溃并重启领导者")
	t.Logf("判定依据: 已提交数据必须完整恢复，commitIdx=%d，恢复后数据=%v", committed, got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch:\n got %v\nwant %v", got, want)
	}
	if c.Replica("r1").CommitIndex() != committed {
		t.Fatalf("commitIndex not recovered: got %d want %d", c.Replica("r1").CommitIndex(), committed)
	}

	// 再次重启：重复回放已提交记录不得改变状态。
	c.Crash("r1")
	c.Restart("r1")
	got2 := c.Replica("r1").KVSnapshot()
	t.Logf("判定依据: 二次重启重复回放，状态不得改变，数据=%v", got2)
	if !reflect.DeepEqual(got2, want) {
		t.Fatalf("replay changed state:\n got %v\nwant %v", got2, want)
	}
}

func TestUncommittedTailNotAppliedAfterCrash(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	// 隔离跟随者，使写入只能本地持久化、无法提交。
	c.Isolate("r2")
	c.Isolate("r3")
	_, err := leader.Propose(Command{Op: OpSet, Key: "x", Value: "uncommitted", ClientID: "c", Seq: 1})
	t.Logf("输入: 无多数派时写入（应失败），随后领导者崩溃重启")
	t.Logf("判定依据: 未提交记录不得被应用，err=%v", err)
	if err == nil {
		t.Fatal("write without quorum unexpectedly succeeded")
	}
	c.Crash("r1")
	c.Restart("r1")
	if v := c.Replica("r1").KVSnapshot()["x"]; v != "" {
		t.Fatalf("uncommitted entry applied after restart: %q", v)
	}
}

func TestTornTailEndToEnd(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	leader.Propose(Command{Op: OpSet, Key: "a", Value: "1", ClientID: "c", Seq: 1})

	// 跟随者崩溃，WAL 尾部被写入半条记录（模拟崩溃时正在写盘）。
	c.Crash("r3")
	walPath := filepath.Join(c.DataDir("r3"), "wal.log")
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0x30, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03})
	f.Close()

	if err := c.Restart("r3"); err != nil {
		t.Fatal(err)
	}
	r3 := c.Replica("r3")
	t.Logf("输入: 跟随者 r3 崩溃且 WAL 尾部有 7 字节残缺数据，重启后继续复制")
	t.Logf("判定依据: 残缺尾部被截断，已提交数据完整，r3 日志条数=%d", len(r3.LogEntries()))
	if v := r3.KVSnapshot()["a"]; v != "1" {
		t.Fatalf("committed data lost after torn tail recovery: %q", v)
	}
	// 重启后应能继续接收复制并收敛。
	leader.Propose(Command{Op: OpSet, Key: "b", Value: "2", ClientID: "c", Seq: 2})
	if v := r3.KVSnapshot()["b"]; v != "2" {
		t.Fatalf("restarted replica failed to catch up: %q", v)
	}
}

func TestConcurrentCommitsDeterministic(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")

	const writers = 8
	const perWriter = 5
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				cmd := Command{Op: OpSet, Key: fmt.Sprintf("w%d-k%d", w, i),
					Value: fmt.Sprintf("v%d-%d", w, i),
					ClientID: fmt.Sprintf("cli-%d", w), Seq: uint64(i + 1)}
				if _, err := leader.Propose(cmd); err != nil {
					t.Errorf("concurrent propose failed: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()

	t.Logf("输入: %d 个客户端并发提交，每个 %d 条写入", writers, perWriter)
	// 各副本日志必须完全一致（确定的提交顺序）。
	ref := c.Replica("r1").LogEntries()
	for _, id := range []string{"r2", "r3"} {
		got := c.Replica(id).LogEntries()
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("replica %s log diverged:\n got %v\nwant %v", id, got, ref)
		}
	}
	t.Logf("判定依据: 并发提交产生确定顺序，三副本日志完全一致（%d 条）", len(ref))
	// 状态机收敛。
	want := leader.KVSnapshot()
	if len(want) != writers*perWriter {
		t.Fatalf("state machine lost writes: got %d keys, want %d", len(want), writers*perWriter)
	}
	for _, id := range []string{"r2", "r3"} {
		if got := c.Replica(id).KVSnapshot(); !reflect.DeepEqual(got, want) {
			t.Fatalf("replica %s state diverged", id)
		}
	}
	t.Logf("判定依据: 各副本最终状态一致，键数=%d", len(want))
}

func TestConvergenceAfterPartitionHeal(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.Elect("r1")
	leader.Propose(Command{Op: OpSet, Key: "p", Value: "1", ClientID: "c", Seq: 1})

	// r3 分区期间领导者继续提交。
	c.Isolate("r3")
	leader.Propose(Command{Op: OpSet, Key: "p", Value: "2", ClientID: "c", Seq: 2})
	leader.Propose(Command{Op: OpSet, Key: "q", Value: "9", ClientID: "c", Seq: 3})
	if v := c.Replica("r3").KVSnapshot()["p"]; v != "1" {
		t.Fatalf("partitioned replica should keep old value, got %q", v)
	}

	// 分区恢复后，r3 应通过后续复制补齐缺失日志并收敛。
	c.Heal("r3")
	leader.Propose(Command{Op: OpSet, Key: "r", Value: "0", ClientID: "c", Seq: 4})
	got := c.Replica("r3").KVSnapshot()
	want := leader.KVSnapshot()
	t.Logf("输入: r3 分区期间错过 2 条提交，恢复后领导者再提交 1 条")
	t.Logf("判定依据: r3 最终收敛到 %v", want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replica did not converge after heal:\n got %v\nwant %v", got, want)
	}
}

func TestKVReplayIdempotent(t *testing.T) {
	kv := newKVStore()
	e := Entry{Term: 1, Index: 1, Command: Command{Op: OpSet, Key: "k", Value: "v", ClientID: "c", Seq: 1}}
	if _, applied := kv.apply(1, e); !applied {
		t.Fatal("first apply should take effect")
	}
	// 重复回放同一条记录：状态与结果不变。
	if _, applied := kv.apply(1, e); applied {
		t.Fatal("replay of committed entry must be a no-op")
	}
	t.Logf("输入: 同一已提交记录回放两次")
	t.Logf("判定依据: 第二次回放被跳过，appliedIndex=%d，数据=%v", kv.appliedIndex(), kv.snapshot())
	if kv.appliedIndex() != 1 {
		t.Fatalf("appliedIndex inflated by replay: %d", kv.appliedIndex())
	}
	if v, _ := kv.get("k"); v != "v" {
		t.Fatalf("state changed by replay: %q", v)
	}
}
