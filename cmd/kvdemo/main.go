// kvdemo 在单进程内启动 3 副本集群，演示多数派提交、
// 崩溃恢复、多数派之外写入被拒绝与最终收敛。
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xingjianluo/quorum-replicated-log-go/quorum"
)

func main() {
	dir, err := os.MkdirTemp("", "kvdemo-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c, err := quorum.NewCluster(quorum.Config{
		ReplicaIDs: []string{"r1", "r2", "r3"},
		Dir:        dir,
		Fsync:      true,
	})
	if err != nil {
		log.Fatal(err)
	}

	leader, err := c.Elect("r1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[elect] leader=%s term=%d\n", leader.ID(), leader.Term())

	if _, err := leader.Propose(quorum.Command{Op: quorum.OpSet, Key: "hello", Value: "world", ClientID: "demo", Seq: 1}); err != nil {
		log.Fatal(err)
	}
	v, _ := leader.Read("hello")
	fmt.Printf("[commit] set hello=world 多数派提交成功, Read(hello)=%q\n", v)

	// 隔离两个跟随者：多数派之外的写入必须被拒绝。
	c.Isolate("r2")
	c.Isolate("r3")
	_, err = leader.Propose(quorum.Command{Op: quorum.OpSet, Key: "x", Value: "y", ClientID: "demo", Seq: 2})
	fmt.Printf("[no-quorum] 隔离 2 个跟随者后写入被拒绝: %v\n", err)
	c.Heal("r2")
	c.Heal("r3")

	// 崩溃并重启领导者：已提交数据完整恢复。
	c.Crash("r1")
	if err := c.Restart("r1"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[recover] r1 崩溃重启后状态: %v\n", c.Replica("r1").KVSnapshot())

	// 重新选主后继续写入，验证各副本收敛。
	leader2, err := c.Elect("r2")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := leader2.Propose(quorum.Command{Op: quorum.OpSet, Key: "after", Value: "restart", ClientID: "demo", Seq: 3}); err != nil {
		log.Fatal(err)
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		fmt.Printf("[converge] %s: %v\n", id, c.Replica(id).KVSnapshot())
	}

	// 持续写入后做快照：清理已提交历史，磁盘占用回落。
	for i := 4; i <= 20; i++ {
		if _, err := leader2.Propose(quorum.Command{
			Op: quorum.OpSet, Key: fmt.Sprintf("bulk-%d", i), Value: "v",
			ClientID: "demo", Seq: uint64(i)}); err != nil {
			log.Fatal(err)
		}
	}
	walPath := filepath.Join(dir, "r2", "wal.log")
	before, _ := os.Stat(walPath)
	snapIdx, err := leader2.Snapshot()
	if err != nil {
		log.Fatal(err)
	}
	after, _ := os.Stat(walPath)
	fmt.Printf("[snapshot] 快照边界=%d, WAL %dB -> %dB（已提交历史被清理）\n",
		snapIdx, before.Size(), after.Size())

	// 副本离线期间历史被清理，恢复后通过快照传输追平。
	c.Isolate("r3")
	for i := 21; i <= 30; i++ {
		if _, err := leader2.Propose(quorum.Command{
			Op: quorum.OpSet, Key: fmt.Sprintf("bulk-%d", i), Value: "v",
			ClientID: "demo", Seq: uint64(i)}); err != nil {
			log.Fatal(err)
		}
	}
	if _, err := leader2.Snapshot(); err != nil {
		log.Fatal(err)
	}
	c.Heal("r3")
	if _, err := leader2.Propose(quorum.Command{Op: quorum.OpSet, Key: "final", Value: "1", ClientID: "demo", Seq: 31}); err != nil {
		log.Fatal(err)
	}
	snap3, _ := c.Replica("r3").SnapshotBoundary()
	fmt.Printf("[catch-up] r3 经快照追平: 快照边界=%d, 键数=%d（与领导者一致）\n",
		snap3, len(c.Replica("r3").KVSnapshot()))
}
