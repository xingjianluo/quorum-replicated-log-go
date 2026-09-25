// kvdemo 在单进程内启动 3 副本集群，演示多数派提交、
// 崩溃恢复、多数派之外写入被拒绝与最终收敛。
package main

import (
	"fmt"
	"log"
	"os"

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
}
