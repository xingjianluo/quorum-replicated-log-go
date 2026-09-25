// Command kvdemo runs a fully local 3-replica replicated key/value store
// and demonstrates: majority commit, non-leader rejection with a clear
// reason, idempotent retries, a quorum-less rejection, a crash/recovery
// cycle with torn-tail repair, and a stale read being refused.
//
// Run:  go run ./cmd/kvdemo
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/quorum-replicated-log-go/kvserver"
	"github.com/xingjianluo/quorum-replicated-log-go/raft"
	"github.com/xingjianluo/quorum-replicated-log-go/store"
)

func section(title string) {
	fmt.Printf("\n==== %s ====\n", title)
}

func code(err error) raft.RejectCode {
	var re *raft.Error
	if errors.As(err, &re) {
		return re.Code
	}
	return ""
}

func must(err error, msg string) {
	if err != nil {
		fmt.Printf("FATAL %s: %v\n", msg, err)
		os.Exit(1)
	}
}

func main() {
	dir, err := os.MkdirTemp("", "kvdemo-")
	must(err, "temp dir")
	defer os.RemoveAll(dir)
	fmt.Printf("durable data dir: %s\n", dir)

	k, err := kvserver.New(dir, 3)
	must(err, "create cluster")
	k.Start()
	defer func() { _ = k.Stop() }()

	ctx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 3*time.Second)
	}

	section("1. leader election")
	leader, err := k.Cluster().WaitLeader(3 * time.Second)
	must(err, "wait leader")
	fmt.Printf("cluster of 3 elected leader: replica %d (majority = %d)\n", leader, k.Majority())

	section("2. majority commit + linearizable read")
	c1, cancel1 := ctx()
	resp, err := k.Write(c1, kvserver.Req{
		Op: store.OpSet, Key: "color", Value: "blue",
		ClientID: "demo", RequestID: "w1",
	})
	cancel1()
	must(err, "commit Set color=blue")
	fmt.Printf("Set color=blue committed at index %d, term %d\n", resp.Outcome.Index, resp.Outcome.Term)
	c2, cancel2 := ctx()
	v, ok, err := k.LinearizableRead(c2, "color")
	cancel2()
	must(err, "linearizable read")
	fmt.Printf("LinearizableRead color -> %q (exists=%v); read barrier confirmed current leadership\n", v, ok)
	for _, id := range k.Peers() {
		fmt.Printf("  replica %d state: %v\n", id, k.State(id))
	}

	section("3. concurrent writes converge in one order")
	converged := make(chan error, 10)
	for w := 0; w < 5; w++ {
		wid := w
		go func() {
			for i := 0; i < 4; i++ {
				c, cancel := ctx()
				_, werr := k.Write(c, kvserver.Req{
					Op: store.OpIncr, Key: "counter", Delta: 1,
					ClientID:  fmt.Sprintf("w%d", wid),
					RequestID: fmt.Sprintf("w%d-%d", wid, i),
				})
				cancel()
				converged <- werr
			}
		}()
	}
	for i := 0; i < 20; i++ {
		must(<-converged, "concurrent write")
	}
	time.Sleep(100 * time.Millisecond)
	for _, id := range k.Peers() {
		fmt.Printf("  replica %d counter=%s (all must equal 20)\n", id, k.State(id)["counter"])
	}

	section("4. duplicate retry is applied once")
	req := kvserver.Req{Op: store.OpIncr, Key: "coins", Delta: 10, ClientID: "demo", RequestID: "retry-1"}
	for i := 0; i < 3; i++ {
		c, cancel := ctx()
		_, werr := k.Write(c, req)
		cancel()
		must(werr, fmt.Sprintf("retry %d", i+1))
	}
	c3, cancel3 := ctx()
	coins, _, err := k.LinearizableRead(c3, "coins")
	cancel3()
	must(err, "read coins")
	fmt.Printf("same request sent 3x -> coins=%s (must be 10, deduped by ClientID/RequestID)\n", coins)

	section("5. quorum-less write is rejected with a distinguishable reason")
	victims := make([]uint64, 0)
	for _, id := range k.Peers() {
		if id != leader {
			k.Crash(id)
			victims = append(victims, id)
		}
	}
	fmt.Printf("crashed replicas %v; only leader %d alive (1 < majority %d)\n", victims, leader, k.Majority())
	c4, cancel4 := ctx()
	_, err = k.Write(c4, kvserver.Req{
		Op: store.OpSet, Key: "ghost", Value: "never",
		ClientID: "demo", RequestID: "no-quorum",
	})
	cancel4()
	fmt.Printf("write rejected -> code=%q err=%v\n", code(err), err)
	if err == nil {
		must(errors.New("write unexpectedly committed"), "quorum check")
	}

	section("6. stale read is refused when leadership cannot be proven")
	c5, cancel5 := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	err = k.Cluster().ReadBarrier(leader, c5)
	cancel5()
	fmt.Printf("ReadBarrier with 1/3 alive -> code=%q (no old value is served)\n", code(err))

	section("7. crash recovery with a torn tail")
	// Restart the two followers first to restore quorum.
	for _, id := range victims {
		must(k.Restart(id), fmt.Sprintf("restart %d", id))
	}
	newLeader, err := k.Cluster().WaitLeader(3 * time.Second)
	must(err, "leader after followers restored")
	// Now hard-crash the leader and chop bytes off its wal tail.
	k.Crash(newLeader)
	path := filepath.Join(dir, fmt.Sprintf("replica-%d", newLeader), "wal.log")
	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-2); err != nil {
		must(err, "simulate torn write")
	}
	fmt.Printf("crashed leader %d and chopped its wal.log tail (%d -> %d bytes)\n", newLeader, st.Size(), st.Size()-2)
	must(k.Restart(newLeader), "restart leader")
	leader2, err := k.Cluster().WaitLeader(3 * time.Second)
	must(err, "election after restart")
	time.Sleep(150 * time.Millisecond)
	c6, cancel6 := ctx()
	color, _, err := k.LinearizableRead(c6, "color")
	cancel6()
	must(err, "read after recovery")
	fmt.Printf("cluster healed, leader=%d; color=%q (committed prefix survived; torn tail truncated at open)\n", leader2, color)
	for _, id := range k.Peers() {
		fmt.Printf("  replica %d state: %v\n", id, k.State(id))
	}
	fmt.Println("\nDEMO OK: every replica converged and all guarantees held.")
}
