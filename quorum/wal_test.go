package quorum

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := openWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	in := []Entry{
		{Term: 1, Index: 1, Command: Command{Op: OpSet, Key: "a", Value: "1", ClientID: "c1", Seq: 1}},
		{Term: 1, Index: 2, Command: Command{Op: OpSet, Key: "b", Value: "2", ClientID: "c1", Seq: 2}},
		{Term: 2, Index: 3, Command: Command{Op: OpDelete, Key: "a", ClientID: "c1", Seq: 3}},
	}
	for _, e := range in {
		if err := w.append(e); err != nil {
			t.Fatal(err)
		}
	}
	w.close()

	w2, err := openWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	got := w2.entries()
	t.Logf("输入: 追加 %d 条记录后关闭重开", len(in))
	t.Logf("判定依据: 重开后应完整读回 %d 条，实际 %d 条", len(in), len(got))
	if len(got) != len(in) {
		t.Fatalf("expected %d entries, got %d", len(in), len(got))
	}
	for i := range in {
		if got[i] != in[i] {
			t.Fatalf("entry %d mismatch: got %+v want %+v", i, got[i], in[i])
		}
	}
	if idx, term := w2.lastIndexTerm(); idx != 3 || term != 2 {
		t.Fatalf("lastIndexTerm = (%d,%d), want (3,2)", idx, term)
	}
}

func TestWALTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, _ := openWAL(path, true)
	w.append(Entry{Term: 1, Index: 1, Command: Command{Op: OpSet, Key: "x", Value: "ok"}})
	w.append(Entry{Term: 1, Index: 2, Command: Command{Op: OpSet, Key: "y", Value: "ok"}})
	w.close()

	// 模拟崩溃造成的残缺尾部：文件末尾追加半条记录。
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0x20, 0x00, 0x00, 0x00, 0xde, 0xad}) // 半个头部
	f.Close()

	w2, err := openWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	got := w2.entries()
	t.Logf("输入: 2 条完整记录 + 尾部 6 字节残缺数据")
	t.Logf("判定依据: 残缺尾部必须被截断而非静默应用，剩余记录数=%d", len(got))
	if len(got) != 2 {
		t.Fatalf("torn tail not truncated: got %d entries, want 2", len(got))
	}
	// 截断后新记录应紧接完整前缀。
	if err := w2.append(Entry{Term: 2, Index: 3, Command: Command{Op: OpSet, Key: "z", Value: "new"}}); err != nil {
		t.Fatal(err)
	}
	w2.close()
	w3, _ := openWAL(path, true)
	if n := len(w3.entries()); n != 3 {
		t.Fatalf("after append past truncation: got %d entries, want 3", n)
	}
}

func TestWALCorruptCRCRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, _ := openWAL(path, true)
	w.append(Entry{Term: 1, Index: 1, Command: Command{Op: OpSet, Key: "k", Value: "good"}})
	w.append(Entry{Term: 1, Index: 2, Command: Command{Op: OpSet, Key: "k", Value: "bad"}})
	w.close()

	// 翻转第二条记录载荷的一个字节，使 CRC 失配。
	fi, _ := os.Stat(path)
	raw := make([]byte, fi.Size())
	f, _ := os.Open(path)
	f.Read(raw)
	f.Close()
	raw[fi.Size()-1] ^= 0xff
	os.WriteFile(path, raw, 0o644)

	w2, _ := openWAL(path, true)
	got := w2.entries()
	t.Logf("输入: 2 条记录，第 2 条载荷被翻转 1 字节（CRC 失配）")
	t.Logf("判定依据: 损坏记录及其后内容不得进入内存日志，剩余记录数=%d", len(got))
	if len(got) != 1 {
		t.Fatalf("corrupt record applied: got %d entries, want 1", len(got))
	}
	if got[0].Command.Value != "good" {
		t.Fatalf("wrong surviving entry: %+v", got[0])
	}
}

func TestWALTruncateFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _ := openWAL(path, true)
	for i := uint64(1); i <= 5; i++ {
		w.append(Entry{Term: i, Index: i, Command: Command{Op: OpSet, Key: "k"}})
	}
	if err := w.truncateFrom(4); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 5 条记录，truncateFrom(4)")
	t.Logf("判定依据: 内存与磁盘都应只剩索引 1..3")
	if idx, _ := w.lastIndexTerm(); idx != 3 {
		t.Fatalf("lastIndex = %d, want 3", idx)
	}
	w.close()
	w2, _ := openWAL(path, true)
	if n := len(w2.entries()); n != 3 {
		t.Fatalf("reopen after truncate: got %d entries, want 3", n)
	}
}
