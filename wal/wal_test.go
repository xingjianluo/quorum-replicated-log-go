package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendScanReopen(t *testing.T) {
	dir := t.TempDir()
	w, rep, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := [][]byte{[]byte("a"), []byte("set k v"), []byte("incr k 5")}
	for _, in := range inputs {
		if err := w.Append(in); err != nil {
			t.Fatalf("Append(%q): %v", in, err)
		}
	}
	t.Logf("input=%q, want=3 durable records", inputs)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, rep2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := w2.Scan()
	if err != nil {
		t.Fatalf("Scan after reopen: %v", err)
	}
	t.Logf("repair=%+v scanned=%q judgement=reopened log must preserve all fsynced records in order", rep2, got)
	if len(got) != len(inputs) {
		t.Fatalf("got %d records, want %d", len(got), len(inputs))
	}
	for i := range inputs {
		if string(got[i]) != string(inputs[i]) {
			t.Fatalf("record %d = %q, want %q", i, got[i], inputs[i])
		}
	}
	if rep.TruncatedBytes != 0 || rep2.TruncatedBytes != 0 {
		t.Fatalf("fresh/clean log must need no repair, got %+v %+v", rep, rep2)
	}
	_ = w2.Close()
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []string{"rec-1", "rec-2", "rec-3"} {
		if err := w.Append([]byte(in)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, logName)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: chop 3 bytes off the last frame.
	torn := st.Size() - 3
	t.Logf("input: truncate file from %d bytes to %d bytes (partial-frame at tail)", st.Size(), torn)
	if err := os.Truncate(path, torn); err != nil {
		t.Fatal(err)
	}

	w2, rep, err := Open(dir)
	if err != nil {
		t.Fatalf("Open must repair torn tail, got error %v", err)
	}
	got, err := w2.Scan()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("repair=%+v scanned=%q judgement=incomplete tail record must be dropped, never applied", rep, got)
	if len(got) != 2 || string(got[0]) != "rec-1" || string(got[1]) != "rec-2" {
		t.Fatalf("torn tail repair wrong, got %q", got)
	}
	if rep.TruncatedBytes == 0 || rep.Reason != "partial-frame" {
		t.Fatalf("repair report must explain partial-frame, got %+v", rep)
	}
	// The next append must work on the repaired prefix.
	if err := w2.Append([]byte("rec-3-rewritten")); err != nil {
		t.Fatal(err)
	}
	_ = w2.Close()
}

func TestCRCMismatchTail(t *testing.T) {
	dir := t.TempDir()
	w, _, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("good-record")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("corrupted-record")); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	path := filepath.Join(dir, logName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a payload byte inside the LAST frame only; prior records stay intact.
	data[len(data)-1-2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: flip one payload byte in last frame; judgement=bad frame and everything after it must be rejected")

	w2, rep, err := Open(dir)
	if err != nil {
		t.Fatalf("corrupt tail must be repairable, got %v", err)
	}
	got, _ := w2.Scan()
	t.Logf("repair=%+v scanned=%q", rep, got)
	if len(got) != 1 || string(got[0]) != "good-record" {
		t.Fatalf("crc-corrupt tail must be truncated, got %q", got)
	}
	if rep.Reason != "crc-mismatch" {
		t.Fatalf("reason = %q, want crc-mismatch", rep.Reason)
	}
	_ = w2.Close()
}

func TestTruncateTo(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := Open(dir)
	_ = w.AppendBatch([][]byte{[]byte("e1"), []byte("e2"), []byte("e3"), []byte("e4")})
	if err := w.TruncateTo(2); err != nil {
		t.Fatalf("TruncateTo: %v", err)
	}
	got, _ := w.Scan()
	t.Logf("input: 4 records, TruncateTo(2), scanned=%q judgement=only the first two frames physically remain", got)
	if len(got) != 2 || string(got[1]) != "e2" {
		t.Fatalf("TruncateTo wrong: %q", got)
	}
	_ = w.Close()
}
