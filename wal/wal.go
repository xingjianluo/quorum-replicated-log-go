// Package wal implements a fsynced, length-prefixed and checksummed
// write-ahead log. A torn or corrupt frame at the tail of the file is
// detected on Open and truncated so that incomplete records are never
// returned to the caller.
//
// On-disk frame layout (all integers big-endian):
//
//	+-------+------+----------+---------+---------+
//	| magic | kind | uint32 n | n bytes | uint32  |
//	| 0xA5  |      | length   | payload | crc32   |
//	+-------+------+----------+---------+---------+
//
// The CRC covers kind, the length bytes and the payload. Every Append is
// followed by an fsync so acknowledged records survive process crashes.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	magicByte byte = 0xA5
	kindData  byte = 0x01
	headerLen      = 6 // magic + kind + uint32 length
	crcLen         = 4
	maxRecord      = 64 << 20
	logName        = "wal.log"
)

// WAL is an append-only write-ahead log backed by a single file.
type WAL struct {
	dir string
	f   *os.File
}

// RepairReport describes what Open found while replaying the log.
type RepairReport struct {
	// ValidRecords is the number of intact frames kept.
	ValidRecords int
	// TruncatedBytes is the number of bytes dropped from the tail.
	TruncatedBytes int64
	// Reason explains a non-empty truncation: "partial-frame",
	// "bad-magic", "length-too-large" or "crc-mismatch"; empty when
	// the log was intact.
	Reason string
}

// Open opens (or creates) the log inside dir and repairs a torn tail:
// every frame from the first damaged one to EOF is dropped atomically.
func Open(dir string) (*WAL, RepairReport, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, RepairReport{}, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, logName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, RepairReport{}, fmt.Errorf("wal: open %s: %w", path, err)
	}
	rep, err := repair(f)
	if err != nil {
		f.Close()
		return nil, RepairReport{}, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, RepairReport{}, fmt.Errorf("wal: seek end: %w", err)
	}
	return &WAL{dir: dir, f: f}, rep, nil
}

// repair scans every frame and truncates the file at the first damaged
// frame, so only the intact prefix survives a crash.
func repair(f *os.File) (RepairReport, error) {
	info, err := f.Stat()
	if err != nil {
		return RepairReport{}, err
	}
	total := info.Size()
	var off int64
	valid := 0
	for off < total {
		hdr := make([]byte, headerLen)
		n, err := f.ReadAt(hdr, off)
		if err != nil && !errors.Is(err, io.EOF) {
			return RepairReport{}, fmt.Errorf("wal: read header: %w", err)
		}
		frameStart := off
		if n < headerLen {
			// A partial header can only be a torn tail record.
			return finishRepair(f, frameStart, total, valid, "partial-frame")
		}
		if hdr[0] != magicByte {
			return finishRepair(f, frameStart, total, valid, "bad-magic")
		}
		length := int64(binary.BigEndian.Uint32(hdr[2:6]))
		if length <= 0 || length > maxRecord {
			return finishRepair(f, frameStart, total, valid, "length-too-large")
		}
		bodyLen := length + crcLen
		body := make([]byte, bodyLen)
		n, err = f.ReadAt(body, off+headerLen)
		if err != nil && !errors.Is(err, io.EOF) {
			return RepairReport{}, fmt.Errorf("wal: read body: %w", err)
		}
		if int64(n) < bodyLen {
			return finishRepair(f, frameStart, total, valid, "partial-frame")
		}
		want := binary.BigEndian.Uint32(body[length:])
		got := crc32.ChecksumIEEE(append(hdr[1:headerLen], body[:length]...))
		if want != got {
			return finishRepair(f, frameStart, total, valid, "crc-mismatch")
		}
		off += headerLen + bodyLen
		valid++
	}
	return RepairReport{ValidRecords: valid}, nil
}

// finishRepair truncates the file at goodEnd and returns the report.
func finishRepair(f *os.File, goodEnd, total int64, valid int, reason string) (RepairReport, error) {
	if err := f.Truncate(goodEnd); err != nil {
		return RepairReport{}, fmt.Errorf("wal: truncate torn tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return RepairReport{}, fmt.Errorf("wal: sync repair: %w", err)
	}
	return RepairReport{
		ValidRecords:   valid,
		TruncatedBytes: total - goodEnd,
		Reason:         reason,
	}, nil
}

// encodeFrame builds one framed record.
func encodeFrame(data []byte) []byte {
	buf := make([]byte, headerLen+len(data)+crcLen)
	buf[0] = magicByte
	buf[1] = kindData
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(data)))
	copy(buf[headerLen:headerLen+len(data)], data)
	crc := crc32.ChecksumIEEE(buf[1 : headerLen+len(data)])
	binary.BigEndian.PutUint32(buf[headerLen+len(data):], crc)
	return buf
}

// Append writes one framed, checksummed record and fsyncs it before
// returning, so a successful Append is durable.
func (w *WAL) Append(data []byte) error {
	return w.appendFrames(data)
}

// AppendBatch writes several records with a single fsync at the end.
// Either all frames become durable together or (after a crash) none of
// the torn tail frames survive replay.
func (w *WAL) AppendBatch(records [][]byte) error {
	if len(records) == 0 {
		return nil
	}
	parts := make([][]byte, 0, len(records))
	for _, rec := range records {
		parts = append(parts, encodeFrame(rec))
	}
	_, err := w.f.Write(ioBytes(parts))
	if err != nil {
		return fmt.Errorf("wal: write batch: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync batch: %w", err)
	}
	return nil
}

func (w *WAL) appendFrames(records ...[]byte) error {
	return w.AppendBatch(records)
}

// ioBytes joins slices without copying their payloads more than needed by
// writing them one by one; Write handles [][]byte through a simple loop.
func ioBytes(parts [][]byte) []byte {
	size := 0
	for _, p := range parts {
		size += len(p)
	}
	out := make([]byte, 0, size)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// Scan replays all intact records in order.
func (w *WAL) Scan() ([][]byte, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek start: %w", err)
	}
	var out [][]byte
	bufLen := make([]byte, 4)
	for {
		hdr := make([]byte, headerLen)
		if _, err := io.ReadFull(w.f, hdr); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.New("wal: unexpected partial header on a repaired log")
			}
			return nil, err
		}
		if hdr[0] != magicByte || hdr[1] != kindData {
			return nil, errors.New("wal: corrupt frame header on a repaired log")
		}
		length := binary.BigEndian.Uint32(hdr[2:6])
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return nil, fmt.Errorf("wal: read payload: %w", err)
		}
		if _, err := io.ReadFull(w.f, bufLen); err != nil {
			return nil, fmt.Errorf("wal: read crc: %w", err)
		}
		want := binary.BigEndian.Uint32(bufLen)
		got := crc32.ChecksumIEEE(append(hdr[1:headerLen], payload...))
		if want != got {
			return nil, errors.New("wal: crc mismatch on a repaired log")
		}
		out = append(out, payload)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}
	return out, nil
}

// TruncateTo keeps the first keep records and physically drops the rest
// in one synced truncation.
func (w *WAL) TruncateTo(keep int) error {
	records, err := w.Scan()
	if err != nil {
		return err
	}
	if keep >= len(records) {
		return nil
	}
	var end int64 = headerLen * int64(keep)
	for _, rec := range records[:keep] {
		end += int64(len(rec)) + crcLen
	}
	if err := w.f.Truncate(end); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// Close releases the underlying file after an fsync.
func (w *WAL) Close() error {
	if w == nil || w.f == nil {
		return nil
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		w.f = nil
		return fmt.Errorf("wal: sync close: %w", err)
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// kindHardState marks the most recent term/vote frame. It is never
// returned by Scan, which only yields kindData frames.
const kindHardState byte = 0x02

// AppendRaw writes one framed record with an explicit kind byte and
// fsyncs it.
func (w *WAL) AppendRaw(kind byte, data []byte) error {
	buf := make([]byte, headerLen+len(data)+crcLen)
	buf[0] = magicByte
	buf[1] = kind
	binary.BigEndian.PutUint32(buf[2:6], uint32(len(data)))
	copy(buf[headerLen:headerLen+len(data)], data)
	crc := crc32.ChecksumIEEE(buf[1 : headerLen+len(data)])
	binary.BigEndian.PutUint32(buf[headerLen+len(data):], crc)
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("wal: write raw: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync raw: %w", err)
	}
	return nil
}

// AppendHardState atomically records a hardstate frame.
func (w *WAL) AppendHardState(data []byte) error {
	return w.AppendRaw(kindHardState, data)
}

// scanWithKind returns (kind, payload) pairs for every intact frame.
func (w *WAL) scanWithKind() ([]struct {
	Kind byte
	Data []byte
}, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	type frame = struct {
		Kind byte
		Data []byte
	}
	var out []frame
	for {
		hdr := make([]byte, headerLen)
		if _, err := io.ReadFull(w.f, hdr); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errors.New("wal: partial header on repaired log")
			}
			return nil, err
		}
		if hdr[0] != magicByte {
			return nil, errors.New("wal: bad magic on repaired log")
		}
		kind := hdr[1]
		length := binary.BigEndian.Uint32(hdr[2:6])
		payload := make([]byte, length)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return nil, fmt.Errorf("wal: read payload: %w", err)
		}
		crcBytes := make([]byte, crcLen)
		if _, err := io.ReadFull(w.f, crcBytes); err != nil {
			return nil, fmt.Errorf("wal: read crc: %w", err)
		}
		want := binary.BigEndian.Uint32(crcBytes)
		got := crc32.ChecksumIEEE(append(hdr[1:headerLen], payload...))
		if want != got {
			return nil, errors.New("wal: crc mismatch on repaired log")
		}
		out = append(out, frame{Kind: kind, Data: payload})
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return out, nil
}

// ScanHardState replays frames and returns the data entries in order plus
// the payload of the last hardstate frame (nil if none).
func (w *WAL) ScanHardState() (dataRecords [][]byte, hardState []byte, err error) {
	frames, err := w.scanWithKind()
	if err != nil {
		return nil, nil, err
	}
	for _, fr := range frames {
		switch fr.Kind {
		case kindData:
			dataRecords = append(dataRecords, fr.Data)
		case kindHardState:
			hardState = fr.Data
		default:
			return nil, nil, fmt.Errorf("wal: unknown frame kind %d", fr.Kind)
		}
	}
	return dataRecords, hardState, nil
}

// Rewrite atomically replaces the log contents: the data frames listed
// in dataRecords followed by one hardstate frame. Used when a raft
// follower must drop conflicting suffix entries while preserving the
// durable current-term/vote frame.
func (w *WAL) Rewrite(dataRecords [][]byte, hardState []byte) error {
	path := filepath.Join(w.dir, logName)
	tmp := path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create rewrite tmp: %w", err)
	}
	writeFrame := func(kind byte, data []byte) error {
		buf := make([]byte, headerLen+len(data)+crcLen)
		buf[0] = magicByte
		buf[1] = kind
		binary.BigEndian.PutUint32(buf[2:6], uint32(len(data)))
		copy(buf[headerLen:headerLen+len(data)], data)
		crc := crc32.ChecksumIEEE(buf[1 : headerLen+len(data)])
		binary.BigEndian.PutUint32(buf[headerLen+len(data):], crc)
		if _, err := tf.Write(buf); err != nil {
			return err
		}
		return nil
	}
	for _, rec := range dataRecords {
		if err := writeFrame(kindData, rec); err != nil {
			tf.Close()
			os.Remove(tmp)
			return fmt.Errorf("wal: rewrite write: %w", err)
		}
	}
	if hardState != nil {
		if err := writeFrame(kindHardState, hardState); err != nil {
			tf.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		os.Remove(tmp)
		return fmt.Errorf("wal: rewrite sync tmp: %w", err)
	}
	if err := tf.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("wal: rewrite rename: %w", err)
	}
	// fsync the directory so the rename itself is durable.
	if dir, derr := os.Open(w.dir); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	nf, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopen after rewrite: %w", err)
	}
	if _, err := nf.Seek(0, io.SeekEnd); err != nil {
		nf.Close()
		return err
	}
	w.f.Close()
	w.f = nf
	return nil
}
