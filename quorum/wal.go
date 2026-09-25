package quorum

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// wal 是预写日志。磁盘记录格式：
//
//	[uint32 payloadLen][uint32 crc32(payload)][payload(gob 编码的 Entry)]
//
// 崩溃恢复时从头扫描，遇到长度残缺、CRC 不匹配或解码失败的记录，
// 一律视为崩溃造成的残缺尾部，将文件截断到最后一条完整记录处，
// 残缺数据绝不进入内存日志。
type wal struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	fsync   bool
	log     []Entry // 内存镜像，与磁盘完整记录一一对应
}

func openWAL(path string, fsync bool) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	w := &wal{path: path, f: f, fsync: fsync}
	if err := w.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// recover 扫描磁盘文件，加载完整记录并截断残缺尾部。
func (w *wal) recover() error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var goodOffset int64 // 最后一条完整记录的结束位置
	var offset int64
	buf := make([]byte, 8)
	for {
		if _, err := w.f.ReadAt(buf, offset); err != nil {
			break // 文件结束或头部残缺：截断点即此处
		}
		payloadLen := binary.LittleEndian.Uint32(buf[0:4])
		crc := binary.LittleEndian.Uint32(buf[4:8])
		if payloadLen == 0 || payloadLen > 1<<20 {
			break // 非法长度：视为损坏
		}
		payload := make([]byte, payloadLen)
		if _, err := w.f.ReadAt(payload, offset+8); err != nil {
			break // 记录体残缺
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // CRC 不匹配：损坏记录
		}
		var e Entry
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&e); err != nil {
			break // 无法解码：损坏记录
		}
		w.log = append(w.log, e)
		offset += 8 + int64(payloadLen)
		goodOffset = offset
	}
	// 截断残缺尾部，保证后续追加紧跟最后一条完整记录。
	if err := w.f.Truncate(goodOffset); err != nil {
		return err
	}
	_, err := w.f.Seek(goodOffset, io.SeekStart)
	return err
}

// append 持久化一条日志记录（写盘 + 可选 fsync）。
// 提交顺序与持久化顺序一致：调用方保证按索引递增顺序追加。
func (w *wal) append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return err
	}
	payload := buf.Bytes()
	rec := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload))
	copy(rec[8:], payload)
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if w.fsync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	w.log = append(w.log, e)
	return nil
}

// truncateFrom 截断掉索引 >= from 的所有记录（磁盘与内存同步截断）。
func (w *wal) truncateFrom(from uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	keep := 0
	for keep < len(w.log) && w.log[keep].Index < from {
		keep++
	}
	if keep == len(w.log) {
		return nil
	}
	// 重新编码保留前缀，保证磁盘内容与内存一致。
	var buf bytes.Buffer
	for _, e := range w.log[:keep] {
		payload := &bytes.Buffer{}
		if err := gob.NewEncoder(payload).Encode(e); err != nil {
			return err
		}
		rec := make([]byte, 8+payload.Len())
		binary.LittleEndian.PutUint32(rec[0:4], uint32(payload.Len()))
		binary.LittleEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(payload.Bytes()))
		copy(rec[8:], payload.Bytes())
		buf.Write(rec)
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.WriteAt(buf.Bytes(), 0); err != nil {
		return err
	}
	if w.fsync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	w.log = append([]Entry(nil), w.log[:keep]...)
	return nil
}

// entries 返回当前完整记录序列的副本。
func (w *wal) entries() []Entry {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Entry(nil), w.log...)
}

// lastIndexTerm 返回最后一条记录的索引与任期；空日志返回 0,0。
func (w *wal) lastIndexTerm() (uint64, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.log) == 0 {
		return 0, 0
	}
	last := w.log[len(w.log)-1]
	return last.Index, last.Term
}

// at 返回指定索引处的记录；越界返回 false。
func (w *wal) at(index uint64) (Entry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range w.log {
		if e.Index == index {
			return e, true
		}
	}
	return Entry{}, false
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// syncDir fsync 目录，保证 rename/创建持久（供 meta 使用）。
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
