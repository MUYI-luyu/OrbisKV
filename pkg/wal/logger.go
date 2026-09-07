package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry 记录了已被 Raft 提交的状态机操作。
type Entry struct {
	RaftIndex       int64    `json:"raft_index"`
	Term            int      `json:"term"`
	NodeID          int      `json:"node_id"`
	ReqID           int64    `json:"req_id"`
	OpType          string   `json:"op_type"`
	Key             string   `json:"key,omitempty"`
	Value           string   `json:"value,omitempty"`
	Version         int64    `json:"version,omitempty"`
	TTL             int64    `json:"ttl,omitempty"`
	Keys            []string `json:"keys,omitempty"`
	Versions        []int64  `json:"versions,omitempty"`
	ShardID         int      `json:"shard_id,omitempty"`
	ShardState      int32    `json:"shard_state,omitempty"`
	TargetGroup     int      `json:"target_group,omitempty"`
	Epoch           int64    `json:"epoch,omitempty"`
	Replicas        []string `json:"replicas,omitempty"`
	Cutoff          int64    `json:"cutoff,omitempty"`
	TxReadKeys      []string `json:"tx_read_keys,omitempty"`
	TxReadVersions  []int64  `json:"tx_read_versions,omitempty"`
	TxWriteKeys     []string `json:"tx_write_keys,omitempty"`
	TxWriteValues   []string `json:"tx_write_values,omitempty"`
	TxWriteVersions []int64  `json:"tx_write_versions,omitempty"`
	TxWriteDeletes  []bool   `json:"tx_write_deletes,omitempty"`
	TxTimeoutMs     int64    `json:"tx_timeout_ms,omitempty"`
	Timestamp       int64    `json:"timestamp"`
}

// WALRequest represents one append operation waiting for durable commit.
type WALRequest struct {
	Data []byte
	Done chan error
}

// Logger persists entries in JSONL format for easy audit and recovery.
type Logger struct {
	mu          sync.Mutex
	path        string
	file        *os.File
	enabled     bool
	syncOnWrite bool

	reqCh       chan WALRequest
	stopCh      chan struct{}
	workerDone  chan struct{}
	workerAlive bool
	closing     bool
	fatalErr    error
	replayDone  bool
}

func NewLogger(path string, enabled bool, syncOnWrite bool) (*Logger, error) {
	l := &Logger{path: path, enabled: enabled, syncOnWrite: syncOnWrite}
	if !enabled {
		return l, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}
	l.file = f

	l.mu.Lock()
	if err := l.startWorkerLocked(); err != nil {
		l.mu.Unlock()
		_ = f.Close()
		return nil, err
	}
	l.mu.Unlock()
	return l, nil
}

// 判断是否启用 WAL
func (l *Logger) Enabled() bool {
	return l != nil && l.enabled
}

// Replay reads WAL JSONL records sequentially and applies them through handler.
// If the file tail contains an incomplete JSON object, replay stops at the last complete one.
func (l *Logger) Replay(handler func(Entry) error) error {
	if l == nil || !l.enabled {
		return nil
	}
	if handler == nil {
		return errors.New("replay handler is nil")
	}

	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return errors.New("wal logger is closed")
	}
	if l.fatalErr != nil {
		err := l.fatalErr
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}
	if err := l.stopWorkerLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	if l.file == nil {
		err := l.fatalErr
		if err == nil {
			err = errors.New("wal logger is closed")
		}
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("sync wal before replay: %w", err)
	}
	l.mu.Unlock()

	err := replayFile(l.path, handler)
	if err != nil {
		_ = l.restartWorkerAfterMaintenance(nil)
		return err
	}

	l.mu.Lock()
	l.replayDone = true
	err = l.startWorkerLocked()
	l.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

// 序列化 Entry，入队，等待 worker 落盘完成
func (l *Logger) Append(entry Entry) error {
	if l == nil || !l.enabled {
		return nil
	}
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal wal entry: %w", err)
	}
	req := WALRequest{
		Data: append(data, '\n'),
		Done: make(chan error, 1),
	}

	l.mu.Lock()
	if !l.replayDone {
		l.mu.Unlock()
		return errors.New("wal replay not completed; call Replay before Append")
	}
	if l.file == nil {
		err := l.fatalErr
		if err == nil {
			err = errors.New("wal logger is closed")
		}
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}
	if l.closing || !l.workerAlive || l.reqCh == nil {
		l.mu.Unlock()
		return errors.New("wal logger is closed")
	}
	if l.fatalErr != nil {
		err := l.fatalErr
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}

	// Keep enqueue under lock so maintenance paths cannot swap channels concurrently.
	l.reqCh <- req
	l.mu.Unlock()

	if err := <-req.Done; err != nil {
		return fmt.Errorf("append wal entry: %w", err)
	}
	return nil
}

// AppendAsync serializes and enqueues an entry without waiting for fsync completion.
// It returns quickly and is intended for non-critical audit logging on hot paths.
func (l *Logger) AppendAsync(entry Entry) error {
	if l == nil || !l.enabled {
		return nil
	}
	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal wal entry: %w", err)
	}
	req := WALRequest{
		Data: append(data, '\n'),
		Done: nil,
	}

	l.mu.Lock()
	if !l.replayDone {
		l.mu.Unlock()
		return errors.New("wal replay not completed; call Replay before AppendAsync")
	}
	if l.file == nil {
		err := l.fatalErr
		if err == nil {
			err = errors.New("wal logger is closed")
		}
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}
	if l.closing || !l.workerAlive || l.reqCh == nil {
		l.mu.Unlock()
		return errors.New("wal logger is closed")
	}
	if l.fatalErr != nil {
		err := l.fatalErr
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}

	select {
	case l.reqCh <- req:
		l.mu.Unlock()
		return nil
	default:
		l.mu.Unlock()
		return errors.New("wal queue is full")
	}
}

// TruncateUpTo 保留 raft_index > upToIndex 的条目。
func (l *Logger) TruncateUpTo(upToIndex int64) error {
	if l == nil || !l.enabled {
		return nil
	}
	l.mu.Lock()
	if err := l.stopWorkerLocked(); err != nil {
		l.mu.Unlock()
		return err
	}

	if l.file == nil {
		err := l.fatalErr
		if err == nil {
			err = errors.New("wal logger is closed")
		}
		l.mu.Unlock()
		return fmt.Errorf("wal logger unavailable: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("sync wal before truncate: %w", err)
	}
	if err := l.file.Close(); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("close wal before truncate: %w", err)
	}
	l.file = nil
	l.mu.Unlock()

	opErr := rewriteWALAfterTruncate(l.path, upToIndex)
	restartErr := l.restartWorkerAfterMaintenance(nil)
	if opErr != nil {
		if restartErr != nil {
			return fmt.Errorf("truncate wal failed: %v (restart failed: %v)", opErr, restartErr)
		}
		return opErr
	}
	if restartErr != nil {
		return restartErr
	}
	return nil
}

// 按 upToIndex 重写 wal.tmp 再 rename
func rewriteWALAfterTruncate(path string, upToIndex int64) error {
	input, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open wal for truncate: %w", err)
	}
	defer input.Close()

	tmpPath := path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open wal tmp: %w", err)
	}
	defer func() {
		_ = tmp.Close()
	}()

	reader := bufio.NewReaderSize(input, 64*1024)

	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read wal for truncate: %w", readErr)
		}

		hasNewline := len(line) > 0 && line[len(line)-1] == '\n'
		raw := bytes.TrimSpace(line)
		if len(raw) > 0 {
			var entry Entry
			if err := json.Unmarshal(raw, &entry); err != nil {
				if errors.Is(readErr, io.EOF) || !hasNewline {
					break
				}
				return fmt.Errorf("decode wal entry during truncate: %w", err)
			}

			if entry.RaftIndex > upToIndex {
				if _, err := tmp.Write(raw); err != nil {
					return fmt.Errorf("write wal tmp: %w", err)
				}
				if _, err := tmp.Write([]byte("\n")); err != nil {
					return fmt.Errorf("write wal tmp newline: %w", err)
				}
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync wal tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close wal tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace wal after truncate: %w", err)
	}
	return nil
}

// 维护后 reopen 文件并重启 worker
func (l *Logger) restartWorkerAfterMaintenance(updateState func()) error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		l.mu.Lock()
		l.fatalErr = err
		l.mu.Unlock()
		return fmt.Errorf("reopen wal after maintenance: %w", err)
	}

	l.mu.Lock()
	if l.file != nil {
		_ = l.file.Close()
	}
	l.file = f
	l.fatalErr = nil
	if updateState != nil {
		updateState()
	}
	err = l.startWorkerLocked()
	l.mu.Unlock()
	if err != nil {
		_ = f.Close()
		return err
	}
	return nil
}

func (l *Logger) Close() error {
	if l == nil || !l.enabled {
		return nil
	}
	l.mu.Lock()
	l.closing = true
	if err := l.stopWorkerLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	if l.file == nil {
		l.mu.Unlock()
		return nil
	}
	err := l.file.Close()
	l.file = nil
	l.mu.Unlock()
	if err != nil {
		return fmt.Errorf("close wal file: %w", err)
	}
	return nil
}

// 创建通道并启动 runWorker
func (l *Logger) startWorkerLocked() error {
	if !l.enabled {
		return nil
	}
	if l.file == nil {
		return errors.New("wal logger has no open file")
	}
	if l.workerAlive {
		return nil
	}
	l.reqCh = make(chan WALRequest, 4096)
	l.stopCh = make(chan struct{})
	l.workerDone = make(chan struct{})
	l.workerAlive = true

	go l.runWorker(l.file, l.reqCh, l.stopCh, l.workerDone)
	return nil
}

// 关闭 stopCh，等待 worker 退出，清空通道状态
func (l *Logger) stopWorkerLocked() error {
	if !l.workerAlive {
		return nil
	}
	stopCh := l.stopCh
	workerDone := l.workerDone
	close(stopCh)
	l.mu.Unlock()
	<-workerDone
	l.mu.Lock()
	l.reqCh = nil
	l.stopCh = nil
	l.workerDone = nil
	l.workerAlive = false
	if l.fatalErr != nil {
		return fmt.Errorf("wal worker stopped with error: %w", l.fatalErr)
	}
	return nil
}

// 批量聚合请求并调用 writeAndSync，回填 Done
func (l *Logger) runWorker(file *os.File, reqCh <-chan WALRequest, stopCh <-chan struct{}, workerDone chan struct{}) {
	defer close(workerDone)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]WALRequest, 0, 256)
	var workerErr error

	flush := func() {
		if len(batch) == 0 {
			return
		}

		if workerErr != nil {
			for i := range batch {
				if batch[i].Done != nil {
					batch[i].Done <- workerErr
					close(batch[i].Done)
				}
			}
			batch = batch[:0]
			return
		}

		var buf bytes.Buffer
		for i := range batch {
			_, _ = buf.Write(batch[i].Data)
		}

		err := writeAndSync(file, buf.Bytes(), l.syncOnWrite)
		if err != nil {
			l.setFatalErr(err)
			workerErr = err
		}
		for i := range batch {
			if batch[i].Done != nil {
				batch[i].Done <- err
				close(batch[i].Done)
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case req := <-reqCh:
			if workerErr != nil {
				if req.Done != nil {
					req.Done <- workerErr
					close(req.Done)
				}
				continue
			}
			batch = append(batch, req)
			if len(batch) >= 256 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-stopCh:
			flush()
			return
		}
	}
}

// 单批 payload 写入并按需 file.Sync
func writeAndSync(file *os.File, payload []byte, syncOnWrite bool) error {
	if len(payload) == 0 {
		return nil
	}
	if file == nil {
		return errors.New("wal file is not available")
	}

	n, err := file.Write(payload)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil && syncOnWrite {
		err = file.Sync()
	}
	if err != nil {
		return err
	}
	return nil
}

// 设置 fatalErr 并关闭文件，进入不可用状态
func (l *Logger) setFatalErr(err error) {
	l.mu.Lock()
	if l.fatalErr == nil {
		l.fatalErr = err
	}
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
	l.mu.Unlock()
}

func replayFile(path string, handler func(Entry) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open wal for replay: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 64*1024)

	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("read wal during replay: %w", readErr)
		}

		hasNewline := len(line) > 0 && line[len(line)-1] == '\n'
		raw := bytes.TrimSpace(line)
		if len(raw) > 0 {
			var entry Entry
			if err := json.Unmarshal(raw, &entry); err != nil {
				if errors.Is(readErr, io.EOF) || !hasNewline {
					break
				}
				return fmt.Errorf("decode wal entry during replay: %w", err)
			}

			if err := handler(entry); err != nil {
				return fmt.Errorf("replay handler failed at raft_index=%d: %w", entry.RaftIndex, err)
			}
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	return nil
}
