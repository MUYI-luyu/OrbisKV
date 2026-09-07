package persister

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	manifestFileName = "manifest.json"
	defaultBatchSize = 64

	appendFrameMagic      uint32 = 0x52414644 // "RAFD"
	appendFrameHeaderSize        = 16

	raftAppendPayloadMagic      uint32 = 0x52504131 // "RPA1"
	raftAppendPayloadHeaderSize        = 28

	raftAppendOffMagic     = 0
	raftAppendOffPrevLen   = 4
	raftAppendOffNewLen    = 12
	raftAppendOffHeaderLen = 20
	raftAppendOffDeltaLen  = 24

	defaultDeltaCompactBytes int64 = 4 * 1024 * 1024
)

type persisterManifest struct {
	Generation   uint64 `json:"generation"`
	RaftStateRel string `json:"raft_state_rel"`
	RaftDeltaRel string `json:"raft_delta_rel,omitempty"`
	SnapshotRel  string `json:"snapshot_rel"`
	UpdatedAt    int64  `json:"updated_at"`
}

type saveRequest struct {
	raftstate []byte
	snapshot  []byte
	done      chan error
}

type hardStateRequest struct {
	hardstate []byte
	done      chan error
}

type appendStateRequest struct {
	data []byte
	done chan error
}

// 性能画像
type PersisterMetrics struct {
	UptimeSeconds    float64 // 运行时间
	FsyncCount       int64   // Fsync计数
	FsyncPerSecond   float64 // Fsync占用时间
	AvgSaveBatch     float64 // save需求平均批次
	AvgAppendBatch   float64 // append需求平均批次
	AvgHardBatch     float64 // 硬状态需求平均批次
	SaveBatchCount   int64   // save需求批次总数
	AppendBatchCount int64   // append需求批次总数
	HardBatchCount   int64   // 硬状态需求批次总数
}

// FilePersister 基于文件系统的持久化器实现
// 原子性写入：先写临时文件，再重命名，确保崩溃时不损坏
type FilePersister struct {
	mu            sync.RWMutex // 保护内存中的 manifest 视图、文件路径映射以及 closed 状态的读写锁
	dataDir       string       // 存储持久化文件的根目录路径
	tmpDir        string       // 临时文件目录，用于“先写临时文件再原子重命名”的机制以保证崩溃一致性
	raftStateFile string       // 当前有效的 Raft 状态基线文件（Base）的绝对路径
	hardStateFile string       // 当前有效的硬状态（Term/Vote）文件的绝对路径
	snapshotFile  string       // 当前有效的状态机快照文件的绝对路径
	manifestPath  string       // 记录元数据索引（manifest.json）的绝对路径，是加载流程的起点

	manifest    persisterManifest       // 内存中缓存的元数据快照，包含当前数据的代际（Generation）和各文件的相对路径
	batchSize   int                     // 批处理触发阈值，定义了单次异步刷盘前最多积压的任务数量
	reqCh       chan saveRequest        // 全量保存（RaftState + Snapshot）的任务分发管道
	hardReqCh   chan hardStateRequest   // 高频硬状态更新的任务分发管道
	appendReqCh chan appendStateRequest // 增量日志追加（Append）的任务分发管道
	stopCh      chan struct{}           // 停止信号，用于通知后台 Worker 协程停止接收新任务并准备退出
	workerDone  chan struct{}           // Worker 退出确认信号，确保所有积压任务已完成 Flush 落盘
	closed      bool                    // 标识当前持久化器是否已关闭，防止关闭后继续写入

	deltaCompactBytes int64 // 增量压实阈值，当 .wal 文件超过此大小时触发“增量转全量”的合并逻辑

	statsStartedAt time.Time // 统计开始时间，用于计算系统运行以来的吞吐率和平均延迟
	fsyncCount     int64     // 累计调用磁盘 fsync 的次数，是评估 I/O 开销的核心原子计数器

	saveBatchCount int64 // 累计执行全量保存的批次总数
	saveBatchItems int64 // 累计处理的全量保存请求总数，与 count 相除可得平均批大小

	appendBatchCount int64 // 累计执行增量追加的批次总数
	appendBatchItems int64 // 累计处理的增量追加请求总数

	hardBatchCount int64 // 累计执行硬状态更新的批次总数
	hardBatchItems int64 // 累计处理的硬状态更新请求总数
}

// NewFilePersister 创建基于文件的持久化器
// dataDir: 数据目录，例如 "badger-127.0.0.1:15000/"
func NewFilePersister(dataDir string) (*FilePersister, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	fp := &FilePersister{
		dataDir:           dataDir,
		raftStateFile:     filepath.Join(dataDir, "raft-state.bin"),
		hardStateFile:     filepath.Join(dataDir, "hard-state.bin"),
		snapshotFile:      filepath.Join(dataDir, "snapshot.bin"),
		manifestPath:      filepath.Join(dataDir, manifestFileName),
		tmpDir:            filepath.Join(dataDir, ".tmp"),
		batchSize:         defaultBatchSize,
		deltaCompactBytes: defaultDeltaCompactBytes,
		statsStartedAt:    time.Now(),
	}

	if err := os.MkdirAll(fp.tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir tmp dir: %w", err)
	}

	if err := fp.loadManifest(); err != nil {
		return nil, err
	}

	fp.mu.Lock()
	fp.startWorkerLocked()
	fp.mu.Unlock()

	return fp, nil
}

func immediateDoneChan(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	close(ch)
	return ch
}

// EnqueueSave 异步入队 Raft 状态保存请求，返回可等待的完成通道。
func (fp *FilePersister) EnqueueSave(raftstate []byte, snapshot []byte) <-chan error {
	fp.mu.RLock()
	if fp.closed || fp.reqCh == nil || fp.stopCh == nil {
		fp.mu.RUnlock()
		return immediateDoneChan(fmt.Errorf("persister closed"))
	}
	reqCh := fp.reqCh
	stopCh := fp.stopCh
	fp.mu.RUnlock()

	req := saveRequest{
		raftstate: append([]byte(nil), raftstate...),
		snapshot:  append([]byte(nil), snapshot...),
		done:      make(chan error, 1),
	}

	select {
	case reqCh <- req:
		return req.done
	case <-stopCh:
		return immediateDoneChan(fmt.Errorf("persister stopping"))
	}
}

// EnqueueAppendRaftState 异步入队增量追加请求，返回可等待的完成通道。
func (fp *FilePersister) EnqueueAppendRaftState(data []byte) <-chan error {
	fp.mu.RLock()
	if fp.closed || fp.appendReqCh == nil || fp.stopCh == nil {
		fp.mu.RUnlock()
		return immediateDoneChan(fmt.Errorf("persister closed"))
	}
	appendReqCh := fp.appendReqCh
	stopCh := fp.stopCh
	fp.mu.RUnlock()

	req := appendStateRequest{
		data: append([]byte(nil), data...),
		done: make(chan error, 1),
	}

	select {
	case appendReqCh <- req:
		return req.done
	case <-stopCh:
		return immediateDoneChan(fmt.Errorf("persister stopping"))
	}
}

// EnqueueSaveHardState 异步入队硬状态保存请求，返回可等待的完成通道。
func (fp *FilePersister) EnqueueSaveHardState(hardstate []byte) <-chan error {
	fp.mu.RLock()
	if fp.closed || fp.hardReqCh == nil || fp.stopCh == nil {
		fp.mu.RUnlock()
		return immediateDoneChan(fmt.Errorf("persister closed"))
	}
	hardReqCh := fp.hardReqCh
	stopCh := fp.stopCh
	fp.mu.RUnlock()

	req := hardStateRequest{
		hardstate: append([]byte(nil), hardstate...),
		done:      make(chan error, 1),
	}

	select {
	case hardReqCh <- req:
		return req.done
	case <-stopCh:
		return immediateDoneChan(fmt.Errorf("persister stopping"))
	}
}

// ReadRaftState 读取 Raft 状态（日志+任期+投票）
func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.RLock()
	basePath := fp.currentRaftStatePathLocked()
	deltaPath := fp.currentRaftDeltaPathLocked()
	defer fp.mu.RUnlock()

	state, _ := composeRaftState(basePath, deltaPath)
	return state
}

// ReadSnapshot 读取快照数据
func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.RLock()
	path := fp.currentSnapshotPathLocked()
	defer fp.mu.RUnlock()

	data, _ := os.ReadFile(path)
	return data
}

// ReadHardState 读取 Raft 硬状态（CurrentTerm/VotedFor）
func (fp *FilePersister) ReadHardState() []byte {
	fp.mu.RLock()
	path := fp.hardStateFile
	fp.mu.RUnlock()

	data, _ := os.ReadFile(path)
	return data
}

// Save 原子地保存 Raft 状态和快照
// 使用 write-then-rename 模式避免崩溃损坏
func (fp *FilePersister) Save(raftstate []byte, snapshot []byte) {
	done := fp.EnqueueSave(raftstate, snapshot)
	if err := <-done; err != nil {
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to save state/snapshot: %v\n", err)
	}
}

// AppendRaftState 以追加模式持久化增量 Raft 状态片段。
// 调用返回前保证本次追加已落盘。
func (fp *FilePersister) AppendRaftState(data []byte) {
	done := fp.EnqueueAppendRaftState(data)
	if err := <-done; err != nil {
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to append raft-state: %v\n", err)
	}
}

// SaveHardState 持久化 Raft 硬状态（CurrentTerm/VotedFor）
func (fp *FilePersister) SaveHardState(hardstate []byte) {
	done := fp.EnqueueSaveHardState(hardstate)
	if err := <-done; err != nil {
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to save hard-state: %v\n", err)
	}
}

// 初始化任务管道（reqCh, appendReqCh 等）并启动后台常驻协程
func (fp *FilePersister) startWorkerLocked() {
	// 检查相关管道是否已存在，确保 Worker 协程不会被重复启动
	if fp.reqCh != nil || fp.hardReqCh != nil || fp.appendReqCh != nil || fp.stopCh != nil || fp.workerDone != nil {
		return
	}
	fp.reqCh = make(chan saveRequest, 1024)
	fp.hardReqCh = make(chan hardStateRequest, 1024)
	fp.appendReqCh = make(chan appendStateRequest, 1024)
	fp.stopCh = make(chan struct{})
	fp.workerDone = make(chan struct{})
	go fp.runWorker(fp.reqCh, fp.hardReqCh, fp.appendReqCh, fp.stopCh, fp.workerDone, fp.batchSize)
}

// 关闭任务管道
func (fp *FilePersister) stopWorkerLocked() (chan struct{}, chan struct{}) {
	stopCh := fp.stopCh
	workerDone := fp.workerDone
	fp.reqCh = nil
	fp.hardReqCh = nil
	fp.appendReqCh = nil
	fp.stopCh = nil
	fp.workerDone = nil
	return stopCh, workerDone
}

// 启动带有定时刷新功能的异步批处理状态机
func (fp *FilePersister) runWorker(reqCh <-chan saveRequest, hardReqCh <-chan hardStateRequest, appendReqCh <-chan appendStateRequest, stopCh <-chan struct{}, workerDone chan struct{}, batchSize int) {
	defer close(workerDone)

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]saveRequest, 0, batchSize)
	hardBatch := make([]hardStateRequest, 0, batchSize)
	appendBatch := make([]appendStateRequest, 0, batchSize)

	flush := func() {
		// 处理硬状态
		if len(hardBatch) > 0 {
			latestHard := hardBatch[len(hardBatch)-1] // 只保留最后一次写入
			err := fp.commitHardState(latestHard.hardstate)
			atomic.AddInt64(&fp.hardBatchCount, 1)
			atomic.AddInt64(&fp.hardBatchItems, int64(len(hardBatch)))
			for i := range hardBatch {
				hardBatch[i].done <- err
				close(hardBatch[i].done)
			}
			hardBatch = hardBatch[:0]
		}

		if len(batch) == 0 {
			if len(appendBatch) == 0 {
				return // 如果全量和增量都没有，直接跳过
			}
		} else {
			latest := batch[len(batch)-1]
			err := fp.commitGeneration(latest.raftstate, latest.snapshot)
			atomic.AddInt64(&fp.saveBatchCount, 1)
			atomic.AddInt64(&fp.saveBatchItems, int64(len(batch)))
			for i := range batch {
				batch[i].done <- err
				close(batch[i].done)
			}
			batch = batch[:0]
		}

		if len(appendBatch) > 0 {
			payloads := make([][]byte, len(appendBatch))
			for i := range appendBatch {
				payloads[i] = appendBatch[i].data
			}
			err := fp.commitAppendRaftState(payloads)
			atomic.AddInt64(&fp.appendBatchCount, 1)
			atomic.AddInt64(&fp.appendBatchItems, int64(len(appendBatch)))
			for i := range appendBatch {
				appendBatch[i].done <- err
				close(appendBatch[i].done)
			}
			appendBatch = appendBatch[:0]
		}
	}

	for {
		select {
		case req := <-reqCh: // 全量保存请求
			batch = append(batch, req)
			if len(batch) >= batchSize {
				flush()
			}
		case req := <-hardReqCh: // 硬状态请求
			hardBatch = append(hardBatch, req)
			if len(hardBatch) >= batchSize {
				flush()
			}
		case req := <-appendReqCh: // 增量追加请求
			appendBatch = append(appendBatch, req)
			if len(appendBatch) >= batchSize {
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

func (fp *FilePersister) commitHardState(hardstate []byte) error {
	fp.mu.RLock()
	path := fp.hardStateFile
	fp.mu.RUnlock()
	if err := fp.durableWrite(path, hardstate); err != nil {
		return fmt.Errorf("durable write hard state: %w", err)
	}
	return nil
}

func buildAppendFrame(payload []byte) []byte {
	frame := make([]byte, appendFrameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], appendFrameMagic)             // 在帧起始位置写入幻数（标识符 0x52414644 即"RAFD"）
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))         // 记录后续原始数据的实际字节数。
	binary.LittleEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(payload)) // 计算并存储原始数据的 CRC32 校验码。
	binary.LittleEndian.PutUint32(frame[12:16], 0)                          // 预留字段填充
	copy(frame[appendFrameHeaderSize:], payload)
	return frame
}

// 输入当前状态 state 和增量载荷 payload，通过头部覆盖与尾部追加，输出更新后的状态。
func applyRaftAppendPayload(state []byte, payload []byte) ([]byte, bool) {
	// 基础校验：幻数识别与长度验证
	if len(payload) < raftAppendPayloadHeaderSize {
		return state, false
	}
	if binary.LittleEndian.Uint32(payload[raftAppendOffMagic:raftAppendOffMagic+4]) != raftAppendPayloadMagic {
		return state, false
	}

	// 解析增量协议字段
	prevLen := int(binary.LittleEndian.Uint64(payload[raftAppendOffPrevLen : raftAppendOffPrevLen+8]))       // 产生此增量前，旧状态应有的长度
	newLen := int(binary.LittleEndian.Uint64(payload[raftAppendOffNewLen : raftAppendOffNewLen+8]))          // 应用此增量后，新状态预期的长度。
	headerLen := int(binary.LittleEndian.Uint32(payload[raftAppendOffHeaderLen : raftAppendOffHeaderLen+4])) // 需要修改的状态头部字节数
	deltaLen := int(binary.LittleEndian.Uint32(payload[raftAppendOffDeltaLen : raftAppendOffDeltaLen+4]))    // 需要追加到末尾的新数据字节数

	// 状态一致性检查：确保这笔增量是基于当前内存状态的长度生成的
	if prevLen != len(state) {
		return state, false
	}
	// 合法性约束
	if headerLen <= 0 || headerLen > prevLen {
		return state, false
	}
	if deltaLen < 0 {
		return state, false
	}
	// 确保 payload 里的数据分布符合逻辑
	if raftAppendPayloadHeaderSize+headerLen+deltaLen != len(payload) {
		return state, false
	}
	if newLen != prevLen+deltaLen {
		return state, false
	}

	// 拆分增量包
	headerPatch := payload[raftAppendPayloadHeaderSize : raftAppendPayloadHeaderSize+headerLen] // 用来“打补丁”的头部数据
	delta := payload[raftAppendPayloadHeaderSize+headerLen:]                                    // 纯新增的日志数据
	// 修改头部：直接覆盖原有状态的前半部分（通常是 Term/Vote 等元数据）
	copy(state[:headerLen], headerPatch)
	// 追加日志：将新的日志条目直接 append 到切片末尾
	if deltaLen == 0 {
		return state, true
	}
	state = append(state, delta...)
	return state, len(state) == newLen
}

// 循环解析增量文件中的数据帧，通过幻数与 CRC 校验确保安全后，按序恢复历史状态演进。
// 返回 raft 状态与数据合法的终点线
func replayDeltaFrames(base []byte, delta []byte) ([]byte, int) {
	state := append([]byte(nil), base...)
	offset := 0
	for {
		// 1. 检查头部长度是否完整
		if len(delta)-offset < appendFrameHeaderSize {
			break
		}
		// 2. 幻数校验：确保从正确的位置开始读取
		if binary.LittleEndian.Uint32(delta[offset:offset+4]) != appendFrameMagic {
			break
		}
		// 3. 读取元数据：载荷长度与预期的 CRC 校验值
		payloadLen := int(binary.LittleEndian.Uint32(delta[offset+4 : offset+8]))
		expectedCRC := binary.LittleEndian.Uint32(delta[offset+8 : offset+12])
		// // 4. 越界检查：防止读取到损坏的截断帧
		if payloadLen < 0 || len(delta)-offset < appendFrameHeaderSize+payloadLen {
			break
		}
		// 5. CRC32 完整性校验：验证这帧数据在磁盘上是否完好
		payload := delta[offset+appendFrameHeaderSize : offset+appendFrameHeaderSize+payloadLen]
		if crc32.ChecksumIEEE(payload) != expectedCRC {
			break
		}
		// 6. 应用修改：将这帧增量合并到当前状态中
		nextState, ok := applyRaftAppendPayload(state, payload)
		if !ok {
			break
		}
		state = nextState
		offset += appendFrameHeaderSize + payloadLen
	}
	return state, offset
}

// 读取基线文件并尝试回放增量日志，以组装出 Raft 节点的最新完整状态。
func composeRaftState(basePath, deltaPath string) ([]byte, int) {
	base, _ := os.ReadFile(basePath) // 读取基线
	if deltaPath == "" {
		return base, 0
	}
	delta, err := os.ReadFile(deltaPath) // 读取增量
	if err != nil || len(delta) == 0 {
		return base, 0
	}
	return replayDeltaFrames(base, delta) // 增量日志循环解析
}

// 以追加模式高效持久化增量日志片段，并在 WAL 文件达到阈值时自动触发合并压实以实现代际更替。
func (fp *FilePersister) commitAppendRaftState(payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}

	fp.mu.RLock()
	deltaPath := fp.currentRaftDeltaPathLocked()
	basePath := fp.currentRaftStatePathLocked()
	compactThreshold := fp.deltaCompactBytes
	fp.mu.RUnlock()

	f, err := os.OpenFile(deltaPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open raft delta file: %w", err)
	}

	// 批量封装与顺序写入
	for i := range payloads {
		frame := buildAppendFrame(payloads[i])
		if _, err := f.Write(frame); err != nil {
			_ = f.Close()
			return fmt.Errorf("append raft delta frame: %w", err)
		}
	}

	// 物理落盘与统计
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync raft delta file: %w", err)
	}
	atomic.AddInt64(&fp.fsyncCount, 1)
	if err := f.Close(); err != nil {
		return fmt.Errorf("close raft delta file: %w", err)
	}

	// 自动压实（Delta-to-Base）检测
	if compactThreshold <= 0 {
		return nil
	}

	info, err := os.Stat(deltaPath)
	if err != nil {
		return nil
	}
	if info.Size() < compactThreshold {
		return nil
	}

	// 执行合并与代际更替
	state, _ := composeRaftState(basePath, deltaPath)
	if len(state) == 0 {
		return nil
	}

	if err := fp.commitGeneration(state, nil); err != nil {
		return fmt.Errorf("compact raft delta to base: %w", err)
	}
	return nil
}

// 通过全量写入 Raft 状态与快照并更新元数据索引，实现合并增量日志、开启新数据代际的原子性操作。
func (fp *FilePersister) commitGeneration(raftstate []byte, snapshot []byte) error {
	fp.mu.Lock()
	nextGen := fp.manifest.Generation + 1
	prevSnapshotRel := fp.manifest.SnapshotRel
	fp.mu.Unlock()

	raftRel := fmt.Sprintf("raft-state.%020d.bin", nextGen)
	deltaRel := fmt.Sprintf("raft-state.delta.%020d.wal", nextGen)
	raftPath := filepath.Join(fp.dataDir, raftRel)
	deltaPath := filepath.Join(fp.dataDir, deltaRel)
	snapshotRel := prevSnapshotRel
	// 持久化 Raft 基线状态
	if err := fp.durableWrite(raftPath, raftstate); err != nil {
		return fmt.Errorf("durable write raft state: %w", err)
	}

	// 仅在调用方显式提供 snapshot 时重写 snapshot 文件，
	// 常规 Raft persist（snapshot=nil）不触发 snapshot IO。
	if snapshot != nil {
		snapshotRel = fmt.Sprintf("snapshot.%020d.bin", nextGen)
		snapshotPath := filepath.Join(fp.dataDir, snapshotRel)
		if err := fp.durableWrite(snapshotPath, snapshot); err != nil {
			return fmt.Errorf("durable write snapshot: %w", err)
		}
	}
	// 如果系统没有任何历史快照记录，则强制创建一个空的引导快照文件，确保元数据完整性
	if snapshotRel == "" {
		snapshotRel = fmt.Sprintf("snapshot.%020d.bin", nextGen)
		snapshotPath := filepath.Join(fp.dataDir, snapshotRel)
		if err := fp.durableWrite(snapshotPath, nil); err != nil {
			return fmt.Errorf("durable write bootstrap snapshot: %w", err)
		}
	}
	// 新代际的开始，后续的增量写入将从这个新文件开始追加
	if err := fp.durableWrite(deltaPath, nil); err != nil {
		return fmt.Errorf("durable write raft delta: %w", err)
	}

	// 更新 Manifest 索引文件
	m := persisterManifest{
		Generation:   nextGen,
		RaftStateRel: raftRel,
		RaftDeltaRel: deltaRel,
		SnapshotRel:  snapshotRel,
		UpdatedAt:    time.Now().UnixNano(),
	}
	manifestBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := fp.durableWrite(fp.manifestPath, manifestBytes); err != nil {
		return fmt.Errorf("durable write manifest: %w", err)
	}

	fp.mu.Lock()
	fp.manifest = m
	fp.mu.Unlock()
	return nil
}

// 执行写入操作+文件同步+重命名+目录同步
func (fp *FilePersister) durableWrite(targetPath string, data []byte) error {
	targetDir := filepath.Dir(targetPath)
	tmpPath := filepath.Join(targetDir, fmt.Sprintf(".%s.tmp.%d", filepath.Base(targetPath), time.Now().UnixNano()))

	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync temp file: %w", err)
	}
	atomic.AddInt64(&fp.fsyncCount, 1)
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, targetPath, err)
	}

	if err := fp.syncDir(targetDir); err != nil {
		return fmt.Errorf("sync target dir: %w", err)
	}
	return nil
}

// 用于同步目录元数据
func (fp *FilePersister) syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return err
	}
	atomic.AddInt64(&fp.fsyncCount, 1)
	return nil
}

// 从索引文件加载并校验元数据，若文件异常则触发自动扫描以恢复最新的合法代际视图
func (fp *FilePersister) loadManifest() error {
	raw, err := os.ReadFile(fp.manifestPath)
	// 处理索引文件不存在
	if err != nil {
		if os.IsNotExist(err) {
			fp.manifest = fp.discoverLatestCompleteGeneration()
			if fp.manifest.Generation > 0 && fp.manifest.RaftDeltaRel == "" {
				fp.manifest.RaftDeltaRel = defaultDeltaRel(fp.manifest.Generation)
			}
			return nil
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	var m persisterManifest
	// 处理 JSON 解析失败
	if err := json.Unmarshal(raw, &m); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}

	// 校验元数据完整性
	if m.RaftStateRel == "" || m.SnapshotRel == "" {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}
	// 补全增量日志路径
	if m.RaftDeltaRel == "" {
		m.RaftDeltaRel = defaultDeltaRel(m.Generation)
	}
	// 校验物理文件：Raft 基线状态、快照状态
	if _, err := os.Stat(filepath.Join(fp.dataDir, m.RaftStateRel)); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}
	if _, err := os.Stat(filepath.Join(fp.dataDir, m.SnapshotRel)); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}
	// 校验并修补增量日志文件
	if _, err := os.Stat(filepath.Join(fp.dataDir, m.RaftDeltaRel)); err != nil {
		if err := fp.durableWrite(filepath.Join(fp.dataDir, m.RaftDeltaRel), nil); err != nil {
			fp.manifest = fp.discoverLatestCompleteGeneration()
			return nil
		}
	}

	fp.manifest = m
	return nil
}

// 通过扫描磁盘文件，在索引丢失时手动寻找并拼凑出版本最高且完整的数据集
func (fp *FilePersister) discoverLatestCompleteGeneration() persisterManifest {
	entries, err := os.ReadDir(fp.dataDir)
	if err != nil {
		return persisterManifest{}
	}

	raftByGen := make(map[uint64]string)  // Raft状态文件
	deltaByGen := make(map[uint64]string) // 增量日志(WAL)
	snapByGen := make(map[uint64]string)  // 快照文件

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "raft-state.") && strings.HasSuffix(name, ".bin") {
			gen, ok := parseGeneration(name, "raft-state.", ".bin")
			if ok {
				raftByGen[gen] = name
			}
			continue
		}
		if strings.HasPrefix(name, "raft-state.delta.") && strings.HasSuffix(name, ".wal") {
			gen, ok := parseGeneration(name, "raft-state.delta.", ".wal")
			if ok {
				deltaByGen[gen] = name
			}
			continue
		}
		if strings.HasPrefix(name, "snapshot.") && strings.HasSuffix(name, ".bin") {
			gen, ok := parseGeneration(name, "snapshot.", ".bin")
			if ok {
				snapByGen[gen] = name
			}
		}
	}

	// 找到代际最高的版本
	var best uint64
	for gen := range raftByGen {
		if _, ok := snapByGen[gen]; !ok {
			continue
		}
		if gen > best {
			best = gen
		}
	}

	if best == 0 {
		return persisterManifest{}
	}
	deltaRel := deltaByGen[best]
	if deltaRel == "" {
		deltaRel = defaultDeltaRel(best)
	}

	return persisterManifest{
		Generation:   best,
		RaftStateRel: raftByGen[best],
		RaftDeltaRel: deltaRel,
		SnapshotRel:  snapByGen[best],
		UpdatedAt:    time.Now().UnixNano(),
	}
}

func defaultDeltaRel(gen uint64) string {
	return fmt.Sprintf("raft-state.delta.%020d.wal", gen)
}

// 剥离出文件名中的代际
func parseGeneration(name, prefix, suffix string) (uint64, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}

func (fp *FilePersister) currentRaftStatePathLocked() string {
	if fp.manifest.RaftStateRel != "" {
		return filepath.Join(fp.dataDir, fp.manifest.RaftStateRel)
	}
	return fp.raftStateFile
}

func (fp *FilePersister) currentRaftDeltaPathLocked() string {
	if fp.manifest.RaftDeltaRel != "" {
		return filepath.Join(fp.dataDir, fp.manifest.RaftDeltaRel)
	}
	if fp.manifest.Generation > 0 {
		return filepath.Join(fp.dataDir, defaultDeltaRel(fp.manifest.Generation))
	}
	return ""
}

func (fp *FilePersister) currentSnapshotPathLocked() string {
	if fp.manifest.SnapshotRel != "" {
		return filepath.Join(fp.dataDir, fp.manifest.SnapshotRel)
	}
	return fp.snapshotFile
}

// RaftStateSize 返回 Raft 状态的字节数
func (fp *FilePersister) RaftStateSize() int {
	fp.mu.RLock()
	path := fp.currentRaftStatePathLocked()
	deltaPath := fp.currentRaftDeltaPathLocked()
	defer fp.mu.RUnlock()

	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	total := info.Size()
	if deltaPath != "" {
		if deltaInfo, err := os.Stat(deltaPath); err == nil {
			total += deltaInfo.Size()
		}
	}
	return int(total)
}

// 计算系统性能指标
func (fp *FilePersister) MetricsSnapshot() PersisterMetrics {
	uptime := time.Since(fp.statsStartedAt).Seconds()
	fsyncCount := atomic.LoadInt64(&fp.fsyncCount)
	fsyncPerSecond := 0.0
	if uptime > 0 {
		fsyncPerSecond = float64(fsyncCount) / uptime
	}

	saveBatchCount := atomic.LoadInt64(&fp.saveBatchCount)
	saveBatchItems := atomic.LoadInt64(&fp.saveBatchItems)
	appendBatchCount := atomic.LoadInt64(&fp.appendBatchCount)
	appendBatchItems := atomic.LoadInt64(&fp.appendBatchItems)
	hardBatchCount := atomic.LoadInt64(&fp.hardBatchCount)
	hardBatchItems := atomic.LoadInt64(&fp.hardBatchItems)

	avgBatch := func(items, batches int64) float64 {
		if batches <= 0 {
			return 0
		}
		return float64(items) / float64(batches)
	}

	return PersisterMetrics{
		UptimeSeconds:    uptime,
		FsyncCount:       fsyncCount,
		FsyncPerSecond:   fsyncPerSecond,
		AvgSaveBatch:     avgBatch(saveBatchItems, saveBatchCount),
		AvgAppendBatch:   avgBatch(appendBatchItems, appendBatchCount),
		AvgHardBatch:     avgBatch(hardBatchItems, hardBatchCount),
		SaveBatchCount:   saveBatchCount,
		AppendBatchCount: appendBatchCount,
		HardBatchCount:   hardBatchCount,
	}
}

// Close 清理资源（如果需要）
func (fp *FilePersister) Close() error {
	fp.mu.Lock()
	if fp.closed {
		fp.mu.Unlock()
		return nil
	}
	fp.closed = true
	stopCh, workerDone := fp.stopWorkerLocked()
	fp.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if workerDone != nil {
		<-workerDone
	}
	return nil
}
