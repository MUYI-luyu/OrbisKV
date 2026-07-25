package storage

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// kvEntry 代表存储中的一个键值对条目
type kvEntry struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Expires int64  `json:"expires"`
}

// Tx 记录 key 前缀 — 将事务元数据与用户数据隔离开。
const (
	txKeyPrefix = "_tx:"
)

// WriteBatchOp 描述单次 WriteBatchWithCAS 中的一个原子写操作。
type WriteBatchOp struct {
	Key             string
	Value           string
	ExpectedVersion uint64
	Expires         int64
	IsDelete        bool
}

// Store 是基于BadgerDB的持久化存储实现
type Store struct {
	db     *badger.DB
	dbPath string

	// TTL 最小堆：按过期时间排序，避免全表扫描
	expiryHeap expiryHeap
	expiryMu   sync.Mutex
	tombstones map[string]bool // 已删除的 key，在堆顶弹出时跳过
}

// expiryItem 是 TTL 堆中的一个条目
type expiryItem struct {
	key     string
	expires int64
}

// expiryHeap 实现 container/heap.Interface，按 expires 升序排列（最小堆）
type expiryHeap []expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expires < h[j].expires }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *expiryHeap) Push(x any) {
	*h = append(*h, x.(expiryItem))
}

func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type PutCASStatus int

const (
	PutCASOK PutCASStatus = iota
	PutCASNoKey
	PutCASVersionMismatch
)

func badgerOptions(dbPath string) badger.Options {
	return badger.DefaultOptions(dbPath).WithLoggingLevel(badger.WARNING)
}

func openBadger(dbPath string) (*badger.DB, error) {
	return badger.Open(badgerOptions(dbPath))
}

// NewStore 创建一个新的Store实例
func NewStore(dbPath string) (*Store, error) {
	// 确保目录存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("创建数据库目录失败: %w", err)
		}
	}

	// 打开BadgerDB数据库
	db, err := openBadger(dbPath)
	if err != nil {
		return nil, fmt.Errorf("打开BadgerDB失败: %w", err)
	}

	return &Store{
		db:         db,
		dbPath:     dbPath,
		tombstones: make(map[string]bool),
		expiryHeap: make(expiryHeap, 0),
	}, nil
}

// trackExpiry 将带有 TTL 的 key 推入过期堆
func (s *Store) trackExpiry(key string, expires int64) {
	if expires <= 0 {
		return
	}
	s.expiryMu.Lock()
	delete(s.tombstones, key) // 清除可能存在的 tombstone
	heap.Push(&s.expiryHeap, expiryItem{key: key, expires: expires})
	s.expiryMu.Unlock()
}

// markTombstone 标记 key 为已删除，当它出现在堆顶时将被跳过
func (s *Store) markTombstone(key string) {
	s.expiryMu.Lock()
	s.tombstones[key] = true
	s.expiryMu.Unlock()
}

// Get 返回一个键的值、版本号和绝对过期时间戳。
func (s *Store) Get(key string) (value string, version uint64, expires int64, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		var entry kvEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}

		value = entry.Value
		version = entry.Version
		expires = entry.Expires
		exists = true
		return nil
	})

	return
}

// Put stores key/value/version with optional absolute expiry unix nano.
func (s *Store) Put(key, value string, version uint64, expires int64) error {
	entry := kvEntry{
		Value:   value,
		Version: version,
		Expires: expires,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// PutCASWithTTL performs version-check and write in a single transaction.
func (s *Store) PutCASWithTTL(key, value string, expectedVersion uint64, expires int64) (oldValue string, status PutCASStatus, err error) {
	status = PutCASOK
	err = s.db.Update(func(txn *badger.Txn) error {
		item, getErr := txn.Get([]byte(key))
		if getErr == badger.ErrKeyNotFound {
			if expectedVersion != 0 {
				status = PutCASNoKey
				return nil
			}
			entry := kvEntry{Value: value, Version: 1, Expires: expires}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return fmt.Errorf("序列化失败: %w", marshalErr)
			}
			return txn.Set([]byte(key), data)
		}
		if getErr != nil {
			return getErr
		}

		raw, copyErr := item.ValueCopy(nil)
		if copyErr != nil {
			return copyErr
		}
		var cur kvEntry
		if unmarshalErr := json.Unmarshal(raw, &cur); unmarshalErr != nil {
			return unmarshalErr
		}

		now := time.Now().UnixNano()
		exists := !(cur.Expires > 0 && cur.Expires <= now)
		if !exists {
			if expectedVersion != 0 {
				status = PutCASNoKey
				return nil
			}
			entry := kvEntry{Value: value, Version: 1, Expires: expires}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return fmt.Errorf("序列化失败: %w", marshalErr)
			}
			return txn.Set([]byte(key), data)
		}

		if cur.Version != expectedVersion {
			status = PutCASVersionMismatch
			return nil
		}

		oldValue = cur.Value
		entry := kvEntry{Value: value, Version: cur.Version + 1, Expires: expires}
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			return fmt.Errorf("序列化失败: %w", marshalErr)
		}
		return txn.Set([]byte(key), data)
	})

	// 写入成功后，将 key 加入 TTL 堆（用于高效过期扫描）
	if err == nil && status == PutCASOK && expires > 0 {
		s.trackExpiry(key, expires)
	}
	return oldValue, status, err
}

// writeBatchWithCASInTxn 在给定的 BadgerDB 事务中执行 CAS 批量写操作。
// 每条 op 在 txn 内先 Get 当前版本，比对 ExpectedVersion，再 Set。
// 任何一个冲突就返回 error 导致整个事务回滚。
func (s *Store) writeBatchWithCASInTxn(txn *badger.Txn, ops []WriteBatchOp) error {
	for _, op := range ops {
		if op.IsDelete {
			item, err := txn.Get([]byte(op.Key))
			if err == badger.ErrKeyNotFound {
				if op.ExpectedVersion != 0 {
					return fmt.Errorf("WriteBatchWithCAS: key %q not found, expected version %d", op.Key, op.ExpectedVersion)
				}
				continue
			}
			if err != nil {
				return err
			}
			raw, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var cur kvEntry
			if err := json.Unmarshal(raw, &cur); err != nil {
				return err
			}
			if cur.Version != op.ExpectedVersion {
				return fmt.Errorf("WriteBatchWithCAS: version mismatch for key %q, expected %d, got %d", op.Key, op.ExpectedVersion, cur.Version)
			}
			if err := txn.Delete([]byte(op.Key)); err != nil {
				return err
			}
		} else {
			item, getErr := txn.Get([]byte(op.Key))
			if getErr == badger.ErrKeyNotFound {
				if op.ExpectedVersion != 0 {
					return fmt.Errorf("WriteBatchWithCAS: key %q not found, expected version %d", op.Key, op.ExpectedVersion)
				}
				entry := kvEntry{Value: op.Value, Version: 1, Expires: op.Expires}
				data, marshalErr := json.Marshal(entry)
				if marshalErr != nil {
					return fmt.Errorf("WriteBatchWithCAS: marshal failed: %w", marshalErr)
				}
				if err := txn.Set([]byte(op.Key), data); err != nil {
					return err
				}
				continue
			}
			if getErr != nil {
				return getErr
			}
			raw, copyErr := item.ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			var cur kvEntry
			if unmarshalErr := json.Unmarshal(raw, &cur); unmarshalErr != nil {
				return unmarshalErr
			}
			if cur.Version != op.ExpectedVersion {
				return fmt.Errorf("WriteBatchWithCAS: version mismatch for key %q, expected %d, got %d", op.Key, op.ExpectedVersion, cur.Version)
			}
			entry := kvEntry{Value: op.Value, Version: cur.Version + 1, Expires: op.Expires}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return fmt.Errorf("WriteBatchWithCAS: marshal failed: %w", marshalErr)
			}
			if err := txn.Set([]byte(op.Key), data); err != nil {
				return err
			}
		}
	}
	return nil
}

// WriteBatchWithCAS 在单个 BadgerDB 事务中原子执行多条 CAS 写操作。
// 每条 op 在 txn 内先 Get 当前版本，比对 ExpectedVersion，再 Set。
// 任何一个冲突就整体回滚。
func (s *Store) WriteBatchWithCAS(ops []WriteBatchOp) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return s.writeBatchWithCASInTxn(txn, ops)
	})
}

// WriteBatchWithCASAndRecord 在单个 BadgerDB 事务中原子执行 CAS 批量写入
// 并同时持久化一条 tx 记录（如 commit/abort 标记）。两者要么一起落盘，
// 要么一起回滚，消除崩溃窗口——防止用户数据已写入但 commit 记录丢失
// 导致 Raft 重放时版本冲突、事务永久卡死。
func (s *Store) WriteBatchWithCASAndRecord(ops []WriteBatchOp, recordKey string, recordData []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := s.writeBatchWithCASInTxn(txn, ops); err != nil {
			return err
		}
		return txn.Set([]byte(txKeyPrefix+recordKey), recordData)
	})
}

// PutTxRecord 在 _tx: 前缀的 key 下写入原始字节。
func (s *Store) PutTxRecord(txKey string, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(txKeyPrefix+txKey), data)
	})
}

// GetTxRecord 从 _tx: 前缀的 key 读取原始字节。
func (s *Store) GetTxRecord(txKey string) ([]byte, bool, error) {
	var val []byte
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(txKeyPrefix + txKey))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return val, found, err
}

// DeleteTxRecord 删除一个 _tx: 前缀的 key。
func (s *Store) DeleteTxRecord(txKey string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(txKeyPrefix + txKey))
	})
}

// ScanTxRecordsByPrefix 扫描 _tx: 前缀且匹配给定子前缀的 key。
func (s *Store) ScanTxRecordsByPrefix(subPrefix string) (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = []byte(txKeyPrefix + subPrefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			result[key] = val
		}
		return nil
	})
	return result, err
}

// PeekExpiredKeys 从 TTL 堆中获取所有过期时间 <= cutoff 的键（最多 limit 个）。
// 相比全表扫描 GetExpiredKeys，此方法复杂度为 O(K log N)，其中 K 为过期键数。
func (s *Store) PeekExpiredKeys(cutoff int64, limit int) []string {
	if limit <= 0 {
		limit = 128
	}
	s.expiryMu.Lock()
	defer s.expiryMu.Unlock()

	keys := make([]string, 0, limit)
	for s.expiryHeap.Len() > 0 && len(keys) < limit {
		top := s.expiryHeap[0]
		if top.expires > cutoff {
			break // 堆顶未过期，后续都未过期
		}
		heap.Pop(&s.expiryHeap)

		// 跳过已删除的 key（tombstone）
		if s.tombstones[top.key] {
			delete(s.tombstones, top.key)
			continue
		}
		keys = append(keys, top.key)
	}
	return keys
}

// GetExpiredKeys 保留原有签名，内部委托给堆实现以获取 O(K log N) 性能。
// 如果堆为空（例如冷启动时尚未重建），退化为全表扫描。
func (s *Store) GetExpiredKeys(cutoff int64, limit int) ([]string, error) {
	// 优先使用堆
	if s.expiryHeap.Len() > 0 {
		return s.PeekExpiredKeys(cutoff, limit), nil
	}
	// 冷启动退化为全表扫描
	if limit <= 0 {
		limit = 128
	}
	keys := make([]string, 0, limit)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if len(keys) >= limit {
				break
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				continue
			}
			if entry.Expires > 0 && entry.Expires <= cutoff {
				keys = append(keys, string(item.Key()))
			}
		}
		return nil
	})
	return keys, err
}

// RebuildExpiryHeap 从 BadgerDB 全量重建 TTL 堆（用于启动和快照恢复后）。
func (s *Store) RebuildExpiryHeap() error {
	s.expiryMu.Lock()
	defer s.expiryMu.Unlock()

	s.expiryHeap = make(expiryHeap, 0)
	s.tombstones = make(map[string]bool)

	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				continue
			}
			if entry.Expires > 0 {
				heap.Push(&s.expiryHeap, expiryItem{
					key:     string(item.Key()),
					expires: entry.Expires,
				})
			}
		}
		return nil
	})
}

// ScanPrefix 使用 Badger 前缀迭代器扫描匹配前缀的键值对。
// 相比 GetAll() + 应用层过滤，此方法仅访问匹配前缀的键，复杂度为 O(匹配键数)。
// limit <= 0 表示不限制。
func (s *Store) ScanPrefix(prefix string, limit int) ([]kvEntry, []string, error) {
	entries := make([]kvEntry, 0)
	keys := make([]string, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		if prefix != "" {
			opts.Prefix = []byte(prefix)
		}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if limit > 0 && len(keys) >= limit {
				break
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				continue
			}
			entries = append(entries, entry)
			keys = append(keys, string(item.Key()))
		}
		return nil
	})
	return entries, keys, err
}

// Delete 从存储中删除一个键，并标记 tombstone 以在 TTL 堆中跳过
func (s *Store) Delete(key string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err == nil {
		s.markTombstone(key)
	}
	return err
}

// GetAll 获取所有键值对（用于加载快照或导出）
func (s *Store) GetAll() (map[string]kvEntry, error) {
	data := make(map[string]kvEntry)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				return err
			}

			data[key] = entry
		}
		return nil
	})

	return data, err
}

// LoadSnapshot 从编码的快照数据中恢复存储
func (s *Store) LoadSnapshot(snapshotData []byte) error {
	var snapshot map[string]kvEntry
	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		return fmt.Errorf("反序列化快照失败: %w", err)
	}

	// 创建临时库
	ts := time.Now().UnixNano()
	tmpPath := fmt.Sprintf("%s.restore.%d", s.dbPath, ts)
	backupPath := fmt.Sprintf("%s.backup.%d", s.dbPath, ts)

	if err := os.RemoveAll(tmpPath); err != nil {
		return fmt.Errorf("清理临时恢复目录失败: %w", err)
	}

	tmpDB, err := openBadger(tmpPath)
	if err != nil {
		return fmt.Errorf("打开临时恢复数据库失败: %w", err)
	}

	// 遍历快照中的每一条数据，序列化后写入到 tmpDB 中
	for key, entry := range snapshot {
		entryCopy := entry
		data, marshalErr := json.Marshal(entryCopy)
		if marshalErr != nil {
			_ = tmpDB.Close()
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("序列化快照条目失败: %w", marshalErr)
		}
		if putErr := tmpDB.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), data)
		}); putErr != nil {
			_ = tmpDB.Close()
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("写入临时恢复数据库失败: %w", putErr)
		}
	}

	if err := tmpDB.Close(); err != nil {
		_ = os.RemoveAll(tmpPath)
		return fmt.Errorf("关闭临时恢复数据库失败: %w", err)
	}

	// 关闭当前正在运行的旧数据库
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("关闭当前数据库失败: %w", err)
		}
		s.db = nil
	}

	if err := os.RemoveAll(backupPath); err != nil {
		_ = os.RemoveAll(tmpPath)
		return fmt.Errorf("清理旧备份目录失败: %w", err)
	}

	// 备份旧数据
	if _, err := os.Stat(s.dbPath); err == nil {
		if err := os.Rename(s.dbPath, backupPath); err != nil {
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("备份当前数据库失败: %w", err)
		}
	}

	// 切换新库。将装满快照数据的 tmpPath 改名为正式的 dbPath
	if err := os.Rename(tmpPath, s.dbPath); err != nil {
		_ = os.Rename(backupPath, s.dbPath)
		return fmt.Errorf("切换恢复数据库失败: %w", err)
	}

	newDB, err := openBadger(s.dbPath)
	if err != nil {
		_ = os.RemoveAll(s.dbPath)
		_ = os.Rename(backupPath, s.dbPath) // 失败回滚
		fallbackDB, fallbackErr := openBadger(s.dbPath)
		if fallbackErr == nil {
			s.db = fallbackDB
		}
		return fmt.Errorf("恢复后重新打开数据库失败: %w", err)
	}

	s.db = newDB
	_ = os.RemoveAll(backupPath)
	return nil
}

// SaveSnapshot 将当前存储导出为快照数据
func (s *Store) SaveSnapshot() ([]byte, error) {
	data, err := s.GetAll()
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

// Close 关闭数据库连接
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// GetStats 获取数据库统计信息
func (s *Store) GetStats() string {
	return "BadgerDB storage"
}

// Clear 清空所有数据（用于测试）
func (s *Store) Clear() error {
	return s.db.DropAll()
}

// DBPath 返回数据库路径
func (s *Store) DBPath() string {
	return s.dbPath
}

// DataSize 返回数据库大小（字节）
func (s *Store) DataSize() (int64, error) {
	var size int64
	err := filepath.Walk(s.dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
