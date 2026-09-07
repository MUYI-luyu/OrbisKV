package watch

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Event 键值变化事件
type Event struct {
	Key        string
	OldValue   string
	NewValue   string
	NewVersion int64
	EventType  string
	Timestamp  time.Time
}

// Watcher 订阅者
type Watcher struct {
	ID            int64
	Key           string
	IsPrefix      bool
	Channel       chan Event
	CreatedAt     time.Time
	LastHeartbeat time.Time
}

// Manager Watch管理器，实现发布-订阅模式
// 支持：高并发、背压处理、超时清理、死信队列、统计收集
type Manager struct {
	mu           sync.RWMutex
	watchers     map[int64]*Watcher
	nextID       int64
	eventCh      chan Event
	deadLetterCh chan Event
	done         chan struct{}
	config       ManagerConfig
	stats        *Stats
	processWg    sync.WaitGroup
}

// ManagerConfig 配置
type ManagerConfig struct {
	EventBufferSize   int
	WatcherBufferSize int
	DLQSize           int
	WatcherTimeout    time.Duration
	HealthCheckTick   time.Duration
	EnqueueTimeout    time.Duration
}

// Stats 统计信息
type Stats struct {
	TotalEvents     int64
	SuccessfulSends int64
	FailedSends     int64
	DroppedEvents   int64
	ActiveWatchers  int64
}

// DefaultConfig 默认配置
func DefaultConfig() ManagerConfig {
	return ManagerConfig{
		EventBufferSize:   1000,
		WatcherBufferSize: 100,
		DLQSize:           500,
		WatcherTimeout:    0,
		HealthCheckTick:   10 * time.Second,
		EnqueueTimeout:    500 * time.Millisecond,
	}
}

// NewManager 创建管理器
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.EventBufferSize <= 0 {
		cfg.EventBufferSize = 1000
	}
	if cfg.WatcherBufferSize <= 0 {
		cfg.WatcherBufferSize = 100
	}
	if cfg.DLQSize <= 0 {
		cfg.DLQSize = 500
	}
	if cfg.HealthCheckTick <= 0 {
		cfg.HealthCheckTick = 10 * time.Second
	}
	if cfg.EnqueueTimeout <= 0 {
		cfg.EnqueueTimeout = 500 * time.Millisecond
	}

	m := &Manager{
		watchers:     make(map[int64]*Watcher),
		nextID:       1,
		eventCh:      make(chan Event, cfg.EventBufferSize),
		deadLetterCh: make(chan Event, cfg.DLQSize),
		done:         make(chan struct{}),
		config:       cfg,
		stats:        &Stats{},
	}

	m.processWg.Add(2)
	go m.processLoop()
	go m.healthCheckLoop()
	return m
}

// Subscribe 订阅
func (m *Manager) Subscribe(key string, prefix bool) (*Watcher, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.watchers) >= 10000 {
		return nil, fmt.Errorf("too many watchers")
	}

	id := atomic.AddInt64(&m.nextID, 1)
	w := &Watcher{
		ID:            id,
		Key:           key,
		IsPrefix:      prefix,
		Channel:       make(chan Event, m.config.WatcherBufferSize),
		CreatedAt:     time.Now(),
		LastHeartbeat: time.Now(),
	}

	m.watchers[id] = w
	atomic.AddInt64(&m.stats.ActiveWatchers, 1)
	return w, nil
}

// Unsubscribe 取消订阅
func (m *Manager) Unsubscribe(id int64) error {
	m.mu.Lock()
	w, ok := m.watchers[id]
	delete(m.watchers, id)
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("watcher not found")
	}

	close(w.Channel)
	atomic.AddInt64(&m.stats.ActiveWatchers, -1)
	return nil
}

// Notify 发送通知
func (m *Manager) Notify(key, oldVal, newVal string, ver int64, evType string) error {
	e := Event{
		Key:        key,
		OldValue:   oldVal,
		NewValue:   newVal,
		NewVersion: ver,
		EventType:  evType,
		Timestamp:  time.Now(),
	}

	select {
	case m.eventCh <- e:
		return nil
	case <-m.done:
		return fmt.Errorf("watch manager closed")
	default:
		timer := time.NewTimer(m.config.EnqueueTimeout)
		defer timer.Stop()
		select {
		case m.deadLetterCh <- e:
			return fmt.Errorf("event queued to dlq")
		case m.eventCh <- e:
			return nil
		case <-timer.C:
			atomic.AddInt64(&m.stats.DroppedEvents, 1)
			return fmt.Errorf("enqueue timeout")
		case <-m.done:
			return fmt.Errorf("watch manager closed")
		}
	}
}

// processLoop 事件处理循环
func (m *Manager) processLoop() {
	defer m.processWg.Done()

	for {
		select {
		case e := <-m.eventCh:
			atomic.AddInt64(&m.stats.TotalEvents, 1)
			m.distributeEvent(e)
		case e := <-m.deadLetterCh:
			atomic.AddInt64(&m.stats.TotalEvents, 1)
			m.distributeEvent(e)
		case <-m.done:
			return
		}
	}
}

// distributeEvent 分发事件到订阅者
func (m *Manager) distributeEvent(e Event) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, w := range m.watchers {
		if !m.matches(w, e.Key) {
			continue
		}

		// 非阻塞发送
		select {
		case w.Channel <- e:
			atomic.AddInt64(&m.stats.SuccessfulSends, 1)
			w.LastHeartbeat = time.Now()
		default:
			atomic.AddInt64(&m.stats.FailedSends, 1)
		}
	}
}

// matches 检查是否匹配
func (m *Manager) matches(w *Watcher, key string) bool {
	if w.IsPrefix {
		return len(key) >= len(w.Key) && key[:len(w.Key)] == w.Key
	}
	return w.Key == key
}

// healthCheckLoop 健康检查循环
func (m *Manager) healthCheckLoop() {
	defer m.processWg.Done()

	ticker := time.NewTicker(m.config.HealthCheckTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupExpired()
		case <-m.done:
			return
		}
	}
}

// cleanupExpired 清理过期订阅者
func (m *Manager) cleanupExpired() {
	if m.config.WatcherTimeout <= 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var toDelete []int64

	for id, w := range m.watchers {
		if now.Sub(w.LastHeartbeat) > m.config.WatcherTimeout {
			toDelete = append(toDelete, id)
		}
	}

	for _, id := range toDelete {
		if w, ok := m.watchers[id]; ok {
			close(w.Channel)
			delete(m.watchers, id)
			atomic.AddInt64(&m.stats.ActiveWatchers, -1)
		}
	}
}

// GetStats 获取统计
func (m *Manager) GetStats() Stats {
	return Stats{
		TotalEvents:     atomic.LoadInt64(&m.stats.TotalEvents),
		SuccessfulSends: atomic.LoadInt64(&m.stats.SuccessfulSends),
		FailedSends:     atomic.LoadInt64(&m.stats.FailedSends),
		DroppedEvents:   atomic.LoadInt64(&m.stats.DroppedEvents),
		ActiveWatchers:  atomic.LoadInt64(&m.stats.ActiveWatchers),
	}
}

// GetWatcherCount 获取订阅者数量
func (m *Manager) GetWatcherCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.watchers)
}

// Close 关闭
func (m *Manager) Close() {
	close(m.done)

	// 等待所有协程完成
	m.processWg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, w := range m.watchers {
		close(w.Channel)
	}
	m.watchers = make(map[int64]*Watcher)
}
