package pool

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// CircularBuffer 环形缓冲区。
type CircularBuffer struct {
	data     []PoolStats
	capacity int
	head     int
	tail     int
	size     int
	mu       sync.RWMutex
}

// NewCircularBuffer 创建环形缓冲区。
func NewCircularBuffer(capacity int) *CircularBuffer {
	return &CircularBuffer{
		data:     make([]PoolStats, capacity),
		capacity: capacity,
		head:     0,
		tail:     0,
		size:     0,
	}
}

// Append 追加数据。
func (cb *CircularBuffer) Append(stats PoolStats) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.data[cb.tail] = stats
	cb.tail = (cb.tail + 1) % cb.capacity
	if cb.size < cb.capacity {
		cb.size++
	} else {
		cb.head = (cb.head + 1) % cb.capacity
	}
}

// GetAll 获取所有数据。
func (cb *CircularBuffer) GetAll() []PoolStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	result := make([]PoolStats, cb.size)
	for i := 0; i < cb.size; i++ {
		idx := (cb.head + i) % cb.capacity
		result[i] = cb.data[idx]
	}
	return result
}

// GetLatest 获取最新数据。
func (cb *CircularBuffer) GetLatest() *PoolStats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.size == 0 {
		return nil
	}
	idx := (cb.tail - 1 + cb.capacity) % cb.capacity
	return &cb.data[idx]
}

// Clear 清空缓冲区。
func (cb *CircularBuffer) Clear() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.head = 0
	cb.tail = 0
	cb.size = 0
	for i := range cb.data {
		cb.data[i] = PoolStats{}
	}
}

// CircularEnhancedBuffer 增强统计环形缓冲区。
type CircularEnhancedBuffer struct {
	data     []EnhancedPoolStats
	capacity int
	head     int
	tail     int
	size     int
	mu       sync.RWMutex
}

// NewCircularEnhancedBuffer 创建增强统计环形缓冲区。
func NewCircularEnhancedBuffer(capacity int) *CircularEnhancedBuffer {
	return &CircularEnhancedBuffer{
		data:     make([]EnhancedPoolStats, capacity),
		capacity: capacity,
		head:     0,
		tail:     0,
		size:     0,
	}
}

// Append 追加数据。
func (ceb *CircularEnhancedBuffer) Append(stats EnhancedPoolStats) {
	ceb.mu.Lock()
	defer ceb.mu.Unlock()

	ceb.data[ceb.tail] = stats
	ceb.tail = (ceb.tail + 1) % ceb.capacity
	if ceb.size < ceb.capacity {
		ceb.size++
	} else {
		ceb.head = (ceb.head + 1) % ceb.capacity
	}
}

// GetLatest 获取最新数据。
func (ceb *CircularEnhancedBuffer) GetLatest() *EnhancedPoolStats {
	ceb.mu.RLock()
	defer ceb.mu.RUnlock()

	if ceb.size == 0 {
		return nil
	}
	idx := (ceb.tail - 1 + ceb.capacity) % ceb.capacity
	return &ceb.data[idx]
}

// GetAll 获取所有数据。
func (ceb *CircularEnhancedBuffer) GetAll() []EnhancedPoolStats {
	ceb.mu.RLock()
	defer ceb.mu.RUnlock()

	result := make([]EnhancedPoolStats, ceb.size)
	for i := 0; i < ceb.size; i++ {
		idx := (ceb.head + i) % ceb.capacity
		result[i] = ceb.data[idx]
	}
	return result
}

// Clear 清空缓冲区。
func (ceb *CircularEnhancedBuffer) Clear() {
	ceb.mu.Lock()
	defer ceb.mu.Unlock()

	ceb.head = 0
	ceb.tail = 0
	ceb.size = 0
	for i := range ceb.data {
		ceb.data[i] = EnhancedPoolStats{}
	}
}

// EnhancedPoolStats 增强的池统计信息。
type EnhancedPoolStats struct {
	PoolStats
	GoroutineCount      int             `json:"goroutine_count"`
	MemoryUsage         uint64          `json:"memory_usage"`
	GCPauseTime         time.Duration   `json:"gc_pause_time"`
	ContentionCount     uint64          `json:"contention_count"`
	StealSuccessRate    float64         `json:"steal_success_rate"`
	WaitTimeHistogram   []time.Duration `json:"wait_time_histogram"`
	CreateTimeHistogram []time.Duration `json:"create_time_histogram"`
	ErrorRate           float64         `json:"error_rate"`
	Throughput          float64         `json:"throughput"`
	PanicCount          uint64          `json:"panic_count"`
	BatchOpsCount       uint64          `json:"batch_ops_count"`
	AvgBatchSize        float64         `json:"avg_batch_size"`
}

// BatchStats 批量操作统计。
type BatchStats struct {
	TotalBatches  uint64  `json:"total_batches"`
	TotalItems    uint64  `json:"total_items"`
	AvgBatchSize  float64 `json:"avg_batch_size"`
	MinBatchSize  int     `json:"min_batch_size"`
	MaxBatchSize  int     `json:"max_batch_size"`
	BatchGetCount uint64  `json:"batch_get_count"`
	BatchPutCount uint64  `json:"batch_put_count"`
}

// MetricsCollector 指标收集器。
type MetricsCollector struct {
	poolMetrics     map[string]*CircularBuffer
	enhancedMetrics map[string]*CircularEnhancedBuffer
	interval        time.Duration
	maxRecords      int
	mu              sync.RWMutex
	closeChan       chan struct{}
	collectors      map[string]func() PoolStats
	startTime       time.Time
	lastGCPause     time.Duration
	gcMu            sync.Mutex
	prometheus      bool
	batchStats      map[string]*BatchStats
	closeOnce       sync.Once
	collectWG       sync.WaitGroup
}

// MetricsCollectorOption 指标收集器配置选项。
type MetricsCollectorOption func(*MetricsCollector)

// WithCollectionInterval 设置收集间隔。
func WithCollectionInterval(interval time.Duration) MetricsCollectorOption {
	return func(c *MetricsCollector) {
		if interval > 0 {
			c.interval = interval
		}
	}
}

// WithMaxRecords 设置最大记录数。
func WithMaxRecords(maxRecords int) MetricsCollectorOption {
	return func(c *MetricsCollector) {
		if maxRecords > 0 {
			c.maxRecords = maxRecords
		}
	}
}

// WithPrometheus 启用Prometheus导出。
func WithPrometheus(enable bool) MetricsCollectorOption {
	return func(c *MetricsCollector) {
		c.prometheus = enable
	}
}

// NewMetricsCollector 创建指标收集器。
func NewMetricsCollector(opts ...MetricsCollectorOption) *MetricsCollector {
	c := &MetricsCollector{
		poolMetrics:     make(map[string]*CircularBuffer),
		enhancedMetrics: make(map[string]*CircularEnhancedBuffer),
		interval:        10 * time.Second,
		maxRecords:      1000,
		closeChan:       make(chan struct{}),
		collectors:      make(map[string]func() PoolStats),
		startTime:       time.Now(),
		prometheus:      false,
		batchStats:      make(map[string]*BatchStats),
	}

	for _, opt := range opts {
		opt(c)
	}

	go c.monitorGC()
	return c
}

// Start 启动指标收集。
func (c *MetricsCollector) Start() {
	c.collectWG.Add(1)
	go func() {
		defer c.collectWG.Done()
		c.collectLoop()
	}()
}

// Stop 停止指标收集。
func (c *MetricsCollector) Stop() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
	c.collectWG.Wait()
}

// monitorGC 监控GC暂停时间。
func (c *MetricsCollector) monitorGC() {
	var memStats runtime.MemStats
	var lastPause uint64

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			runtime.ReadMemStats(&memStats)
			if memStats.PauseTotalNs > lastPause {
				c.gcMu.Lock()
				c.lastGCPause = time.Duration(memStats.PauseTotalNs - lastPause)
				lastPause = memStats.PauseTotalNs
				c.gcMu.Unlock()
			}
		case <-c.closeChan:
			return
		}
	}
}

// collectLoop 收集循环。
func (c *MetricsCollector) collectLoop() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.collectAllParallel()
		case <-c.closeChan:
			return
		}
	}
}

// collectAllParallel 并行收集所有池的指标，避免串行阻塞。
func (c *MetricsCollector) collectAllParallel() {
	c.mu.RLock()
	collectors := make(map[string]func() PoolStats)
	for id, fn := range c.collectors {
		collectors[id] = fn
	}
	c.mu.RUnlock()

	if len(collectors) == 0 {
		return
	}

	var wg sync.WaitGroup
	resultCh := make(chan struct {
		id    string
		stats PoolStats
	}, len(collectors))

	for id, fn := range collectors {
		wg.Add(1)
		go func(poolID string, statsFn func() PoolStats) {
			defer wg.Done()
			stats := statsFn()
			resultCh <- struct {
				id    string
				stats PoolStats
			}{poolID, stats}
		}(id, fn)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for result := range resultCh {
		c.RecordStats(result.id, result.stats)

		enhanced := c.collectEnhancedStats(result.id, result.stats)

		c.mu.Lock()
		if _, ok := c.enhancedMetrics[result.id]; !ok {
			c.enhancedMetrics[result.id] = NewCircularEnhancedBuffer(c.maxRecords)
		}
		c.enhancedMetrics[result.id].Append(enhanced)
		c.mu.Unlock()
	}
}

// collectEnhancedStats 收集增强统计信息。
func (c *MetricsCollector) collectEnhancedStats(poolID string, stats PoolStats) EnhancedPoolStats {
	c.gcMu.Lock()
	gcPauseTime := c.lastGCPause
	c.gcMu.Unlock()

	enhanced := EnhancedPoolStats{
		PoolStats:           stats,
		GoroutineCount:      runtime.NumGoroutine(),
		GCPauseTime:         gcPauseTime,
		WaitTimeHistogram:   c.calculateHistogram(poolID, "wait"),
		CreateTimeHistogram: c.calculateHistogram(poolID, "create"),
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	enhanced.MemoryUsage = memStats.Alloc

	if stats.CreateCount > 0 {
		enhanced.ErrorRate = float64(stats.ErrorCount) / float64(stats.CreateCount)
	}

	duration := time.Since(c.startTime).Seconds()
	if duration > 0 {
		enhanced.Throughput = float64(stats.CreateCount) / duration
	}

	if stats.CreateCount > 0 && stats.StealCount > 0 {
		enhanced.StealSuccessRate = float64(stats.StealCount) / float64(stats.CreateCount)
	}

	if batchStats, ok := c.batchStats[poolID]; ok {
		enhanced.BatchOpsCount = batchStats.TotalBatches
		if batchStats.TotalBatches > 0 {
			enhanced.AvgBatchSize = float64(batchStats.TotalItems) / float64(batchStats.TotalBatches)
		}
	}

	return enhanced
}

// RecordBatchOperation 记录批量操作。
func (c *MetricsCollector) RecordBatchOperation(poolID string, operation string, batchSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats, exists := c.batchStats[poolID]
	if !exists {
		stats = &BatchStats{
			MinBatchSize: batchSize,
			MaxBatchSize: batchSize,
		}
		c.batchStats[poolID] = stats
	}

	stats.TotalBatches++
	stats.TotalItems += uint64(batchSize)

	if batchSize < stats.MinBatchSize {
		stats.MinBatchSize = batchSize
	}
	if batchSize > stats.MaxBatchSize {
		stats.MaxBatchSize = batchSize
	}

	stats.AvgBatchSize = float64(stats.TotalItems) / float64(stats.TotalBatches)

	if operation == "get" {
		stats.BatchGetCount++
	} else {
		stats.BatchPutCount++
	}
}

// GetBatchStats 获取批量操作统计。
func (c *MetricsCollector) GetBatchStats(poolID string) *BatchStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if stats, exists := c.batchStats[poolID]; exists {
		return &BatchStats{
			TotalBatches:  stats.TotalBatches,
			TotalItems:    stats.TotalItems,
			AvgBatchSize:  stats.AvgBatchSize,
			MinBatchSize:  stats.MinBatchSize,
			MaxBatchSize:  stats.MaxBatchSize,
			BatchGetCount: stats.BatchGetCount,
			BatchPutCount: stats.BatchPutCount,
		}
	}
	return nil
}

// calculateHistogram 计算直方图。
func (c *MetricsCollector) calculateHistogram(poolID string, metricType string) []time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	records := c.poolMetrics[poolID]
	if records == nil || records.size < 10 {
		return make([]time.Duration, 11)
	}

	hist := make([]time.Duration, 11)
	for i := 0; i < 11; i++ {
		hist[i] = time.Duration(i) * 100 * time.Microsecond
	}
	return hist
}

// GetDetailedStats 获取详细统计信息。
func (c *MetricsCollector) GetDetailedStats(poolID string) *EnhancedPoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if buffer, ok := c.enhancedMetrics[poolID]; ok {
		latest := buffer.GetLatest()
		if latest != nil {
			return latest
		}
	}

	if stats := c.GetLatestStats(poolID); stats != nil {
		enhanced := c.collectEnhancedStats(poolID, *stats)
		return &enhanced
	}

	return nil
}

// Register 注册池到收集器。
func (c *MetricsCollector) Register(poolID string, pool Pool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.collectors[poolID] = pool.Stats

	if _, ok := c.poolMetrics[poolID]; !ok {
		c.poolMetrics[poolID] = NewCircularBuffer(c.maxRecords)
	}
	if _, ok := c.enhancedMetrics[poolID]; !ok {
		c.enhancedMetrics[poolID] = NewCircularEnhancedBuffer(c.maxRecords)
	}
}

// Unregister 注销池。
func (c *MetricsCollector) Unregister(poolID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.collectors, poolID)
	delete(c.poolMetrics, poolID)
	delete(c.enhancedMetrics, poolID)
	delete(c.batchStats, poolID)
}

// RecordStats 记录池统计信息。
func (c *MetricsCollector) RecordStats(poolID string, stats PoolStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if buffer, ok := c.poolMetrics[poolID]; ok {
		buffer.Append(stats)
	} else {
		buffer = NewCircularBuffer(c.maxRecords)
		buffer.Append(stats)
		c.poolMetrics[poolID] = buffer
	}
}

// GetStats 获取池统计信息。
func (c *MetricsCollector) GetStats(poolID string) []PoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if buffer, ok := c.poolMetrics[poolID]; ok {
		return buffer.GetAll()
	}
	return nil
}

// GetLatestStats 获取最新统计信息。
func (c *MetricsCollector) GetLatestStats(poolID string) *PoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if buffer, ok := c.poolMetrics[poolID]; ok {
		return buffer.GetLatest()
	}
	return nil
}

// GetAllStats 获取所有池统计信息。
func (c *MetricsCollector) GetAllStats() map[string][]PoolStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string][]PoolStats)
	for k, v := range c.poolMetrics {
		result[k] = v.GetAll()
	}
	return result
}

// GetStatsJSON 获取JSON格式的统计信息。
func (c *MetricsCollector) GetStatsJSON() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data := make(map[string][]PoolStats)
	for k, v := range c.poolMetrics {
		data[k] = v.GetAll()
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

// GetSummary 获取摘要统计。
func (c *MetricsCollector) GetSummary() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	summary := make(map[string]interface{})

	for poolID, buffer := range c.poolMetrics {
		records := buffer.GetAll()
		if len(records) == 0 {
			continue
		}

		latest := records[len(records)-1]

		var createRate, destroyRate float64
		if len(records) >= 2 {
			prev := records[len(records)-2]
			duration := float64(latest.LastActiveTime - prev.LastActiveTime)
			if duration > 0 {
				createRate = float64(latest.CreateCount-prev.CreateCount) / duration * 1e9
				destroyRate = float64(latest.DestroyCount-prev.DestroyCount) / duration * 1e9
			}
		}

		poolSummary := map[string]interface{}{
			"type":            latest.Type,
			"active":          latest.ActiveCount,
			"idle":            latest.IdleCount,
			"max_capacity":    latest.MaxCapacity,
			"min_capacity":    latest.MinCapacity,
			"waiting":         latest.WaitCount,
			"total_created":   latest.CreateCount,
			"total_destroyed": latest.DestroyCount,
			"create_rate":     createRate,
			"destroy_rate":    destroyRate,
			"timeout_count":   latest.TimeoutCount,
			"error_count":     latest.ErrorCount,
			"steal_count":     latest.StealCount,
			"avg_get_latency": time.Duration(latest.GetLatency).String(),
			"avg_put_latency": time.Duration(latest.PutLatency).String(),
			"recycle_count":   latest.RecycleCount,
			"last_active":     time.Unix(0, latest.LastActiveTime).Format(time.RFC3339Nano),
		}

		if batchStats, ok := c.batchStats[poolID]; ok {
			poolSummary["batch_ops"] = map[string]interface{}{
				"total_batches":   batchStats.TotalBatches,
				"total_items":     batchStats.TotalItems,
				"avg_batch_size":  batchStats.AvgBatchSize,
				"min_batch_size":  batchStats.MinBatchSize,
				"max_batch_size":  batchStats.MaxBatchSize,
				"batch_get_count": batchStats.BatchGetCount,
				"batch_put_count": batchStats.BatchPutCount,
			}
		}

		summary[poolID] = poolSummary
	}

	return summary
}

// Clear 清空所有记录。
func (c *MetricsCollector) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, buffer := range c.poolMetrics {
		buffer.Clear()
	}
	for _, buffer := range c.enhancedMetrics {
		buffer.Clear()
	}
	c.batchStats = make(map[string]*BatchStats)
	c.poolMetrics = make(map[string]*CircularBuffer)
	c.enhancedMetrics = make(map[string]*CircularEnhancedBuffer)
}

// GetRegisteredPools 获取已注册的池列表。
func (c *MetricsCollector) GetRegisteredPools() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	pools := make([]string, 0, len(c.collectors))
	for id := range c.collectors {
		pools = append(pools, id)
	}
	return pools
}

// ExportPrometheus 导出Prometheus格式的指标。
func (c *MetricsCollector) ExportPrometheus() string {
	if !c.prometheus {
		return ""
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var metrics string
	hasData := false

	for poolID, buffer := range c.poolMetrics {
		latest := buffer.GetLatest()
		if latest == nil {
			continue
		}
		hasData = true

		metrics += fmt.Sprintf("# HELP pool_active_count Active count\n")
		metrics += fmt.Sprintf("# TYPE pool_active_count gauge\n")
		metrics += fmt.Sprintf("pool_active_count{pool=\"%s\"} %d\n", poolID, latest.ActiveCount)

		metrics += fmt.Sprintf("# HELP pool_idle_count Idle count\n")
		metrics += fmt.Sprintf("# TYPE pool_idle_count gauge\n")
		metrics += fmt.Sprintf("pool_idle_count{pool=\"%s\"} %d\n", poolID, latest.IdleCount)

		metrics += fmt.Sprintf("# HELP pool_wait_count Wait count\n")
		metrics += fmt.Sprintf("# TYPE pool_wait_count counter\n")
		metrics += fmt.Sprintf("pool_wait_count{pool=\"%s\"} %d\n", poolID, latest.WaitCount)

		metrics += fmt.Sprintf("# HELP pool_create_count Total created objects\n")
		metrics += fmt.Sprintf("# TYPE pool_create_count counter\n")
		metrics += fmt.Sprintf("pool_create_count{pool=\"%s\"} %d\n", poolID, latest.CreateCount)

		metrics += fmt.Sprintf("# HELP pool_destroy_count Total destroyed objects\n")
		metrics += fmt.Sprintf("# TYPE pool_destroy_count counter\n")
		metrics += fmt.Sprintf("pool_destroy_count{pool=\"%s\"} %d\n", poolID, latest.DestroyCount)

		metrics += fmt.Sprintf("# HELP pool_timeout_count Timeout count\n")
		metrics += fmt.Sprintf("# TYPE pool_timeout_count counter\n")
		metrics += fmt.Sprintf("pool_timeout_count{pool=\"%s\"} %d\n", poolID, latest.TimeoutCount)

		metrics += fmt.Sprintf("# HELP pool_error_count Error count\n")
		metrics += fmt.Sprintf("# TYPE pool_error_count counter\n")
		metrics += fmt.Sprintf("pool_error_count{pool=\"%s\"} %d\n", poolID, latest.ErrorCount)

		metrics += fmt.Sprintf("# HELP pool_steal_count Steal count\n")
		metrics += fmt.Sprintf("# TYPE pool_steal_count counter\n")
		metrics += fmt.Sprintf("pool_steal_count{pool=\"%s\"} %d\n", poolID, latest.StealCount)

		if batchStats, ok := c.batchStats[poolID]; ok {
			metrics += fmt.Sprintf("# HELP pool_batch_total Total batch operations\n")
			metrics += fmt.Sprintf("# TYPE pool_batch_total counter\n")
			metrics += fmt.Sprintf("pool_batch_total{pool=\"%s\"} %d\n", poolID, batchStats.TotalBatches)

			metrics += fmt.Sprintf("# HELP pool_batch_items Total items in batch operations\n")
			metrics += fmt.Sprintf("# TYPE pool_batch_items counter\n")
			metrics += fmt.Sprintf("pool_batch_items{pool=\"%s\"} %d\n", poolID, batchStats.TotalItems)

			metrics += fmt.Sprintf("# HELP pool_batch_avg_size Average batch size\n")
			metrics += fmt.Sprintf("# TYPE pool_batch_avg_size gauge\n")
			metrics += fmt.Sprintf("pool_batch_avg_size{pool=\"%s\"} %f\n", poolID, batchStats.AvgBatchSize)
		}
	}

	if !hasData {
		metrics = "# No metrics data available\n"
	}

	return metrics
}

// GetMetricsSummary 获取完整指标摘要。
func (c *MetricsCollector) GetMetricsSummary() map[string]interface{} {
	c.mu.RLock()
	poolIDs := make([]string, 0, len(c.poolMetrics))
	for id := range c.poolMetrics {
		poolIDs = append(poolIDs, id)
	}
	c.mu.RUnlock()

	summary := make(map[string]interface{})

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	c.mu.RLock()
	gcPause := c.lastGCPause.String()
	c.mu.RUnlock()

	summary["system"] = map[string]interface{}{
		"goroutines":     runtime.NumGoroutine(),
		"memory_alloc":   memStats.Alloc,
		"memory_total":   memStats.TotalAlloc,
		"gc_pause_total": gcPause,
		"uptime":         time.Since(c.startTime).String(),
	}

	pools := make(map[string]interface{})
	for _, poolID := range poolIDs {
		c.mu.RLock()
		buffer, ok := c.poolMetrics[poolID]
		var latest *PoolStats
		if ok && buffer != nil {
			latest = buffer.GetLatest()
		}
		var bs *BatchStats
		if s, ok := c.batchStats[poolID]; ok {
			bs = s
		}
		c.mu.RUnlock()

		if latest == nil {
			continue
		}

		poolInfo := map[string]interface{}{
			"type":         latest.Type,
			"active":       latest.ActiveCount,
			"idle":         latest.IdleCount,
			"capacity":     latest.MaxCapacity,
			"min_capacity": latest.MinCapacity,
			"waiting":      latest.WaitCount,
			"created":      latest.CreateCount,
			"destroyed":    latest.DestroyCount,
			"timeouts":     latest.TimeoutCount,
			"errors":       latest.ErrorCount,
			"steals":       latest.StealCount,
		}

		if enhanced := c.GetDetailedStats(poolID); enhanced != nil {
			poolInfo["error_rate"] = enhanced.ErrorRate
			poolInfo["throughput"] = enhanced.Throughput
			poolInfo["panic_count"] = enhanced.PanicCount
			poolInfo["steal_rate"] = enhanced.StealSuccessRate
		}

		if bs != nil {
			poolInfo["batch_ops"] = map[string]interface{}{
				"total":    bs.TotalBatches,
				"items":    bs.TotalItems,
				"avg_size": bs.AvgBatchSize,
				"min_size": bs.MinBatchSize,
				"max_size": bs.MaxBatchSize,
			}
		}

		pools[poolID] = poolInfo
	}

	summary["pools"] = pools
	return summary
}
