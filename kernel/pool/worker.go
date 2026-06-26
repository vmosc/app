package pool

import (
	"container/heap"
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Task 任务接口。
type Task interface {
	Execute(ctx context.Context) error
	ID() string
	Priority() int
}

// BaseTask 基础任务结构。
type BaseTask struct {
	id       string
	priority int
}

// NewBaseTask 创建基础任务。
func NewBaseTask(id string, priority int) *BaseTask {
	return &BaseTask{
		id:       id,
		priority: priority,
	}
}

func (t *BaseTask) ID() string {
	return t.id
}

func (t *BaseTask) Priority() int {
	return t.priority
}

func (t *BaseTask) Execute(ctx context.Context) error {
	return nil
}

// FuncTask 函数任务。
type FuncTask struct {
	BaseTask
	fn func(context.Context) error
}

// NewFuncTask 创建函数任务。
func NewFuncTask(id string, fn func(context.Context) error) *FuncTask {
	return &FuncTask{
		BaseTask: BaseTask{id: id},
		fn:       fn,
	}
}

func (t *FuncTask) Execute(ctx context.Context) error {
	return t.fn(ctx)
}

// taskWrapper 任务包装器。
type taskWrapper struct {
	task    Task
	ctx     context.Context
	seq     uint64
	prioIdx int
}

// TaskSource 可选任务来源。
type TaskSource interface {
	BlockingPop() *taskWrapper
}

// TaskResult 任务结果。
type TaskResult struct {
	TaskID    string
	Error     error
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Priority  int
	Panic     interface{}
}

// WorkerPool 工作池结构。
type WorkerPool struct {
	name       string
	taskQueue  chan *taskWrapper
	resultChan chan *TaskResult
	closed     uint32
	wg         sync.WaitGroup
	stats      struct {
		totalTasks      int64
		waitingTasks    int64
		processingTasks int64
		completedTasks  int64
		busyWorkers     int64
		totalWorkers    int32
		avgProcessTime  int64
		rejectedTasks   int64
		panicCount      int64
		errorCount      int64
	}
	mu      sync.RWMutex
	closeMu sync.Mutex

	minWorkers  int32
	maxWorkers  int32
	workerCount int32
	idleTimeout time.Duration

	autoScaler    *WorkerPoolAutoScaler
	scaleStopChan chan struct{}

	taskSource  TaskSource
	beforeClose func()
}

// NewWorkerPoolWithName 创建带名称的工作池。
func NewWorkerPoolWithName(name string, workerCount int, queueSize int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	cfg := DefaultConfig()
	return NewWorkerPoolWithConfig(name, workerCount, queueSize, cfg)
}

// NewWorkerPoolWithConfig 使用配置创建工作池。
func NewWorkerPoolWithConfig(name string, workerCount int, queueSize int, cfg Config) *WorkerPool {
	p := &WorkerPool{
		name:          name,
		taskQueue:     make(chan *taskWrapper, queueSize),
		resultChan:    make(chan *TaskResult, queueSize),
		minWorkers:    int32(cfg.MinSize),
		maxWorkers:    int32(cfg.MaxSize),
		idleTimeout:   cfg.WorkerIdleTimeout,
		scaleStopChan: make(chan struct{}),
	}
	if p.minWorkers <= 0 {
		p.minWorkers = 1
	}
	if p.maxWorkers < p.minWorkers {
		p.maxWorkers = p.minWorkers
	}
	for i := 0; i < int(p.minWorkers); i++ {
		p.startWorker()
	}
	if cfg.EnableAutoScale {
		p.autoScaler = NewWorkerPoolAutoScaler(p, cfg)
		p.autoScaler.Start()
	}
	return p
}

// newWorkerPoolWithoutWorkers 创建工作池核心但不启动 worker。
func newWorkerPoolWithoutWorkers(name string, workerCount int, queueSize int, cfg Config) *WorkerPool {
	p := &WorkerPool{
		name:          name,
		taskQueue:     make(chan *taskWrapper, queueSize),
		resultChan:    make(chan *TaskResult, queueSize),
		idleTimeout:   cfg.WorkerIdleTimeout,
		scaleStopChan: make(chan struct{}),
	}
	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
	}
	p.minWorkers = int32(workerCount)
	p.maxWorkers = int32(cfg.MaxSize)
	if p.maxWorkers < p.minWorkers {
		p.maxWorkers = p.minWorkers
	}
	if p.idleTimeout <= 0 {
		p.idleTimeout = time.Minute
	}
	if cfg.EnableAutoScale {
		p.autoScaler = NewWorkerPoolAutoScaler(p, cfg)
		p.autoScaler.Start()
	}
	return p
}

// startWorker 启动一个新的 worker goroutine。
func (p *WorkerPool) startWorker() {
	p.wg.Add(1)
	atomic.AddInt32(&p.workerCount, 1)
	atomic.AddInt32(&p.stats.totalWorkers, 1)
	go p.workerLoop()
}

// workerLoop 单个 worker 的主循环。
func (p *WorkerPool) workerLoop() {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&p.stats.panicCount, 1)
		}
		atomic.AddInt32(&p.workerCount, -1)
		atomic.AddInt32(&p.stats.totalWorkers, -1)
		p.wg.Done()
	}()

	for {
		if atomic.LoadUint32(&p.closed) == 1 {
			return
		}

		if p.taskSource != nil {
			wrapper := p.taskSource.BlockingPop()
			if wrapper == nil {
				if atomic.LoadUint32(&p.closed) == 1 {
					return
				}
				continue
			}
			p.processTask(wrapper)
			continue
		}

		select {
		case wrapper, ok := <-p.taskQueue:
			if !ok {
				return
			}
			if wrapper != nil {
				p.processTask(wrapper)
			}
		case <-time.After(p.idleTimeout):
			if atomic.LoadInt32(&p.workerCount) > atomic.LoadInt32(&p.minWorkers) {
				return
			}
		}
	}
}

// processTask 处理单个任务。
func (p *WorkerPool) processTask(wrapper *taskWrapper) {
	atomic.AddInt64(&p.stats.waitingTasks, -1)
	atomic.AddInt64(&p.stats.processingTasks, 1)
	atomic.AddInt64(&p.stats.busyWorkers, 1)
	defer func() {
		atomic.AddInt64(&p.stats.processingTasks, -1)
		atomic.AddInt64(&p.stats.busyWorkers, -1)
	}()

	start := time.Now()
	result := &TaskResult{
		TaskID:    wrapper.task.ID(),
		StartTime: start,
		Priority:  wrapper.task.Priority(),
	}

	ctx := wrapper.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				atomic.AddInt64(&p.stats.panicCount, 1)
				result.Error = &PoolError{Op: "execute", Code: ErrCodeRejected, Err: ErrTaskRejected, PoolID: p.name}
				result.Panic = r
			}
		}()
		err := wrapper.task.Execute(ctx)
		result.Error = err
		if err != nil {
			atomic.AddInt64(&p.stats.errorCount, 1)
		}
	}()

	result.EndTime = time.Now()
	result.Duration = result.EndTime.Sub(start)

	select {
	case p.resultChan <- result:
	default:
		atomic.AddInt64(&p.stats.errorCount, 1)
	}

	atomic.AddInt64(&p.stats.completedTasks, 1)
	p.updateAvgProcessTime(result.Duration.Nanoseconds())
}

// updateAvgProcessTime 更新平均处理时间。
func (p *WorkerPool) updateAvgProcessTime(d int64) {
	old := atomic.LoadInt64(&p.stats.avgProcessTime)
	completed := atomic.LoadInt64(&p.stats.completedTasks)
	if completed <= 1 {
		atomic.StoreInt64(&p.stats.avgProcessTime, d)
		return
	}
	newAvg := (old*(completed-1) + d) / completed
	atomic.StoreInt64(&p.stats.avgProcessTime, newAvg)
}

// Submit 提交任务（默认 context）。
func (p *WorkerPool) Submit(task Task) error {
	return p.SubmitWithContext(context.Background(), task)
}

// SubmitWithContext 带 context 提交任务。
func (p *WorkerPool) SubmitWithContext(ctx context.Context, task Task) error {
	if atomic.LoadUint32(&p.closed) == 1 {
		return &PoolError{Op: "submit", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name}
	}

	wrapper := &taskWrapper{task: task, ctx: ctx, prioIdx: -1}

	select {
	case p.taskQueue <- wrapper:
		atomic.AddInt64(&p.stats.totalTasks, 1)
		atomic.AddInt64(&p.stats.waitingTasks, 1)
		if atomic.LoadInt64(&p.stats.waitingTasks) > int64(cap(p.taskQueue))/2 &&
			atomic.LoadInt32(&p.workerCount) < atomic.LoadInt32(&p.maxWorkers) {
			p.startWorker()
		}
		return nil
	default:
		select {
		case <-ctx.Done():
			atomic.AddInt64(&p.stats.rejectedTasks, 1)
			return ctx.Err()
		default:
			select {
			case p.taskQueue <- wrapper:
				atomic.AddInt64(&p.stats.totalTasks, 1)
				atomic.AddInt64(&p.stats.waitingTasks, 1)
				if atomic.LoadInt64(&p.stats.waitingTasks) > int64(cap(p.taskQueue))/2 &&
					atomic.LoadInt32(&p.workerCount) < atomic.LoadInt32(&p.maxWorkers) {
					p.startWorker()
				}
				return nil
			case <-ctx.Done():
				atomic.AddInt64(&p.stats.rejectedTasks, 1)
				return ctx.Err()
			}
		}
	}
}

// TrySubmit 非阻塞提交任务。
func (p *WorkerPool) TrySubmit(task Task) error {
	if atomic.LoadUint32(&p.closed) == 1 {
		return &PoolError{Op: "try_submit", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name}
	}
	wrapper := &taskWrapper{task: task, ctx: context.Background(), prioIdx: -1}
	select {
	case p.taskQueue <- wrapper:
		atomic.AddInt64(&p.stats.totalTasks, 1)
		atomic.AddInt64(&p.stats.waitingTasks, 1)
		if atomic.LoadInt64(&p.stats.waitingTasks) > int64(cap(p.taskQueue))/2 &&
			atomic.LoadInt32(&p.workerCount) < atomic.LoadInt32(&p.maxWorkers) {
			p.startWorker()
		}
		return nil
	default:
		for i := 0; i < 3; i++ {
			time.Sleep(10 * time.Millisecond)
			select {
			case p.taskQueue <- wrapper:
				atomic.AddInt64(&p.stats.totalTasks, 1)
				atomic.AddInt64(&p.stats.waitingTasks, 1)
				return nil
			default:
			}
		}
		atomic.AddInt64(&p.stats.rejectedTasks, 1)
		return ErrTaskRejected
	}
}

// SubmitBatch 批量提交任务。
func (p *WorkerPool) SubmitBatch(tasks []Task) []error {
	errs := make([]error, len(tasks))
	for i, t := range tasks {
		errs[i] = p.TrySubmit(t)
	}
	return errs
}

// ResultChan 返回结果通道。
func (p *WorkerPool) ResultChan() <-chan *TaskResult {
	return p.resultChan
}

// Close 默认关闭（5秒优雅期）。
func (p *WorkerPool) Close() error {
	return p.CloseWithGracePeriod(5 * time.Second)
}

// CloseWithGracePeriod 带优雅期的关闭。
func (p *WorkerPool) CloseWithGracePeriod(gracePeriod time.Duration) error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	if atomic.LoadUint32(&p.closed) == 1 {
		return nil
	}
	atomic.StoreUint32(&p.closed, 1)

	if p.autoScaler != nil {
		p.autoScaler.Stop()
	}

	if p.beforeClose != nil {
		p.beforeClose()
	}

	close(p.taskQueue)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracePeriod):
	}

	close(p.resultChan)
	return nil
}

// Stats 返回基础统计。
func (p *WorkerPool) Stats() PoolStats {
	totalWorkers := int64(atomic.LoadInt32(&p.stats.totalWorkers))
	busyWorkers := atomic.LoadInt64(&p.stats.busyWorkers)
	idleWorkers := totalWorkers - busyWorkers
	if idleWorkers < 0 {
		idleWorkers = 0
	}

	return PoolStats{
		Type:           TypeWorker,
		ActiveCount:    uint64(atomic.LoadInt64(&p.stats.processingTasks)),
		IdleCount:      uint64(idleWorkers),
		MaxCapacity:    uint64(atomic.LoadInt32(&p.maxWorkers)),
		MinCapacity:    uint64(atomic.LoadInt32(&p.minWorkers)),
		WaitCount:      uint64(atomic.LoadInt64(&p.stats.waitingTasks)),
		CreateCount:    uint64(atomic.LoadInt64(&p.stats.totalTasks)),
		DestroyCount:   uint64(atomic.LoadInt64(&p.stats.completedTasks)),
		ErrorCount:     uint64(atomic.LoadInt64(&p.stats.errorCount)),
		LastActiveTime: time.Now().UnixNano(),
	}
}

// IsClosed 检查池是否已关闭。
func (p *WorkerPool) IsClosed() bool {
	return atomic.LoadUint32(&p.closed) == 1
}

// Name 返回池名称。
func (p *WorkerPool) Name() string { return p.name }

// SetName 设置池名称。
func (p *WorkerPool) SetName(name string) { p.name = name }

// Type 返回池类型。
func (p *WorkerPool) Type() string { return TypeWorker }

// WorkerPoolStats 返回详细统计。
func (p *WorkerPool) WorkerPoolStats() map[string]interface{} {
	return map[string]interface{}{
		"totalTasks":      atomic.LoadInt64(&p.stats.totalTasks),
		"waitingTasks":    atomic.LoadInt64(&p.stats.waitingTasks),
		"processingTasks": atomic.LoadInt64(&p.stats.processingTasks),
		"completedTasks":  atomic.LoadInt64(&p.stats.completedTasks),
		"totalWorkers":    atomic.LoadInt32(&p.workerCount),
		"busyWorkers":     atomic.LoadInt64(&p.stats.busyWorkers),
		"avgProcessTime":  atomic.LoadInt64(&p.stats.avgProcessTime),
		"rejectedTasks":   atomic.LoadInt64(&p.stats.rejectedTasks),
		"panicCount":      atomic.LoadInt64(&p.stats.panicCount),
		"errorCount":      atomic.LoadInt64(&p.stats.errorCount),
	}
}

// Resize 调整最大/最小 worker 数。
func (p *WorkerPool) Resize(newMax, newMin int) error {
	if newMax < newMin {
		newMax = newMin
	}
	atomic.StoreInt32(&p.maxWorkers, int32(newMax))
	atomic.StoreInt32(&p.minWorkers, int32(newMin))
	return nil
}

// ========== 自动扩缩容 ==========

// WorkerPoolAutoScaler 工作池自动扩缩容器。
type WorkerPoolAutoScaler struct {
	pool           *WorkerPool
	config         Config
	stopChan       chan struct{}
	done           chan struct{}
	lastAdjustTime time.Time
	mu             sync.Mutex
}

// NewWorkerPoolAutoScaler 创建工作池自动扩缩容器。
func NewWorkerPoolAutoScaler(pool *WorkerPool, config Config) *WorkerPoolAutoScaler {
	return &WorkerPoolAutoScaler{
		pool:     pool,
		config:   config,
		stopChan: make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start 启动自动扩缩容。
func (as *WorkerPoolAutoScaler) Start() {
	go as.run()
}

// Stop 停止自动扩缩容。
func (as *WorkerPoolAutoScaler) Stop() {
	close(as.stopChan)
	<-as.done
}

// run 定期执行调整。
func (as *WorkerPoolAutoScaler) run() {
	defer close(as.done)
	ticker := time.NewTicker(as.config.ScaleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			as.adjust()
		case <-as.stopChan:
			return
		}
	}
}

// adjust 根据当前负载调整 worker 数量。
func (as *WorkerPoolAutoScaler) adjust() {
	as.mu.Lock()
	defer as.mu.Unlock()

	if as.pool.IsClosed() {
		return
	}

	stats := as.pool.WorkerPoolStats()
	totalWorkers := stats["totalWorkers"].(int32)
	busy := stats["busyWorkers"].(int64)

	utilization := 0.0
	if totalWorkers > 0 {
		utilization = float64(busy) / float64(totalWorkers)
	}

	targetUtil := as.config.TargetUtilization
	if utilization > targetUtil*1.2 && totalWorkers < atomic.LoadInt32(&as.pool.maxWorkers) {
		newWorkers := int(float64(totalWorkers) * as.config.ScaleUpFactor)
		if newWorkers > int(atomic.LoadInt32(&as.pool.maxWorkers)) {
			newWorkers = int(atomic.LoadInt32(&as.pool.maxWorkers))
		}
		for i := int(totalWorkers); i < newWorkers; i++ {
			as.pool.startWorker()
		}
	}
	as.lastAdjustTime = time.Now()
}

// ========== 优先级工作池 ==========

type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

type prioHeap []*taskWrapper

func (h prioHeap) Len() int { return len(h) }

func (h prioHeap) Less(i, j int) bool {
	pi, pj := h[i].task.Priority(), h[j].task.Priority()
	if pi != pj {
		return pi > pj
	}
	return h[i].seq < h[j].seq
}

func (h prioHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *prioHeap) Push(x interface{}) {
	*h = append(*h, x.(*taskWrapper))
}

func (h *prioHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PriorityWorkerPool 优先级工作池。
type PriorityWorkerPool struct {
	*WorkerPool
	levels      int
	perLevelCap int
	heap        prioHeap
	nPerLevel   []int
	prioMu      sync.Mutex
	prioCond    *sync.Cond
	submitSeq   uint64
}

// NewPriorityWorkerPoolWithName 创建带名称的优先级工作池。
func NewPriorityWorkerPoolWithName(name string, workerCount int, queueSize int, levels int) *PriorityWorkerPool {
	if levels <= 0 {
		levels = 4
	}
	if queueSize <= 0 {
		queueSize = 1024
	}
	cfg := DefaultConfig()
	tqSize := queueSize * levels
	wp := newWorkerPoolWithoutWorkers(name, workerCount, tqSize, cfg)
	pp := &PriorityWorkerPool{
		WorkerPool:  wp,
		levels:      levels,
		perLevelCap: queueSize,
		nPerLevel:   make([]int, levels),
	}
	pp.prioCond = sync.NewCond(&pp.prioMu)
	wp.taskSource = pp
	wp.beforeClose = func() {
		pp.prioMu.Lock()
		pp.prioCond.Broadcast()
		pp.prioMu.Unlock()
	}
	for i := 0; i < int(wp.minWorkers); i++ {
		wp.startWorker()
	}
	return pp
}

// BlockingPop 实现 TaskSource。
func (p *PriorityWorkerPool) BlockingPop() *taskWrapper {
	p.prioMu.Lock()
	defer p.prioMu.Unlock()
	for p.heap.Len() == 0 {
		if atomic.LoadUint32(&p.WorkerPool.closed) == 1 {
			return nil
		}
		p.prioCond.Wait()
	}
	w := heap.Pop(&p.heap).(*taskWrapper)
	if w.prioIdx >= 0 && w.prioIdx < len(p.nPerLevel) {
		p.nPerLevel[w.prioIdx]--
	}
	return w
}

// SubmitWithPriority 带优先级提交任务。
func (p *PriorityWorkerPool) SubmitWithPriority(task Task, prio Priority) error {
	if atomic.LoadUint32(&p.closed) == 1 {
		return &PoolError{Op: "submit_priority", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name}
	}
	if int(prio) < 0 || int(prio) >= p.levels {
		prio = PriorityNormal
	}
	pi := int(prio)

	p.prioMu.Lock()
	defer p.prioMu.Unlock()
	if p.nPerLevel[pi] >= p.perLevelCap {
		atomic.AddInt64(&p.stats.rejectedTasks, 1)
		return ErrTaskRejected
	}
	seq := atomic.AddUint64(&p.submitSeq, 1)
	w := &taskWrapper{task: task, ctx: context.Background(), seq: seq, prioIdx: pi}
	heap.Push(&p.heap, w)
	p.nPerLevel[pi]++
	atomic.AddInt64(&p.stats.totalTasks, 1)
	atomic.AddInt64(&p.stats.waitingTasks, 1)
	p.prioCond.Signal()
	return nil
}

// Submit 覆盖原提交方法（默认普通优先级）。
func (p *PriorityWorkerPool) Submit(task Task) error {
	return p.SubmitWithPriority(task, PriorityNormal)
}

// Close 关闭优先级工作池。
func (p *PriorityWorkerPool) Close() error {
	return p.WorkerPool.Close()
}
