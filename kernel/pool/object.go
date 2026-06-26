// app/kernel/pool/object.go
package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// poolItem 包装实际对象，携带元数据。
type poolItem struct {
	val      interface{}
	createAt int64
	lastAt   int64
}

// waitEntry 等待队列条目。
type waitEntry struct {
	ch chan interface{}
	id uint64
}

// LockFreeObjectPool 基于 sync.Pool 和 channel 的无锁对象池。
type LockFreeObjectPool struct {
	name   string
	config Config

	idleChan chan *poolItem
	itemPool sync.Pool

	createCount  int64
	destroyCount int64
	activeCount  int64
	timeoutCount int64
	errorCount   int64
	waitCount    int64
	getLatency   int64
	putLatency   int64
	recycleCount int64

	waitMu      sync.Mutex
	waitEntries []*waitEntry
	nextWaitID  uint64

	closeChan chan struct{}
	closed    uint32
	closeOnce sync.Once

	createFn   func() (interface{}, error)
	validateFn func(interface{}) bool
	destroyFn  func(interface{})

	debugLogger DebugLogger

	scaleMu    sync.RWMutex
	currentMax int32
}

// ObjectPoolOption 对象池配置选项。
type ObjectPoolOption func(*objectPoolOptions)

type objectPoolOptions struct {
	debugLogger DebugLogger
	name        string
}

// WithDebugLogger 设置调试日志。
func WithDebugLogger(logger DebugLogger) ObjectPoolOption {
	return func(o *objectPoolOptions) {
		o.debugLogger = logger
	}
}

// WithPoolName 设置池名称。
func WithPoolName(name string) ObjectPoolOption {
	return func(o *objectPoolOptions) {
		o.name = name
	}
}

// NewLockFreeObjectPool 构造函数。
func NewLockFreeObjectPool(
	create func() (interface{}, error),
	validate func(interface{}) bool,
	destroy func(interface{}),
	config Config,
	opts ...ObjectPoolOption,
) *LockFreeObjectPool {
	options := &objectPoolOptions{
		debugLogger: &defaultDebugLogger{},
		name:        "object-pool",
	}
	for _, opt := range opts {
		opt(options)
	}

	if config.MinSize > config.MaxSize {
		config.MinSize = config.MaxSize
	}

	p := &LockFreeObjectPool{
		name:        options.name,
		config:      config,
		idleChan:    make(chan *poolItem, config.MaxSize),
		closeChan:   make(chan struct{}),
		createFn:    create,
		validateFn:  validate,
		destroyFn:   destroy,
		debugLogger: options.debugLogger,
		currentMax:  int32(config.MaxSize),
		itemPool: sync.Pool{
			New: func() interface{} {
				return &poolItem{}
			},
		},
	}

	if config.PreAllocate && config.MinSize > 0 {
		p.preAllocate()
	}

	if config.HealthCheckInterval > 0 {
		go p.healthCheck()
	}

	return p
}

// Name 返回池名称。
func (p *LockFreeObjectPool) Name() string {
	return p.name
}

// SetName 设置池名称。
func (p *LockFreeObjectPool) SetName(name string) {
	p.name = name
}

// preAllocate 预分配对象。
func (p *LockFreeObjectPool) preAllocate() {
	n := p.config.MinSize
	for i := 0; i < n; i++ {
		obj, err := p.createFn()
		if err != nil {
			atomic.AddInt64(&p.errorCount, 1)
			continue
		}
		p.putIdle(obj)
	}
}

// putIdle 将对象放入空闲队列。
func (p *LockFreeObjectPool) putIdle(obj interface{}) {
	now := time.Now().UnixNano()
	item := p.itemPool.Get().(*poolItem)
	item.val = obj
	item.createAt = now
	item.lastAt = now
	select {
	case p.idleChan <- item:
		atomic.AddInt64(&p.createCount, 1)
	default:
		p.destroyObj(obj)
		atomic.AddInt64(&p.destroyCount, 1)
		p.itemPool.Put(item)
	}
}

// getIdle 从空闲队列获取一个对象（非阻塞）。
func (p *LockFreeObjectPool) getIdle() *poolItem {
	select {
	case item := <-p.idleChan:
		return item
	default:
		return nil
	}
}

// Get 获取对象（使用默认超时）。
func (p *LockFreeObjectPool) Get() (interface{}, error) {
	return p.GetWithTimeout(p.config.GetTimeout)
}

// GetWithContext 带上下文获取对象。
func (p *LockFreeObjectPool) GetWithContext(ctx context.Context) (interface{}, error) {
	if atomic.LoadUint32(&p.closed) == 1 {
		return nil, &PoolError{Op: "get", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name}
	}

	start := time.Now()
	defer func() { atomic.AddInt64(&p.getLatency, int64(time.Since(start).Nanoseconds())) }()

	if item := p.getIdle(); item != nil {
		now := time.Now().UnixNano()
		if (p.config.MaxIdleTime > 0 && now-item.lastAt > int64(p.config.MaxIdleTime)) ||
			(p.config.MaxLifeTime > 0 && now-item.createAt > int64(p.config.MaxLifeTime)) {
			p.destroyObj(item.val)
			atomic.AddInt64(&p.destroyCount, 1)
			p.itemPool.Put(item)
			return p.GetWithContext(ctx)
		}
		if p.validateFn != nil && !p.validateFn(item.val) {
			p.destroyObj(item.val)
			atomic.AddInt64(&p.destroyCount, 1)
			p.itemPool.Put(item)
			return p.GetWithContext(ctx)
		}
		obj := item.val
		p.itemPool.Put(item)
		atomic.AddInt64(&p.activeCount, 1)
		return obj, nil
	}

	if p.canCreateNew() {
		type createResult struct {
			obj interface{}
			err error
		}
		ch := make(chan createResult, 1)
		go func() {
			obj, err := p.createFn()
			ch <- createResult{obj, err}
		}()

		select {
		case r := <-ch:
			if r.err != nil {
				atomic.AddInt64(&p.errorCount, 1)
				return nil, r.err
			}
			atomic.AddInt64(&p.createCount, 1)
			atomic.AddInt64(&p.activeCount, 1)
			return r.obj, nil
		case <-ctx.Done():
			waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			select {
			case r := <-ch:
				if r.err == nil && r.obj != nil {
					p.destroyObj(r.obj)
					atomic.AddInt64(&p.destroyCount, 1)
				}
			case <-waitCtx.Done():
			}
			atomic.AddInt64(&p.timeoutCount, 1)
			return nil, ctx.Err()
		}
	}

	return p.waitForObject(ctx)
}

// GetWithTimeout 带超时获取对象。
func (p *LockFreeObjectPool) GetWithTimeout(timeout time.Duration) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.GetWithContext(ctx)
}

// waitForObject 等待对象可用。
func (p *LockFreeObjectPool) waitForObject(ctx context.Context) (interface{}, error) {
	atomic.AddInt64(&p.waitCount, 1)
	defer atomic.AddInt64(&p.waitCount, -1)

	waitCh := make(chan interface{}, 1)
	id := atomic.AddUint64(&p.nextWaitID, 1)
	entry := &waitEntry{ch: waitCh, id: id}

	p.waitMu.Lock()
	p.waitEntries = append(p.waitEntries, entry)
	p.waitMu.Unlock()

	select {
	case obj := <-waitCh:
		if obj != nil {
			atomic.AddInt64(&p.activeCount, 1)
			return obj, nil
		}
		return nil, &PoolError{Op: "wait", Code: ErrCodeInvalidObject, Err: ErrInvalidObject, PoolID: p.name}
	case <-ctx.Done():
		p.waitMu.Lock()
		for i, e := range p.waitEntries {
			if e.id == id {
				p.waitEntries = append(p.waitEntries[:i], p.waitEntries[i+1:]...)
				break
			}
		}
		p.waitMu.Unlock()
		select {
		case obj := <-waitCh:
			if obj != nil {
				p.returnToIdle(obj)
			}
		default:
		}
		atomic.AddInt64(&p.timeoutCount, 1)
		return nil, ctx.Err()
	case <-p.closeChan:
		p.waitMu.Lock()
		for i, e := range p.waitEntries {
			if e.id == id {
				p.waitEntries = append(p.waitEntries[:i], p.waitEntries[i+1:]...)
				break
			}
		}
		p.waitMu.Unlock()
		select {
		case obj := <-waitCh:
			if obj != nil {
				p.returnToIdle(obj)
			}
		default:
		}
		return nil, &PoolError{Op: "wait", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name}
	}
}

// returnToIdle 将对象归还空闲队列（用于超时/关闭时回收）。
func (p *LockFreeObjectPool) returnToIdle(obj interface{}) {
	if obj == nil {
		return
	}
	now := time.Now().UnixNano()
	item := p.itemPool.Get().(*poolItem)
	item.val = obj
	item.createAt = now
	item.lastAt = now
	select {
	case p.idleChan <- item:
	default:
		p.destroyObj(obj)
		atomic.AddInt64(&p.destroyCount, 1)
		p.itemPool.Put(item)
	}
}

// Put 归还对象。
func (p *LockFreeObjectPool) Put(obj interface{}) error {
	if atomic.LoadUint32(&p.closed) == 1 {
		if obj != nil {
			p.destroyObj(obj)
			atomic.AddInt64(&p.destroyCount, 1)
		}
		atomic.AddInt64(&p.activeCount, -1)
		return nil
	}
	if obj == nil {
		return &PoolError{Op: "put", Code: ErrCodeInvalidObject, Err: ErrInvalidObject, PoolID: p.name}
	}

	start := time.Now()
	defer func() { atomic.AddInt64(&p.putLatency, int64(time.Since(start).Nanoseconds())) }()

	if p.validateFn != nil && !p.validateFn(obj) {
		p.destroyObj(obj)
		atomic.AddInt64(&p.destroyCount, 1)
		atomic.AddInt64(&p.activeCount, -1)
		return nil
	}

	p.waitMu.Lock()
	var entry *waitEntry
	if len(p.waitEntries) > 0 {
		entry = p.waitEntries[0]
		p.waitEntries = p.waitEntries[1:]
	}
	p.waitMu.Unlock()

	if entry != nil {
		select {
		case entry.ch <- obj:
			return nil
		default:
		}
	}

	if p.config.MinSize > 0 && len(p.idleChan) >= p.config.MinSize {
		p.destroyObj(obj)
		atomic.AddInt64(&p.destroyCount, 1)
		atomic.AddInt64(&p.activeCount, -1)
		return nil
	}

	now := time.Now().UnixNano()
	item := p.itemPool.Get().(*poolItem)
	item.val = obj
	item.createAt = now
	item.lastAt = now
	select {
	case p.idleChan <- item:
		atomic.AddInt64(&p.activeCount, -1)
		return nil
	default:
		p.destroyObj(obj)
		atomic.AddInt64(&p.destroyCount, 1)
		p.itemPool.Put(item)
		atomic.AddInt64(&p.activeCount, -1)
		return nil
	}
}

// GetBatch 批量获取对象。
func (p *LockFreeObjectPool) GetBatch(count int) ([]interface{}, error) {
	if count <= 0 || count > p.config.BatchGetMax {
		return nil, &PoolError{Op: "batch_get", Code: ErrCodeInvalidObject, Err: ErrInvalidCount,
			PoolID: p.name, Time: time.Now(), Count: count}
	}
	if atomic.LoadUint32(&p.closed) == 1 {
		return nil, &PoolError{Op: "batch_get", Code: ErrCodeClosed, Err: ErrPoolClosed, PoolID: p.name, Time: time.Now()}
	}

	objs := make([]interface{}, 0, count)
	start := time.Now()
	defer func() { atomic.AddInt64(&p.getLatency, int64(time.Since(start).Nanoseconds())) }()

	for len(objs) < count {
		if item := p.getIdle(); item != nil {
			now := time.Now().UnixNano()
			if (p.config.MaxIdleTime > 0 && now-item.lastAt > int64(p.config.MaxIdleTime)) ||
				(p.config.MaxLifeTime > 0 && now-item.createAt > int64(p.config.MaxLifeTime)) {
				p.destroyObj(item.val)
				atomic.AddInt64(&p.destroyCount, 1)
				p.itemPool.Put(item)
				continue
			}
			if p.validateFn != nil && !p.validateFn(item.val) {
				p.destroyObj(item.val)
				atomic.AddInt64(&p.destroyCount, 1)
				p.itemPool.Put(item)
				continue
			}
			objs = append(objs, item.val)
			atomic.AddInt64(&p.activeCount, 1)
			p.itemPool.Put(item)
		} else {
			break
		}
	}

	for len(objs) < count && p.canCreateNew() {
		obj, err := p.createFn()
		if err != nil {
			atomic.AddInt64(&p.errorCount, 1)
			return objs, &PoolError{Op: "batch_get", Err: err, PoolID: p.name, Time: time.Now(), Count: count - len(objs)}
		}
		objs = append(objs, obj)
		atomic.AddInt64(&p.createCount, 1)
		atomic.AddInt64(&p.activeCount, 1)
	}

	return objs, nil
}

// PutBatch 批量归还对象。
func (p *LockFreeObjectPool) PutBatch(objs []interface{}) error {
	if len(objs) == 0 {
		return nil
	}
	if atomic.LoadUint32(&p.closed) == 1 {
		for _, obj := range objs {
			if obj != nil {
				p.destroyObj(obj)
				atomic.AddInt64(&p.destroyCount, 1)
			}
		}
		atomic.AddInt64(&p.activeCount, -int64(len(objs)))
		return nil
	}

	start := time.Now()
	defer func() { atomic.AddInt64(&p.putLatency, int64(time.Since(start).Nanoseconds())) }()

	validObjs := make([]interface{}, 0, len(objs))
	for _, obj := range objs {
		if obj == nil {
			continue
		}
		if p.validateFn != nil && !p.validateFn(obj) {
			p.destroyObj(obj)
			atomic.AddInt64(&p.destroyCount, 1)
			atomic.AddInt64(&p.activeCount, -1)
		} else {
			validObjs = append(validObjs, obj)
		}
	}
	if len(validObjs) == 0 {
		return nil
	}

	for len(validObjs) > 0 {
		p.waitMu.Lock()
		var entry *waitEntry
		if len(p.waitEntries) > 0 {
			entry = p.waitEntries[0]
			p.waitEntries = p.waitEntries[1:]
		}
		p.waitMu.Unlock()

		if entry != nil {
			select {
			case entry.ch <- validObjs[0]:
				validObjs = validObjs[1:]
				continue
			default:
			}
		} else {
			break
		}
	}

	now := time.Now().UnixNano()
	for _, obj := range validObjs {
		if p.config.MinSize > 0 && len(p.idleChan) >= p.config.MinSize {
			p.destroyObj(obj)
			atomic.AddInt64(&p.destroyCount, 1)
			atomic.AddInt64(&p.activeCount, -1)
			continue
		}
		item := p.itemPool.Get().(*poolItem)
		item.val = obj
		item.createAt = now
		item.lastAt = now
		select {
		case p.idleChan <- item:
			atomic.AddInt64(&p.activeCount, -1)
		default:
			p.destroyObj(obj)
			atomic.AddInt64(&p.destroyCount, 1)
			p.itemPool.Put(item)
			atomic.AddInt64(&p.activeCount, -1)
		}
	}
	return nil
}

// canCreateNew 检查是否可以创建新对象。
func (p *LockFreeObjectPool) canCreateNew() bool {
	p.scaleMu.RLock()
	max := p.currentMax
	p.scaleMu.RUnlock()
	total := atomic.LoadInt64(&p.createCount) - atomic.LoadInt64(&p.destroyCount)
	return int(total) < int(max)
}

// destroyObj 销毁对象。
func (p *LockFreeObjectPool) destroyObj(obj interface{}) {
	if p.destroyFn != nil && obj != nil {
		p.destroyFn(obj)
	}
}

// healthCheck 健康检查协程。
func (p *LockFreeObjectPool) healthCheck() {
	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.checkAndRecycle()
		case <-p.closeChan:
			return
		}
	}
}

// checkAndRecycle 检查并回收过期对象。
func (p *LockFreeObjectPool) checkAndRecycle() {
	// 池已关闭，直接返回，避免操作已关闭的 channel
	if atomic.LoadUint32(&p.closed) == 1 {
		return
	}

	now := time.Now().UnixNano()
	maxIdle := int64(p.config.MaxIdleTime)
	maxLife := int64(p.config.MaxLifeTime)
	if maxIdle <= 0 && maxLife <= 0 {
		return
	}

	for {
		select {
		case item, ok := <-p.idleChan:
			if !ok {
				// channel 已关闭，退出
				return
			}
			if item != nil {
				if (maxIdle > 0 && now-item.lastAt > maxIdle) ||
					(maxLife > 0 && now-item.createAt > maxLife) {
					// 防御：检查 val 是否为 nil
					if item.val != nil {
						p.destroyObj(item.val)
						atomic.AddInt64(&p.destroyCount, 1)
					}
					p.itemPool.Put(item)
					atomic.AddInt64(&p.recycleCount, 1)
				} else {
					// 放回前再次检查池是否关闭，避免 send on closed channel
					if atomic.LoadUint32(&p.closed) == 1 {
						if item.val != nil {
							p.destroyObj(item.val)
							atomic.AddInt64(&p.destroyCount, 1)
						}
						p.itemPool.Put(item)
						return
					}
					select {
					case p.idleChan <- item:
					default:
						if item.val != nil {
							p.destroyObj(item.val)
							atomic.AddInt64(&p.destroyCount, 1)
						}
						p.itemPool.Put(item)
					}
				}
			}
		default:
			return
		}
	}
}

// Resize 调整池大小。
func (p *LockFreeObjectPool) Resize(newSize int) error {
	if newSize < p.config.MinSize {
		newSize = p.config.MinSize
	}
	p.scaleMu.Lock()
	p.currentMax = int32(newSize)
	p.scaleMu.Unlock()
	return nil
}

// Close 关闭池。
func (p *LockFreeObjectPool) Close() error {
	p.closeOnce.Do(func() {
		atomic.StoreUint32(&p.closed, 1)
		close(p.closeChan)

		for {
			select {
			case item := <-p.idleChan:
				if item.val != nil {
					p.destroyObj(item.val)
					atomic.AddInt64(&p.destroyCount, 1)
				}
				p.itemPool.Put(item)
			default:
				close(p.idleChan)
				goto afterIdle
			}
		}
	afterIdle:
		p.waitMu.Lock()
		for _, entry := range p.waitEntries {
			select {
			case obj := <-entry.ch:
				if obj != nil {
					p.destroyObj(obj)
					atomic.AddInt64(&p.destroyCount, 1)
				}
			default:
			}
			close(entry.ch)
		}
		p.waitEntries = nil
		p.waitMu.Unlock()
	})
	return nil
}

// Stats 获取统计信息。
func (p *LockFreeObjectPool) Stats() PoolStats {
	return PoolStats{
		Type:           TypeObject,
		ActiveCount:    uint64(atomic.LoadInt64(&p.activeCount)),
		IdleCount:      uint64(len(p.idleChan)),
		MaxCapacity:    uint64(atomic.LoadInt32(&p.currentMax)),
		MinCapacity:    uint64(p.config.MinSize),
		WaitCount:      uint64(atomic.LoadInt64(&p.waitCount)),
		CreateCount:    uint64(atomic.LoadInt64(&p.createCount)),
		DestroyCount:   uint64(atomic.LoadInt64(&p.destroyCount)),
		TimeoutCount:   uint64(atomic.LoadInt64(&p.timeoutCount)),
		ErrorCount:     uint64(atomic.LoadInt64(&p.errorCount)),
		GetLatency:     uint64(atomic.LoadInt64(&p.getLatency)),
		PutLatency:     uint64(atomic.LoadInt64(&p.putLatency)),
		RecycleCount:   uint64(atomic.LoadInt64(&p.recycleCount)),
		LastActiveTime: time.Now().UnixNano(),
	}
}

// IsClosed 检查池是否已关闭。
func (p *LockFreeObjectPool) IsClosed() bool {
	return atomic.LoadUint32(&p.closed) == 1
}

// Type 返回池类型。
func (p *LockFreeObjectPool) Type() string {
	return TypeObject
}
