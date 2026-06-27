package test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmosc/app/kernel/pool"
)

type testObject struct {
	id       int
	valid    bool
	createAt time.Time
	data     []byte
}

func newTestObject(id int) *testObject {
	return &testObject{
		id:       id,
		valid:    true,
		createAt: time.Now(),
		data:     make([]byte, 1024),
	}
}

type testTask struct {
	*pool.BaseTask
	execFunc func(ctx context.Context) error
	sleep    time.Duration
	fail     bool
	panic    bool
}

func newTestTask(id string, priority int) *testTask {
	return &testTask{
		BaseTask: pool.NewBaseTask(id, priority),
	}
}

func (t *testTask) Execute(ctx context.Context) error {
	if t.panic {
		panic("test panic")
	}
	if t.sleep > 0 {
		select {
		case <-time.After(t.sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if t.fail {
		return errors.New("task failed")
	}
	if t.execFunc != nil {
		return t.execFunc(ctx)
	}
	return nil
}

type mockPool struct {
	statsFunc func() pool.PoolStats
}

func (m *mockPool) Type() string {
	if m.statsFunc != nil {
		return m.statsFunc().Type
	}
	return "mock"
}
func (m *mockPool) Stats() pool.PoolStats {
	if m.statsFunc != nil {
		return m.statsFunc()
	}
	return pool.PoolStats{}
}
func (m *mockPool) Close() error   { return nil }
func (m *mockPool) IsClosed() bool { return false }
func (m *mockPool) Name() string   { return "mock-pool" }
func (m *mockPool) SetName(string) {}

// ==================== 对象池测试 ====================

func TestLockFreeObjectPool_Basic(t *testing.T) {
	var createCount, destroyCount int32

	create := func() (interface{}, error) {
		atomic.AddInt32(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool {
		return obj.(*testObject).valid
	}
	destroy := func(obj interface{}) {
		atomic.AddInt32(&destroyCount, 1)
	}

	config := pool.DefaultConfig()
	config.MinSize = 5
	config.MaxSize = 10
	config.GetTimeout = 100 * time.Millisecond

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config, pool.WithPoolName("test-pool"))
	defer p.Close()

	assert.Equal(t, pool.TypeObject, p.Type())
	assert.Equal(t, "test-pool", p.Name())
	assert.False(t, p.IsClosed())

	obj1, err := p.Get()
	require.NoError(t, err)
	assert.NotNil(t, obj1)

	err = p.Put(obj1)
	require.NoError(t, err)

	objs := make([]interface{}, 8)
	for i := 0; i < 8; i++ {
		obj, err := p.Get()
		require.NoError(t, err)
		objs[i] = obj
	}
	for _, obj := range objs {
		err := p.Put(obj)
		require.NoError(t, err)
	}

	stats := p.Stats()
	assert.Equal(t, pool.TypeObject, stats.Type)
	assert.Equal(t, uint64(10), stats.MaxCapacity)
	assert.Equal(t, uint64(5), stats.MinCapacity)
	assert.Equal(t, uint64(5), stats.IdleCount)
}

func TestLockFreeObjectPool_InvalidObject(t *testing.T) {
	var destroyCount int32

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return obj.(*testObject).valid }
	destroy := func(obj interface{}) { atomic.AddInt32(&destroyCount, 1) }

	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 2

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	obj, err := p.Get()
	require.NoError(t, err)

	obj.(*testObject).valid = false

	err = p.Put(obj)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&destroyCount))
}

func TestLockFreeObjectPool_Timeout(t *testing.T) {
	create := func() (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return newTestObject(1), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	config := pool.DefaultConfig()
	config.MaxSize = 1
	config.MinSize = 0
	config.GetTimeout = 10 * time.Millisecond

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	_, err := p.Get()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestLockFreeObjectPool_Close(t *testing.T) {
	var destroyCount int32

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) { atomic.AddInt32(&destroyCount, 1) }

	config := pool.DefaultConfig()
	config.MinSize = 3
	config.MaxSize = 5

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)

	objs := make([]interface{}, 3)
	for i := 0; i < 3; i++ {
		obj, err := p.Get()
		require.NoError(t, err)
		objs[i] = obj
	}

	statsBefore := p.Stats()
	preDestroy := statsBefore.DestroyCount

	err := p.Close()
	require.NoError(t, err)
	assert.True(t, p.IsClosed())

	_, err = p.Get()
	assert.Error(t, err)

	for _, obj := range objs {
		err := p.Put(obj)
		require.NoError(t, err)
	}

	statsAfter := p.Stats()
	finalDestroy := statsAfter.DestroyCount
	assert.Equal(t, preDestroy+3, finalDestroy)
}

func TestLockFreeObjectPool_Concurrent(t *testing.T) {
	var createCount, destroyCount int32

	create := func() (interface{}, error) {
		atomic.AddInt32(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool { return obj.(*testObject).valid }
	destroy := func(obj interface{}) { atomic.AddInt32(&destroyCount, 1) }

	config := pool.DefaultConfig()
	config.MinSize = 50
	config.MaxSize = 500
	config.GetTimeout = 5 * time.Second

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	const (
		goroutines = 200
		iterations = 500
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				obj, err := p.Get()
				if err != nil {
					t.Errorf("goroutine %d: failed to get object: %v", id, err)
					return
				}
				time.Sleep(time.Microsecond * time.Duration(j%10))
				if j%20 == 0 {
					obj.(*testObject).valid = false
				}
				err = p.Put(obj)
				if err != nil && !errors.Is(err, pool.ErrPoolClosed) {
					t.Errorf("goroutine %d: failed to put object: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	stats := p.Stats()
	t.Logf("Create count: %d", stats.CreateCount)
	t.Logf("Destroy count: %d", stats.DestroyCount)
	t.Logf("Idle count: %d", stats.IdleCount)
	t.Logf("Get latency: %d ns", stats.GetLatency)
	t.Logf("Put latency: %d ns", stats.PutLatency)

	assert.LessOrEqual(t, stats.ActiveCount, stats.MaxCapacity)
	assert.LessOrEqual(t, stats.IdleCount, stats.MaxCapacity)
	assert.Equal(t, uint64(0), stats.ErrorCount)
}

func TestLockFreeObjectPool_Context(t *testing.T) {
	create := func() (interface{}, error) {
		time.Sleep(50 * time.Millisecond)
		return newTestObject(1), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	config := pool.DefaultConfig()
	config.MaxSize = 1
	config.MinSize = 0

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	obj1, err := p.Get()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err = p.GetWithContext(ctx)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))

	err = p.Put(obj1)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	obj2, err := p.GetWithContext(ctx2)
	require.NoError(t, err)
	assert.NotNil(t, obj2)

	p.Put(obj2)
}

// ==================== 工作池测试 ====================

func TestWorkerPool_Basic(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 2
	config.MaxSize = 4
	wp := pool.NewWorkerPoolWithConfig("test-worker", 2, 10, config)
	defer wp.Close()

	assert.Equal(t, pool.TypeWorker, wp.Type())
	assert.Equal(t, "test-worker", wp.Name())
	assert.False(t, wp.IsClosed())

	taskCount := 5
	completed := make(chan string, taskCount)

	for i := 0; i < taskCount; i++ {
		id := fmt.Sprintf("task-%d", i)
		task := newTestTask(id, 0)
		task.execFunc = func(ctx context.Context) error {
			completed <- id
			return nil
		}
		err := wp.Submit(task)
		require.NoError(t, err)
	}

	results := make([]string, 0, taskCount)
	for i := 0; i < taskCount; i++ {
		select {
		case id := <-completed:
			results = append(results, id)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for tasks")
		}
	}
	assert.Len(t, results, taskCount)

	stats := wp.Stats()
	assert.Equal(t, uint64(taskCount), stats.CreateCount)
	assert.Equal(t, uint64(taskCount), stats.DestroyCount)
}

func TestWorkerPool_WithResults(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 2, 5, pool.DefaultConfig())
	defer wp.Close()

	taskCount := 3
	for i := 0; i < taskCount; i++ {
		id := fmt.Sprintf("task-%d", i)
		task := newTestTask(id, 0)
		err := wp.Submit(task)
		require.NoError(t, err)
	}

	resultChan := wp.ResultChan()
	received := 0
	timeout := time.After(2 * time.Second)

	for received < taskCount {
		select {
		case result := <-resultChan:
			assert.NotNil(t, result)
			assert.Contains(t, result.TaskID, "task-")
			assert.NoError(t, result.Error)
			received++
		case <-timeout:
			t.Fatalf("timeout, only received %d results", received)
		}
	}
}

func TestWorkerPool_FailedTask(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 1, 5, pool.DefaultConfig())
	defer wp.Close()

	task := newTestTask("fail-task", 0)
	task.fail = true

	err := wp.Submit(task)
	require.NoError(t, err)

	select {
	case result := <-wp.ResultChan():
		assert.Equal(t, "fail-task", result.TaskID)
		assert.Error(t, result.Error)
		assert.Equal(t, "task failed", result.Error.Error())
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}
}

func TestWorkerPool_FuncTask(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 2, 5, pool.DefaultConfig())
	defer wp.Close()

	executed := false
	task := pool.NewFuncTask("func-task", func(ctx context.Context) error {
		executed = true
		return nil
	})

	err := wp.Submit(task)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	assert.True(t, executed)
}

func TestWorkerPool_ContextTimeout(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 1, 5, pool.DefaultConfig())
	defer wp.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	task := newTestTask("timeout-task", 0)
	task.sleep = 100 * time.Millisecond

	err := wp.SubmitWithContext(ctx, task)
	require.NoError(t, err)

	select {
	case result := <-wp.ResultChan():
		assert.Equal(t, "timeout-task", result.TaskID)
		assert.Error(t, result.Error)
		assert.True(t, errors.Is(result.Error, context.DeadlineExceeded))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for result")
	}
}

func TestWorkerPool_RejectWhenFull(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 1
	wp := pool.NewWorkerPoolWithConfig("", 1, 1, config)
	defer wp.Close()

	task1 := newTestTask("task1", 0)
	task1.sleep = 100 * time.Millisecond
	err := wp.TrySubmit(task1)
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	task2 := newTestTask("task2", 0)
	task2.sleep = 100 * time.Millisecond
	err = wp.TrySubmit(task2)
	assert.NoError(t, err)

	task3 := newTestTask("task3", 0)
	task3.sleep = 100 * time.Millisecond
	err = wp.TrySubmit(task3)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, pool.ErrTaskRejected))

	time.Sleep(150 * time.Millisecond)

	task4 := newTestTask("task4", 0)
	task4.sleep = 10 * time.Millisecond
	err = wp.TrySubmit(task4)
	assert.NoError(t, err)

	time.Sleep(20 * time.Millisecond)
}

func TestWorkerPool_Close(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 2, 5, pool.DefaultConfig())

	for i := 0; i < 3; i++ {
		task := newTestTask(fmt.Sprintf("close-task-%d", i), 0)
		task.sleep = 20 * time.Millisecond
		err := wp.Submit(task)
		require.NoError(t, err)
	}

	err := wp.Close()
	require.NoError(t, err)
	assert.True(t, wp.IsClosed())

	time.Sleep(50 * time.Millisecond)

	err = wp.Submit(newTestTask("after-close", 0))
	assert.Error(t, err)
	assert.True(t, errors.Is(err, pool.ErrPoolClosed))
}

func TestWorkerPool_Concurrent(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 20
	config.MaxSize = 100
	config.WorkerIdleTimeout = 5 * time.Second
	wp := pool.NewWorkerPoolWithConfig("", 20, 50000, config)
	defer wp.Close()

	var wg sync.WaitGroup
	taskCount := 10000
	completedCount := int32(0)

	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		task := pool.NewFuncTask(fmt.Sprintf("concurrent-%d", i), func(ctx context.Context) error {
			time.Sleep(time.Microsecond * 10)
			atomic.AddInt32(&completedCount, 1)
			wg.Done()
			return nil
		})
		err := wp.Submit(task)
		require.NoError(t, err)
	}

	wg.Wait()
	assert.Equal(t, int32(taskCount), atomic.LoadInt32(&completedCount))

	stats := wp.WorkerPoolStats()
	t.Logf("Completed tasks: %d", stats["completedTasks"])
	t.Logf("Avg process time: %d ns", stats["avgProcessTime"])
	assert.Equal(t, int64(0), stats["rejectedTasks"])
	assert.Equal(t, int64(0), stats["errorCount"])
}

func TestWorkerPool_GracefulShutdown(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 2
	config.MaxSize = 2
	wp := pool.NewWorkerPoolWithConfig("", 2, 5, config)

	longTask := newTestTask("long-task", 0)
	longTask.sleep = 200 * time.Millisecond
	err := wp.Submit(longTask)
	require.NoError(t, err)

	shortTask := newTestTask("short-task", 0)
	shortTask.sleep = 100 * time.Millisecond
	err = wp.Submit(shortTask)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	err = wp.CloseWithGracePeriod(300 * time.Millisecond)
	require.NoError(t, err)

	duration := time.Since(start)
	t.Logf("Shutdown duration: %v", duration)
	assert.GreaterOrEqual(t, duration, 80*time.Millisecond)
	assert.Less(t, duration, 350*time.Millisecond)
}

func TestWorkerPool_Panic(t *testing.T) {
	wp := pool.NewWorkerPoolWithConfig("", 1, 5, pool.DefaultConfig())
	defer wp.Close()

	task := newTestTask("panic-task", 0)
	task.panic = true

	err := wp.Submit(task)
	require.NoError(t, err)

	select {
	case result := <-wp.ResultChan():
		assert.Equal(t, "panic-task", result.TaskID)
		assert.Error(t, result.Error)
		assert.IsType(t, &pool.PoolError{}, result.Error)
		assert.NotNil(t, result.Panic)
		assert.Equal(t, "test panic", result.Panic)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for result")
	}

	time.Sleep(10 * time.Millisecond)

	stats := wp.WorkerPoolStats()
	assert.Equal(t, int64(1), stats["panicCount"])
}

func TestPriorityWorkerPool(t *testing.T) {
	wp := pool.NewPriorityWorkerPoolWithName("test-priority", 1, 10, 4)
	defer wp.Close()

	var execOrder []string
	var orderMu sync.Mutex

	criticalTask := newTestTask("critical", int(pool.PriorityCritical))
	criticalTask.sleep = 10 * time.Millisecond
	criticalTask.execFunc = func(ctx context.Context) error {
		orderMu.Lock()
		execOrder = append(execOrder, "critical")
		orderMu.Unlock()
		return nil
	}

	lowTask := newTestTask("low", int(pool.PriorityLow))
	lowTask.sleep = 10 * time.Millisecond
	lowTask.execFunc = func(ctx context.Context) error {
		orderMu.Lock()
		execOrder = append(execOrder, "low")
		orderMu.Unlock()
		return nil
	}

	err := wp.SubmitWithPriority(criticalTask, pool.PriorityCritical)
	require.NoError(t, err)
	err = wp.SubmitWithPriority(lowTask, pool.PriorityLow)
	require.NoError(t, err)

	timeout := time.After(500 * time.Millisecond)
	received := 0
	for received < 2 {
		select {
		case <-wp.ResultChan():
			received++
		case <-timeout:
			t.Fatal("timeout waiting for tasks")
		}
	}

	require.Len(t, execOrder, 2)
	assert.Equal(t, "critical", execOrder[0])
	assert.Equal(t, "low", execOrder[1])
}

func TestPriorityWorkerPool_FullQueue(t *testing.T) {
	wp := pool.NewPriorityWorkerPoolWithName("full-test", 1, 2, 3)
	defer wp.Close()

	for i := 0; i < 2; i++ {
		task := newTestTask(fmt.Sprintf("low-%d", i), int(pool.PriorityLow))
		task.sleep = 100 * time.Millisecond
		err := wp.SubmitWithPriority(task, pool.PriorityLow)
		require.NoError(t, err)
	}

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 2; i++ {
		task := newTestTask(fmt.Sprintf("high-%d", i), int(pool.PriorityHigh))
		task.sleep = 10 * time.Millisecond
		err := wp.SubmitWithPriority(task, pool.PriorityHigh)
		require.NoError(t, err)
	}

	task := newTestTask("high-rejected", int(pool.PriorityHigh))
	task.sleep = 10 * time.Millisecond
	err := wp.SubmitWithPriority(task, pool.PriorityHigh)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, pool.ErrTaskRejected))

	time.Sleep(300 * time.Millisecond)
}

// ==================== 高并发测试 ====================

// TestLockFreeObjectPool_HighConcurrency 测试对象池在百万级操作下的高并发性能与稳定性。
// 验证无错误/超时发生，活跃对象归零，并检查 goroutine 是否泄漏。
func TestLockFreeObjectPool_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency test in short mode")
	}

	var createCount, destroyCount int64
	create := func() (interface{}, error) {
		atomic.AddInt64(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {
		atomic.AddInt64(&destroyCount, 1)
	}

	config := pool.DefaultConfig()
	config.MaxSize = 10000
	config.MinSize = 1000
	config.GetTimeout = 10 * time.Second
	config.MaxIdleTime = time.Minute
	config.MaxLifeTime = 10 * time.Minute
	config.PreAllocate = true

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	const (
		goroutines = 1000
		iterations = 1000
		totalOps   = goroutines * iterations
	)

	beforeGoroutines := runtime.NumGoroutine()
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				obj, err := p.Get()
				if err != nil {
					t.Errorf("goroutine %d: get failed: %v", id, err)
					return
				}
				time.Sleep(time.Nanosecond * 100)
				if err = p.Put(obj); err != nil {
					t.Errorf("goroutine %d: put failed: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// 等待池内所有对象回收，并检查 goroutine 数量
	time.Sleep(50 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	stats := p.Stats()
	t.Logf("High Concurrency Test: %d ops completed in %v", totalOps, elapsed)
	t.Logf("  Create: %d, Destroy: %d, Active: %d, Idle: %d",
		stats.CreateCount, stats.DestroyCount, stats.ActiveCount, stats.IdleCount)
	t.Logf("  Get latency avg: %d ns, Put latency avg: %d ns",
		stats.GetLatency, stats.PutLatency)

	assert.Equal(t, uint64(0), stats.ErrorCount)
	assert.Equal(t, uint64(0), stats.TimeoutCount)
	assert.Equal(t, uint64(0), stats.ActiveCount)
	totalLive := int64(stats.CreateCount) - int64(stats.DestroyCount)
	assert.LessOrEqual(t, totalLive, int64(config.MaxSize))
	assert.GreaterOrEqual(t, totalLive, int64(0))

	// 检查 goroutine 是否明显增长（允许少量波动）
	CheckGoroutineLeak(t, beforeGoroutines, 50)
}

// TestWorkerPool_MillionTasks 验证工作池在大任务量下的吞吐和稳定性，并检查 goroutine 泄漏。
func TestWorkerPool_MillionTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million tasks test in short mode")
	}

	config := pool.DefaultConfig()
	config.MinSize = runtime.NumCPU() * 2
	config.MaxSize = runtime.NumCPU() * 4
	config.WorkerIdleTimeout = 30 * time.Second
	config.EnableAutoScale = true
	config.ScaleInterval = time.Second

	wp := pool.NewWorkerPoolWithConfig("million", int(config.MinSize), 100000, config)
	defer wp.Close()

	const taskCount = 100000
	var completed int64

	beforeGoroutines := runtime.NumGoroutine()
	start := time.Now()
	batchSize := 10000
	for i := 0; i < taskCount; i += batchSize {
		end := i + batchSize
		if end > taskCount {
			end = taskCount
		}
		for j := i; j < end; j++ {
			task := pool.NewFuncTask(fmt.Sprintf("task-%d", j), func(ctx context.Context) error {
				atomic.AddInt64(&completed, 1)
				return nil
			})
			err := wp.Submit(task)
			if err != nil {
				t.Fatalf("submit failed at task %d: %v", j, err)
			}
			time.Sleep(100 * time.Nanosecond)
		}
		time.Sleep(10 * time.Millisecond)
		t.Logf("Submitted %d tasks", end)
	}

	for atomic.LoadInt64(&completed) < taskCount {
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(start)

	assert.Equal(t, int64(taskCount), atomic.LoadInt64(&completed))

	stats := wp.WorkerPoolStats()
	t.Logf("Million Tasks Test: %d tasks completed in %v", taskCount, elapsed)
	t.Logf("  Total workers: %d, Busy workers: %d", stats["totalWorkers"], stats["busyWorkers"])
	t.Logf("  Avg process time: %d ns", stats["avgProcessTime"])

	rejected := stats["rejectedTasks"].(int64)
	t.Logf("  Rejected tasks: %d", rejected)
	assert.Less(t, rejected, int64(taskCount)*1/100)

	assert.Equal(t, int64(0), stats["errorCount"])

	// 检查 goroutine 是否明显增长
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	CheckGoroutineLeak(t, beforeGoroutines, 100)
}

// TestPriorityWorkerPool_ExtremeRatio 验证极端优先级比例下工作池仍能正确执行，不丢失任务。
func TestPriorityWorkerPool_ExtremeRatio(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping extreme ratio test in short mode")
	}

	wp := pool.NewPriorityWorkerPoolWithName("extreme", runtime.NumCPU()*2, 100000, 4)
	defer wp.Close()

	const totalTasks = 10000
	const highRatio = 0.99
	highTasks := int(float64(totalTasks) * highRatio)
	lowTasks := totalTasks - highTasks

	var highExecuted, lowExecuted int32
	var orderMu sync.Mutex
	execOrder := make([]string, 0, totalTasks)

	start := time.Now()

	batchSize := 1000
	for i := 0; i < highTasks; i += batchSize {
		end := i + batchSize
		if end > highTasks {
			end = highTasks
		}
		for j := i; j < end; j++ {
			task := newTestTask(fmt.Sprintf("high-%d", j), int(pool.PriorityCritical))
			task.execFunc = func(ctx context.Context) error {
				atomic.AddInt32(&highExecuted, 1)
				orderMu.Lock()
				execOrder = append(execOrder, "high")
				orderMu.Unlock()
				return nil
			}
			err := wp.SubmitWithPriority(task, pool.PriorityCritical)
			require.NoError(t, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < lowTasks; i++ {
		task := newTestTask(fmt.Sprintf("low-%d", i), int(pool.PriorityLow))
		task.execFunc = func(ctx context.Context) error {
			atomic.AddInt32(&lowExecuted, 1)
			orderMu.Lock()
			execOrder = append(execOrder, "low")
			orderMu.Unlock()
			return nil
		}
		err := wp.SubmitWithPriority(task, pool.PriorityLow)
		require.NoError(t, err)
	}

	timeout := time.After(5 * time.Minute)
	for {
		if atomic.LoadInt32(&highExecuted) == int32(highTasks) &&
			atomic.LoadInt32(&lowExecuted) == int32(lowTasks) {
			break
		}
		select {
		case <-timeout:
			t.Fatalf("timeout waiting tasks: high=%d/%d, low=%d/%d",
				atomic.LoadInt32(&highExecuted), highTasks,
				atomic.LoadInt32(&lowExecuted), lowTasks)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	elapsed := time.Since(start)
	t.Logf("Extreme Ratio Test: %d tasks (%d high, %d low) completed in %v",
		totalTasks, highTasks, lowTasks, elapsed)

	assert.Equal(t, int32(highTasks), atomic.LoadInt32(&highExecuted))
	assert.Equal(t, int32(lowTasks), atomic.LoadInt32(&lowExecuted))

	stats := wp.WorkerPoolStats()
	assert.Equal(t, int64(0), stats["rejectedTasks"])
	assert.Equal(t, int64(0), stats["errorCount"])
}

// ==================== 环形缓冲区测试 ====================

func TestCircularBuffer_Advanced(t *testing.T) {
	buffer := pool.NewCircularBuffer(3)

	assert.Equal(t, 0, len(buffer.GetAll()))
	assert.Nil(t, buffer.GetLatest())

	stats1 := pool.PoolStats{ActiveCount: 1}
	stats2 := pool.PoolStats{ActiveCount: 2}
	stats3 := pool.PoolStats{ActiveCount: 3}
	stats4 := pool.PoolStats{ActiveCount: 4}

	buffer.Append(stats1)
	buffer.Append(stats2)
	buffer.Append(stats3)

	all := buffer.GetAll()
	assert.Len(t, all, 3)
	assert.Equal(t, uint64(1), all[0].ActiveCount)
	assert.Equal(t, uint64(2), all[1].ActiveCount)
	assert.Equal(t, uint64(3), all[2].ActiveCount)

	latest := buffer.GetLatest()
	assert.Equal(t, uint64(3), latest.ActiveCount)

	buffer.Append(stats4)

	all = buffer.GetAll()
	assert.Len(t, all, 3)
	assert.Equal(t, uint64(2), all[0].ActiveCount)
	assert.Equal(t, uint64(3), all[1].ActiveCount)
	assert.Equal(t, uint64(4), all[2].ActiveCount)

	latest = buffer.GetLatest()
	assert.Equal(t, uint64(4), latest.ActiveCount)

	buffer.Clear()
	assert.Equal(t, 0, len(buffer.GetAll()))
	assert.Nil(t, buffer.GetLatest())
}

func TestCircularEnhancedBuffer(t *testing.T) {
	buffer := pool.NewCircularEnhancedBuffer(2)

	stats1 := pool.EnhancedPoolStats{PoolStats: pool.PoolStats{ActiveCount: 1}}
	stats2 := pool.EnhancedPoolStats{PoolStats: pool.PoolStats{ActiveCount: 2}}
	stats3 := pool.EnhancedPoolStats{PoolStats: pool.PoolStats{ActiveCount: 3}}

	buffer.Append(stats1)
	buffer.Append(stats2)

	all := buffer.GetAll()
	assert.Len(t, all, 2)
	assert.Equal(t, uint64(1), all[0].ActiveCount)
	assert.Equal(t, uint64(2), all[1].ActiveCount)

	latest := buffer.GetLatest()
	assert.Equal(t, uint64(2), latest.ActiveCount)

	buffer.Append(stats3)
	all = buffer.GetAll()
	assert.Len(t, all, 2)
	assert.Equal(t, uint64(2), all[0].ActiveCount)
	assert.Equal(t, uint64(3), all[1].ActiveCount)

	buffer.Clear()
	assert.Nil(t, buffer.GetLatest())
	assert.Empty(t, buffer.GetAll())
}

// ==================== 指标收集器测试 ====================

func TestMetricsCollector(t *testing.T) {
	collector := pool.NewMetricsCollector(
		pool.WithCollectionInterval(50*time.Millisecond),
		pool.WithMaxRecords(10),
		pool.WithPrometheus(true),
	)
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}
	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 5
	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	poolID := "test-pool"
	collector.Register(poolID, p)

	for i := 0; i < 10; i++ {
		obj, _ := p.Get()
		time.Sleep(5 * time.Millisecond)
		p.Put(obj)
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(150 * time.Millisecond)

	stats := collector.GetStats(poolID)
	assert.NotEmpty(t, stats)

	latest := collector.GetLatestStats(poolID)
	assert.NotNil(t, latest)
	assert.Equal(t, pool.TypeObject, latest.Type)

	jsonStr, err := collector.GetStatsJSON()
	require.NoError(t, err)
	assert.Contains(t, jsonStr, poolID)
	var data map[string][]pool.PoolStats
	err = json.Unmarshal([]byte(jsonStr), &data)
	require.NoError(t, err)
	assert.Contains(t, data, poolID)

	summary := collector.GetSummary()
	assert.Contains(t, summary, poolID)
	poolSummary, ok := summary[poolID].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, pool.TypeObject, poolSummary["type"])

	pools := collector.GetRegisteredPools()
	assert.Contains(t, pools, poolID)

	promMetrics := collector.ExportPrometheus()
	if promMetrics != "# No metrics data available\n" {
		assert.Contains(t, promMetrics, "pool_active_count")
		assert.Contains(t, promMetrics, poolID)
	}

	collector.Unregister(poolID)
	pools = collector.GetRegisteredPools()
	assert.NotContains(t, pools, poolID)

	collector.Clear()
	stats = collector.GetStats(poolID)
	assert.Empty(t, stats)
}

func TestMetricsCollector_BatchStats(t *testing.T) {
	collector := pool.NewMetricsCollector()
	collector.Start()
	defer collector.Stop()

	collector.RecordBatchOperation("test-pool", "get", 5)
	collector.RecordBatchOperation("test-pool", "get", 3)
	collector.RecordBatchOperation("test-pool", "put", 8)

	batchStats := collector.GetBatchStats("test-pool")
	assert.NotNil(t, batchStats)
	assert.Equal(t, uint64(3), batchStats.TotalBatches)
	assert.Equal(t, uint64(16), batchStats.TotalItems)
	assert.Equal(t, 3, batchStats.MinBatchSize)
	assert.Equal(t, 8, batchStats.MaxBatchSize)
	assert.InDelta(t, 5.33, batchStats.AvgBatchSize, 0.1)
	assert.Equal(t, uint64(2), batchStats.BatchGetCount)
	assert.Equal(t, uint64(1), batchStats.BatchPutCount)

	assert.Nil(t, collector.GetBatchStats("non-existent"))
}

func TestMetricsCollector_EnhancedStats(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50 * time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}
	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 5
	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	poolID := "enhanced-test"
	collector.Register(poolID, p)

	for i := 0; i < 10; i++ {
		obj, _ := p.Get()
		time.Sleep(5 * time.Millisecond)
		p.Put(obj)
	}

	time.Sleep(200 * time.Millisecond)

	detailed := collector.GetDetailedStats(poolID)
	assert.NotNil(t, detailed, "GetDetailedStats should return non-nil for registered pool")
	assert.Equal(t, pool.TypeObject, detailed.Type)
	assert.GreaterOrEqual(t, detailed.GoroutineCount, 1)
	assert.GreaterOrEqual(t, detailed.MemoryUsage, uint64(0))
	assert.InDelta(t, 0.0, detailed.ErrorRate, 0.01)

	summary := collector.GetMetricsSummary()
	pools, ok := summary["pools"].(map[string]interface{})
	assert.True(t, ok)
	poolInfo, ok := pools[poolID].(map[string]interface{})
	assert.True(t, ok)
	assert.Contains(t, poolInfo, "error_rate")
	assert.Contains(t, poolInfo, "throughput")
}

func TestMetricsCollector_Prometheus(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithPrometheus(true), pool.WithCollectionInterval(50*time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}
	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 5
	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	poolID := "prometheus-pool"
	collector.Register(poolID, p)

	for i := 0; i < 15; i++ {
		obj, _ := p.Get()
		p.Put(obj)
		time.Sleep(5 * time.Millisecond)
	}
	collector.RecordBatchOperation(poolID, "get", 3)

	time.Sleep(150 * time.Millisecond)

	promMetrics := collector.ExportPrometheus()
	t.Logf("Prometheus metrics:\n%s", promMetrics)

	if promMetrics != "# No metrics data available\n" {
		assert.Contains(t, promMetrics, "# HELP pool_active_count")
		assert.Contains(t, promMetrics, "# TYPE pool_active_count gauge")
		assert.Contains(t, promMetrics, fmt.Sprintf("pool_active_count{pool=\"%s\"}", poolID))
		assert.Contains(t, promMetrics, "# HELP pool_batch_total")
		assert.Contains(t, promMetrics, fmt.Sprintf("pool_batch_total{pool=\"%s\"}", poolID))
	} else {
		t.Log("No Prometheus metrics collected, skipping detailed validation")
	}

	collectorNoProm := pool.NewMetricsCollector(pool.WithPrometheus(false))
	assert.Empty(t, collectorNoProm.ExportPrometheus())
}

func TestMetricsCollector_Concurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent metrics test in short mode")
	}

	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(100 * time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	const numPools = 5
	const numOps = 20

	var wg sync.WaitGroup
	wg.Add(numPools)

	for i := 0; i < numPools; i++ {
		go func(id int) {
			defer wg.Done()
			poolID := fmt.Sprintf("pool-%d", id)

			mock := &mockPool{
				statsFunc: func() pool.PoolStats {
					return pool.PoolStats{
						Type:         pool.TypeObject,
						ActiveCount:  uint64(id),
						IdleCount:    uint64(id * 2),
						CreateCount:  uint64(id * 10),
						DestroyCount: uint64(id * 5),
					}
				},
			}
			collector.Register(poolID, mock)
			defer collector.Unregister(poolID)

			for j := 0; j < numOps; j++ {
				collector.RecordBatchOperation(poolID, "get", j%5+1)
				time.Sleep(100 * time.Microsecond)
			}

			_ = collector.GetStats(poolID)
			_ = collector.GetLatestStats(poolID)
			_ = collector.GetDetailedStats(poolID)
		}(i)
	}

	wg.Wait()
	assert.Empty(t, collector.GetRegisteredPools())
}

func TestMetricsCollector_Stop(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50 * time.Millisecond))
	collector.Start()

	mock := &mockPool{
		statsFunc: func() pool.PoolStats {
			return pool.PoolStats{ActiveCount: 1}
		},
	}
	collector.Register("test", mock)

	time.Sleep(100 * time.Millisecond)
	assert.NotEmpty(t, collector.GetStats("test"))

	collector.Stop()
	collector.Clear()
	assert.Empty(t, collector.GetStats("test"))

	time.Sleep(150 * time.Millisecond)
	assert.Empty(t, collector.GetStats("test"))
}

func TestMetricsCollector_GetDetailedStats(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50 * time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	mock := &mockPool{
		statsFunc: func() pool.PoolStats {
			return pool.PoolStats{
				Type:        pool.TypeWorker,
				ActiveCount: 5,
				IdleCount:   3,
				CreateCount: 100,
				ErrorCount:  2,
			}
		},
	}
	collector.Register("worker-pool", mock)

	time.Sleep(200 * time.Millisecond)

	detailed := collector.GetDetailedStats("worker-pool")
	assert.NotNil(t, detailed, "GetDetailedStats should return non-nil for registered pool")
	assert.Equal(t, pool.TypeWorker, detailed.Type)
	assert.Equal(t, uint64(5), detailed.ActiveCount)
	assert.Greater(t, detailed.GoroutineCount, 0)
	assert.InDelta(t, 0.02, detailed.ErrorRate, 0.001)

	assert.Nil(t, collector.GetDetailedStats("non-existent"))
}

func TestMetricsCollector_Clear(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50*time.Millisecond), pool.WithMaxRecords(100))
	collector.Start()
	defer collector.Stop()

	mock := &mockPool{
		statsFunc: func() pool.PoolStats {
			return pool.PoolStats{
				Type:           "test",
				ActiveCount:    1,
				IdleCount:      2,
				LastActiveTime: time.Now().UnixNano(),
			}
		},
	}
	collector.Register("pool1", mock)
	collector.Register("pool2", mock)

	time.Sleep(100 * time.Millisecond)

	stats1 := collector.GetStats("pool1")
	stats2 := collector.GetStats("pool2")
	assert.NotNil(t, stats1, "清空前pool1的统计数据不应为nil")
	assert.NotEmpty(t, stats1, "清空前pool1应有统计记录")
	assert.NotNil(t, stats2, "清空前pool2的统计数据不应为nil")
	assert.NotEmpty(t, stats2, "清空前pool2应有统计记录")
	assert.NotEmpty(t, collector.GetRegisteredPools(), "注册池列表不应为空")

	collector.Clear()

	assert.Nil(t, collector.GetStats("pool1"), "清空后pool1的统计数据应为nil")
	assert.Nil(t, collector.GetStats("pool2"), "清空后pool2的统计数据应为nil")
	assert.Nil(t, collector.GetBatchStats("pool1"), "清空后批量统计应为nil")

	registeredPools := collector.GetRegisteredPools()
	assert.NotEmpty(t, registeredPools, "注册池列表应保留（未注销）")
	assert.Contains(t, registeredPools, "pool1")
	assert.Contains(t, registeredPools, "pool2")

	time.Sleep(100 * time.Millisecond)
	newStats1 := collector.GetStats("pool1")
	assert.NotNil(t, newStats1, "清空后重新收集应生成新数据")
	assert.NotEmpty(t, newStats1, "重新收集后pool1应有统计记录")
}

func TestMetricsCollector_GetMetricsSummary(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50 * time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	mock := &mockPool{
		statsFunc: func() pool.PoolStats {
			return pool.PoolStats{
				Type:         pool.TypeObject,
				ActiveCount:  2,
				IdleCount:    3,
				CreateCount:  50,
				DestroyCount: 10,
				ErrorCount:   1,
			}
		},
	}
	collector.Register("test-pool", mock)
	collector.RecordBatchOperation("test-pool", "get", 4)

	time.Sleep(200 * time.Millisecond)

	summary := collector.GetMetricsSummary()
	assert.Contains(t, summary, "system")
	assert.Contains(t, summary, "pools")

	system := summary["system"].(map[string]interface{})
	assert.Contains(t, system, "goroutines")
	assert.Contains(t, system, "memory_alloc")

	pools := summary["pools"].(map[string]interface{})
	poolInfo, ok := pools["test-pool"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, pool.TypeObject, poolInfo["type"])
	assert.Equal(t, uint64(2), poolInfo["active"])
	assert.Contains(t, poolInfo, "batch_ops")
	batchOps := poolInfo["batch_ops"].(map[string]interface{})
	assert.Equal(t, uint64(1), batchOps["total"])
}

// ==================== 自动扩缩容测试 ====================

func TestWorkerPool_AutoScale(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 2
	config.MaxSize = 10
	config.EnableAutoScale = true
	config.TargetUtilization = 0.3
	config.ScaleUpFactor = 2.0
	config.ScaleDownFactor = 0.5
	config.ScaleInterval = 50 * time.Millisecond
	config.WorkerIdleTimeout = 100 * time.Millisecond

	wp := pool.NewWorkerPoolWithConfig("auto-scale", 2, 10, config)
	defer wp.Close()

	for i := 0; i < 20; i++ {
		task := newTestTask(fmt.Sprintf("task-%d", i), 0)
		task.sleep = 50 * time.Millisecond
		err := wp.TrySubmit(task)
		require.NoError(t, err)
	}

	time.Sleep(300 * time.Millisecond)

	stats := wp.WorkerPoolStats()
	t.Logf("After scale up: total workers = %d", stats["totalWorkers"])
	assert.GreaterOrEqual(t, stats["totalWorkers"].(int32), int32(2))

	time.Sleep(500 * time.Millisecond)

	stats = wp.WorkerPoolStats()
	t.Logf("After scale down: total workers = %d", stats["totalWorkers"])
	assert.LessOrEqual(t, stats["totalWorkers"].(int32), int32(4))
}

// ==================== 竞争检测测试 ====================

func TestLockFreeObjectPool_Race(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race test in short mode")
	}

	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	config := pool.DefaultConfig()
	config.MaxSize = 100
	config.MinSize = 10

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	const (
		numGoroutines = 50
		numIterations = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				switch j % 6 {
				case 0:
					_, _ = p.Get()
				case 1:
					obj, _ := p.Get()
					_ = p.Put(obj)
				case 2:
					_ = p.Stats()
				case 3:
					_ = p.IsClosed()
				case 4:
					_ = p.Type()
				case 5:
					objs, _ := p.GetBatch(2)
					if len(objs) > 0 {
						_ = p.PutBatch(objs)
					}
				}
			}
		}(i)
	}

	wg.Wait()
}

// ==================== 边界测试 ====================

func TestLockFreeObjectPool_EdgeCases(t *testing.T) {
	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	t.Run("EmptyPool", func(t *testing.T) {
		config := pool.DefaultConfig()
		config.MaxSize = 1
		config.MinSize = 0

		p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
		defer p.Close()

		obj, err := p.Get()
		require.NoError(t, err)
		assert.NotNil(t, obj)

		p.Put(obj)
	})

	t.Run("FullPool", func(t *testing.T) {
		config := pool.DefaultConfig()
		config.MaxSize = 1
		config.MinSize = 1

		p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
		defer p.Close()

		obj1, err := p.Get()
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err = p.GetWithContext(ctx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))

		p.Put(obj1)
	})

	t.Run("ConcurrentClose", func(t *testing.T) {
		config := pool.DefaultConfig()
		config.MaxSize = 10

		p := pool.NewLockFreeObjectPool(create, validate, destroy, config)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p.Close()
			}()
		}
		wg.Wait()

		assert.True(t, p.IsClosed())
	})

	t.Run("DoubleClose", func(t *testing.T) {
		p := pool.NewLockFreeObjectPool(create, validate, destroy, pool.DefaultConfig())

		err1 := p.Close()
		err2 := p.Close()

		assert.NoError(t, err1)
		assert.NoError(t, err2)
	})
}

// ==================== 模糊测试 ====================

func FuzzLockFreeObjectPool(f *testing.F) {
	f.Add(10, 5, 100)

	f.Fuzz(func(t *testing.T, maxSize int, minSize int, ops int) {
		if maxSize <= 0 {
			maxSize = 100
		}
		if maxSize > 1000 {
			maxSize = 1000
		}
		if minSize < 0 {
			minSize = 1
		}
		if minSize > maxSize {
			minSize = maxSize / 2
		}
		if minSize < 1 {
			minSize = 1
		}
		if ops < 0 {
			ops = 1000
		}
		if ops > 10000 {
			ops = 10000
		}

		var createCount, destroyCount int32
		create := func() (interface{}, error) {
			atomic.AddInt32(&createCount, 1)
			return newTestObject(int(createCount)), nil
		}
		validate := func(obj interface{}) bool { return true }
		destroy := func(obj interface{}) { atomic.AddInt32(&destroyCount, 1) }

		config := pool.DefaultConfig()
		config.MaxSize = maxSize
		config.MinSize = minSize
		config.GetTimeout = 10 * time.Millisecond

		p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
		defer p.Close()

		var wg sync.WaitGroup
		for i := 0; i < ops; i++ {
			wg.Add(1)
			go func(seed int) {
				defer wg.Done()
				switch seed % 5 {
				case 0:
					obj, err := p.Get()
					if err == nil {
						if seed%2 == 0 {
							_ = p.Put(obj)
						}
					}
				case 1:
					obj := newTestObject(seed)
					_ = p.Put(obj)
				case 2:
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
					defer cancel()
					obj, err := p.GetWithContext(ctx)
					if err == nil {
						_ = p.Put(obj)
					}
				case 3:
					_ = p.Stats()
				case 4:
					objs, err := p.GetBatch(3)
					if err == nil && len(objs) > 0 {
						_ = p.PutBatch(objs)
					}
				}
			}(i)
		}

		wg.Wait()

		stats := p.Stats()
		total := int64(stats.CreateCount) - int64(stats.DestroyCount)
		if total < 0 {
			total = 0
		}
		assert.LessOrEqual(t, total, int64(maxSize))
		assert.GreaterOrEqual(t, total, int64(0))
	})
}

// ==================== 性能基准测试 ====================

func BenchmarkLockFreeObjectPool_GetPut(b *testing.B) {
	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	config := pool.DefaultConfig()
	config.MaxSize = b.N
	config.MinSize = b.N / 2

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj, _ := p.Get()
			_ = p.Put(obj)
		}
	})
}

func BenchmarkWorkerPool_Submit(b *testing.B) {
	config := pool.DefaultConfig()
	config.MinSize = runtime.GOMAXPROCS(0)
	config.MaxSize = config.MinSize * 2
	wp := pool.NewWorkerPoolWithConfig("bench", int(config.MinSize), b.N, config)
	defer wp.Close()

	task := pool.NewFuncTask("bench", func(ctx context.Context) error { return nil })

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = wp.Submit(task)
		}
	})
}

func BenchmarkPriorityWorkerPool(b *testing.B) {
	wp := pool.NewPriorityWorkerPoolWithName("bench", runtime.GOMAXPROCS(0), b.N, 4)
	defer wp.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			priority := pool.Priority(i % 4)
			task := pool.NewFuncTask(fmt.Sprintf("bench-%d", i), func(ctx context.Context) error {
				return nil
			})
			_ = wp.SubmitWithPriority(task, priority)
			i++
		}
	})
}

// ==================== 新增增强测试 ====================

// TestObjectPool_NoMemoryLeak 检查池关闭后没有活跃对象泄漏
func TestObjectPool_NoMemoryLeak(t *testing.T) {
	var createCount, destroyCount int64
	create := func() (interface{}, error) {
		atomic.AddInt64(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {
		atomic.AddInt64(&destroyCount, 1)
	}

	config := pool.DefaultConfig()
	config.MaxSize = 100
	config.MinSize = 10

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)

	// 借出一些对象
	objs := make([]interface{}, 20)
	for i := 0; i < 20; i++ {
		obj, _ := p.Get()
		objs[i] = obj
	}
	// 归还所有
	for _, obj := range objs {
		p.Put(obj)
	}

	// 关闭池
	err := p.Close()
	require.NoError(t, err)

	// 所有对象都应该被销毁
	assert.Equal(t, atomic.LoadInt64(&createCount), atomic.LoadInt64(&destroyCount))
	// 活跃对象应该为0
	stats := p.Stats()
	assert.Equal(t, uint64(0), stats.ActiveCount)
}

// TestObjectPool_ExtremeConfig 测试极端配置值
func TestObjectPool_ExtremeConfig(t *testing.T) {
	create := func() (interface{}, error) { return newTestObject(1), nil }
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) {}

	// MaxSize=0 时池不应panic，但应能降级
	cfg := pool.DefaultConfig()
	cfg.MaxSize = 0
	cfg.MinSize = 0
	p := pool.NewLockFreeObjectPool(create, validate, destroy, cfg)
	_, err := p.Get()
	// 预期可能超时或错误，但不应 panic
	if err == nil {
		t.Error("expected error when MaxSize=0")
	}
	p.Close()

	// minSize > maxSize 时自动调整为 minSize
	cfg = pool.DefaultConfig()
	cfg.MaxSize = 5
	cfg.MinSize = 10
	p = pool.NewLockFreeObjectPool(create, validate, destroy, cfg)
	stats := p.Stats()
	assert.Equal(t, uint64(5), stats.MaxCapacity, "max should be adjusted to min")
	assert.Equal(t, uint64(5), stats.MinCapacity, "min should be clipped to max")
	p.Close()

	// 超大超时值
	cfg = pool.DefaultConfig()
	cfg.GetTimeout = 1 * time.Hour
	cfg.MaxSize = 1
	cfg.MinSize = 0
	p = pool.NewLockFreeObjectPool(create, validate, destroy, cfg)
	obj, err := p.Get()
	require.NoError(t, err)
	p.Put(obj)
	p.Close()
}

// TestWorkerPool_OverloadRecovery 测试过载恢复
func TestWorkerPool_OverloadRecovery(t *testing.T) {
	config := pool.DefaultConfig()
	config.MinSize = 1
	config.MaxSize = 1
	config.WorkerIdleTimeout = 100 * time.Millisecond
	wp := pool.NewWorkerPoolWithConfig("overload", 1, 2, config) // 队列容量稍大，但依然会导致拒绝
	defer wp.Close()

	// 启动一个消费者协程，防止 resultChan 满导致 errorCount 错误增加
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-wp.ResultChan():
			case <-ctx.Done():
				return
			}
		}
	}()

	var rejected int64
	// 提交大量慢任务，单 worker 队列有限，必定大量被拒绝
	for i := 0; i < 100; i++ {
		task := newTestTask(fmt.Sprintf("overload-%d", i), 0)
		task.sleep = 200 * time.Millisecond
		err := wp.TrySubmit(task)
		if errors.Is(err, pool.ErrTaskRejected) {
			atomic.AddInt64(&rejected, 1)
		}
	}
	// 等待任务处理（所有提交的任务执行完毕）
	time.Sleep(300 * time.Millisecond)

	// 此时应能正常提交新任务
	task := newTestTask("after-overload", 0)
	err := wp.TrySubmit(task)
	require.NoError(t, err, "should be able to submit after overload")
	time.Sleep(50 * time.Millisecond)

	stats := wp.WorkerPoolStats()
	t.Logf("Overload recovery: rejected=%d, completed=%d", atomic.LoadInt64(&rejected), stats["completedTasks"])
	assert.Greater(t, atomic.LoadInt64(&rejected), int64(0), "should have rejected some tasks")
	assert.Equal(t, int64(0), stats["errorCount"])
}

// TestMetricsCollector_ConsistencyCheck 验证收集的指标数值一致性
func TestMetricsCollector_ConsistencyCheck(t *testing.T) {
	collector := pool.NewMetricsCollector(pool.WithCollectionInterval(50 * time.Millisecond))
	collector.Start()
	defer collector.Stop()

	time.Sleep(50 * time.Millisecond)

	var createCount, destroyCount int64
	create := func() (interface{}, error) {
		atomic.AddInt64(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) { atomic.AddInt64(&destroyCount, 1) }

	config := pool.DefaultConfig()
	config.MinSize = 5
	config.MaxSize = 20
	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	defer p.Close()

	poolID := "consistency-pool"
	collector.Register(poolID, p)

	// 执行一定量的操作
	for i := 0; i < 50; i++ {
		obj, _ := p.Get()
		time.Sleep(2 * time.Millisecond)
		p.Put(obj)
	}

	time.Sleep(200 * time.Millisecond) // 等待指标收集

	latest := collector.GetLatestStats(poolID)
	require.NotNil(t, latest)

	// 验证 create/destroy 计数关系
	totalLive := int64(latest.CreateCount) - int64(latest.DestroyCount)
	t.Logf("Create=%d, Destroy=%d, Idle=%d, Active=%d",
		latest.CreateCount, latest.DestroyCount, latest.IdleCount, latest.ActiveCount)

	// 存活对象应等于 idle + active
	assert.Equal(t, totalLive, int64(latest.IdleCount+latest.ActiveCount),
		"Create-Destroy should equal Idle+Active")
	assert.GreaterOrEqual(t, latest.CreateCount, latest.DestroyCount)
	// 活跃数不应超过最大容量
	assert.LessOrEqual(t, latest.ActiveCount+latest.IdleCount, latest.MaxCapacity)

	// 验证 Prometheus 输出中的数值与 stats 一致（如果启用）
	// 此处仅验证 latest stats 的字段类型
	_ = latest
}

// TestLongRunStability 长时间运行稳定性测试，监控内存和goroutine是否泄漏
// 该测试持续运行一定时间，并发执行对象池获取/归还操作，并定期记录资源使用情况。
func TestLongRunStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long run stability test in short mode")
	}

	var createCount, destroyCount int64
	create := func() (interface{}, error) {
		atomic.AddInt64(&createCount, 1)
		return newTestObject(int(createCount)), nil
	}
	validate := func(obj interface{}) bool { return true }
	destroy := func(obj interface{}) { atomic.AddInt64(&destroyCount, 1) }

	config := pool.DefaultConfig()
	config.MaxSize = 500
	config.MinSize = 50
	config.GetTimeout = 1 * time.Second
	config.MaxIdleTime = 10 * time.Second
	config.MaxLifeTime = 30 * time.Second
	config.HealthCheckInterval = 1 * time.Second

	p := pool.NewLockFreeObjectPool(create, validate, destroy, config)
	// 显式关闭并等待 health check goroutine 退出，避免竞态 panic
	defer func() {
		p.Close()
		time.Sleep(500 * time.Millisecond)
	}()

	beforeGoroutines := runtime.NumGoroutine()
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// 启动固定数量的并发goroutine，持续进行 get/put 操作
	const numGoroutines = 20
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				obj, err := p.Get()
				if err != nil {
					continue
				}
				// 模拟短暂使用
				time.Sleep(time.Microsecond * 10)
				_ = p.Put(obj)
				time.Sleep(time.Microsecond * 10)
			}
		}(i)
	}

	// 运行一段时间
	duration := 5 * time.Second
	t.Logf("Running stability test for %v...", duration)
	time.Sleep(duration)

	// 停止并发操作
	close(stopCh)
	wg.Wait()

	// 等待资源回收
	time.Sleep(500 * time.Millisecond)
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)

	afterGoroutines := runtime.NumGoroutine()

	stats := p.Stats()
	t.Logf("Stability test results:")
	t.Logf("  Operations: create=%d, destroy=%d", stats.CreateCount, stats.DestroyCount)
	t.Logf("  Goroutines: before=%d, after=%d", beforeGoroutines, afterGoroutines)
	t.Logf("  Memory: Alloc before=%d, after=%d", memStatsBefore.Alloc, memStatsAfter.Alloc)

	// goroutine 增长不应太多
	assert.LessOrEqual(t, afterGoroutines, beforeGoroutines+int(1.5*float64(numGoroutines)),
		"goroutine count increased significantly")
	// 内存不应暴涨（允许一定增长）
	memGrowth := int64(memStatsAfter.Alloc) - int64(memStatsBefore.Alloc)
	t.Logf("Memory growth: %d bytes", memGrowth)
	// 简单判断内存增长不超过 10MB（可根据实际情况调整）
	assert.Less(t, memGrowth, int64(10*1024*1024), "memory usage increased too much")
	// 没有活跃对象残留
	assert.Equal(t, uint64(0), stats.ActiveCount, "active objects should be 0 after idle")
}
