package app

import (
	"app/kernel/log"
	"app/kernel/pool"
	"sync"
	"time"
)

const defaultPoolBufferCap = 4096

var (
	objectPool     *pool.LockFreeObjectPool
	objectPoolOnce sync.Once
)

// initObjectPool 初始化默认对象池（仅内部使用）
func initObjectPool(create func() (any, error), validate func(any) bool, destroy func(any),
	maxSize, minSize int,
	getTimeout, maxIdleTime, maxLifeTime, healthCheckInterval time.Duration,
	enableAutoScale bool) {

	objectPoolOnce.Do(func() {
		log.Info("object_pool: initializing", "max_size", maxSize, "min_size", minSize)
		poolCfg := pool.Config{
			MaxSize:             maxSize,
			MinSize:             minSize,
			GetTimeout:          getTimeout,
			MaxIdleTime:         maxIdleTime,
			MaxLifeTime:         maxLifeTime,
			HealthCheckInterval: healthCheckInterval,
			EnableAutoScale:     enableAutoScale,
			PreAllocate:         true,
			EnableMetrics:       true,
		}
		objectPool = pool.NewLockFreeObjectPool(create, validate, destroy, poolCfg, pool.WithPoolName("app-object-pool"))
		log.Info("object_pool: initialized successfully")
	})
}

// NewObjectPool 创建新的对象池实例（独立于默认池，供业务层使用）
func NewObjectPool(create func() (any, error), validate func(any) bool, destroy func(any),
	maxSize, minSize int,
	getTimeout, maxIdleTime, maxLifeTime, healthCheckInterval time.Duration,
	enableAutoScale bool) *pool.LockFreeObjectPool {

	poolCfg := pool.Config{
		MaxSize:             maxSize,
		MinSize:             minSize,
		GetTimeout:          getTimeout,
		MaxIdleTime:         maxIdleTime,
		MaxLifeTime:         maxLifeTime,
		HealthCheckInterval: healthCheckInterval,
		EnableAutoScale:     enableAutoScale,
		PreAllocate:         true,
		EnableMetrics:       true,
	}
	return pool.NewLockFreeObjectPool(create, validate, destroy, poolCfg, pool.WithPoolName("app-object-pool"))
}

// GetBuffer 从默认对象池获取一个字节切片（容量至少为 size）
// 简化逻辑，去掉无意义的重试
func GetBuffer(size int) []byte {
	if size > defaultPoolBufferCap {
		return make([]byte, size)
	}
	if objectPool == nil {
		return make([]byte, size)
	}

	obj, err := objectPool.Get()
	if err != nil {
		return make([]byte, size)
	}
	buf := obj.([]byte)
	if cap(buf) >= defaultPoolBufferCap {
		return buf[:size]
	}
	// 容量不对，归还后新建
	objectPool.Put(buf[:0])
	return make([]byte, size)
}

// PutBuffer 归还字节切片到默认对象池
func PutBuffer(buf []byte) {
	if objectPool == nil {
		return
	}
	if cap(buf) != defaultPoolBufferCap {
		return
	}
	objectPool.Put(buf[:0])
}
