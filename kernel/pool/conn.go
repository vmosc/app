package pool

import (
	"context"
	"time"
)

// Conn 通用连接接口。
type Conn interface {
	Close() error
	IsAlive() bool
}

// ConnPool 通用连接池。
type ConnPool struct {
	pool       *LockFreeObjectPool
	dial       func() (Conn, error)
	closeConn  func(Conn)
	checkAlive func(Conn) bool
}

// ConnPoolConfig 连接池配置。
type ConnPoolConfig struct {
	MaxIdle           int
	MaxActive         int
	GetTimeout        time.Duration
	MaxIdleTime       time.Duration
	MaxLifeTime       time.Duration
	HealthCheck       time.Duration
	EnableAutoScale   bool
	TargetUtilization float64
	ScaleUpFactor     float64
	ScaleDownFactor   float64
	ScaleInterval     time.Duration
}

// DefaultConnPoolConfig 默认连接池配置。
func DefaultConnPoolConfig() ConnPoolConfig {
	return ConnPoolConfig{
		MaxIdle:           10,
		MaxActive:         100,
		GetTimeout:        30 * time.Second,
		MaxIdleTime:       5 * time.Minute,
		MaxLifeTime:       30 * time.Minute,
		HealthCheck:       1 * time.Minute,
		EnableAutoScale:   false,
		TargetUtilization: 0.7,
		ScaleUpFactor:     1.5,
		ScaleDownFactor:   0.7,
		ScaleInterval:     30 * time.Second,
	}
}

// NewConnPool 创建连接池。
func NewConnPool(dial func() (Conn, error), closeConn func(Conn), checkAlive func(Conn) bool, config ConnPoolConfig) *ConnPool {
	if closeConn == nil {
		closeConn = func(c Conn) { c.Close() }
	}
	if checkAlive == nil {
		checkAlive = func(c Conn) bool { return c.IsAlive() }
	}

	objConfig := DefaultConfig()
	objConfig.MaxSize = config.MaxActive
	objConfig.MinSize = config.MaxIdle
	objConfig.GetTimeout = config.GetTimeout
	objConfig.MaxIdleTime = config.MaxIdleTime
	objConfig.MaxLifeTime = config.MaxLifeTime
	objConfig.HealthCheckInterval = config.HealthCheck
	objConfig.EnableAutoScale = config.EnableAutoScale
	objConfig.TargetUtilization = config.TargetUtilization
	objConfig.ScaleUpFactor = config.ScaleUpFactor
	objConfig.ScaleDownFactor = config.ScaleDownFactor
	objConfig.ScaleInterval = config.ScaleInterval

	create := func() (interface{}, error) {
		return dial()
	}
	validate := func(obj interface{}) bool {
		return checkAlive(obj.(Conn))
	}
	destroy := func(obj interface{}) {
		if obj != nil {
			closeConn(obj.(Conn))
		}
	}

	objPool := NewLockFreeObjectPool(create, validate, destroy, objConfig, WithPoolName("conn-pool"))

	return &ConnPool{
		pool:       objPool,
		dial:       dial,
		closeConn:  closeConn,
		checkAlive: checkAlive,
	}
}

// Get 获取一个连接。
func (cp *ConnPool) Get() (Conn, error) {
	obj, err := cp.pool.Get()
	if err != nil {
		return nil, err
	}
	conn := obj.(Conn)
	if !cp.checkAlive(conn) {
		cp.pool.Put(conn)
		return cp.Get()
	}
	return conn, nil
}

// GetWithContext 带上下文的获取连接。
func (cp *ConnPool) GetWithContext(ctx context.Context) (Conn, error) {
	obj, err := cp.pool.GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	conn := obj.(Conn)
	if !cp.checkAlive(conn) {
		cp.pool.Put(conn)
		return cp.GetWithContext(ctx)
	}
	return conn, nil
}

// GetWithTimeout 带超时的获取连接。
func (cp *ConnPool) GetWithTimeout(timeout time.Duration) (Conn, error) {
	obj, err := cp.pool.GetWithTimeout(timeout)
	if err != nil {
		return nil, err
	}
	conn := obj.(Conn)
	if !cp.checkAlive(conn) {
		cp.pool.Put(conn)
		return cp.GetWithTimeout(timeout)
	}
	return conn, nil
}

// Put 归还连接。
func (cp *ConnPool) Put(conn Conn) error {
	if conn == nil {
		return ErrInvalidObject
	}
	return cp.pool.Put(conn)
}

// Close 关闭连接池。
func (cp *ConnPool) Close() error {
	return cp.pool.Close()
}

// Stats 获取连接池统计信息。
func (cp *ConnPool) Stats() map[string]interface{} {
	s := cp.pool.Stats()
	return map[string]interface{}{
		"active":  s.ActiveCount,
		"idle":    s.IdleCount,
		"total":   s.CreateCount,
		"wait":    s.WaitCount,
		"timeout": s.TimeoutCount,
	}
}

// IsClosed 检查连接池是否已关闭。
func (cp *ConnPool) IsClosed() bool {
	return cp.pool.IsClosed()
}

// Resize 调整连接池大小。
func (cp *ConnPool) Resize(newMaxActive int) error {
	return cp.pool.Resize(newMaxActive)
}
