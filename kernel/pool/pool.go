// Package pool 提供高性能的对象池、工作池和连接池实现。
package pool

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

const (
	TypeObject = "object"
	TypeWorker = "worker"
)

var (
	ErrPoolClosed    = errors.New("pool is closed")
	ErrPoolTimeout   = errors.New("pool timeout")
	ErrPoolFull      = errors.New("pool is full")
	ErrTaskRejected  = errors.New("task rejected")
	ErrInvalidObject = errors.New("invalid object")
	ErrInvalidCount  = errors.New("invalid batch count")
)

type PoolErrorCode int

const (
	ErrCodeNone PoolErrorCode = iota
	ErrCodeClosed
	ErrCodeTimeout
	ErrCodeFull
	ErrCodeInvalidObject
	ErrCodeRejected
)

// PoolError 增强的错误类型。
type PoolError struct {
	Op     string
	Code   PoolErrorCode
	Err    error
	PoolID string
	Time   time.Time
	Count  int
}

func (e *PoolError) Error() string {
	msg := fmt.Sprintf("pool operation %s failed", e.Op)
	if e.PoolID != "" {
		msg += fmt.Sprintf(" [pool=%s]", e.PoolID)
	}
	if e.Err != nil {
		msg += fmt.Sprintf(": %v", e.Err)
	}
	return msg
}

func (e *PoolError) Unwrap() error {
	return e.Err
}

// Pool 高性能池基础接口。
type Pool interface {
	Type() string
	Stats() PoolStats
	Close() error
	IsClosed() bool
	Name() string
	SetName(name string)
}

// PoolStats 池统计信息。
type PoolStats struct {
	_ [64]byte

	Type         string `json:"type"`
	ActiveCount  uint64 `json:"active_count"`
	IdleCount    uint64 `json:"idle_count"`
	MaxCapacity  uint64 `json:"max_capacity"`
	MinCapacity  uint64 `json:"min_capacity"`
	WaitCount    uint64 `json:"wait_count"`
	CreateCount  uint64 `json:"create_count"`
	DestroyCount uint64 `json:"destroy_count"`
	TimeoutCount uint64 `json:"timeout_count"`
	ErrorCount   uint64 `json:"error_count"`
	GetLatency   uint64 `json:"get_latency_ns"`
	PutLatency   uint64 `json:"put_latency_ns"`
	RecycleCount uint64 `json:"recycle_count"`
	StealCount   uint64 `json:"steal_count"`

	_ [64]byte

	LastActiveTime int64 `json:"last_active_time"`
}

// Config 高性能配置。
type Config struct {
	MaxSize             int           `json:"max_size"`
	MinSize             int           `json:"min_size"`
	GetTimeout          time.Duration `json:"get_timeout"`
	MaxIdleTime         time.Duration `json:"max_idle_time"`
	MaxLifeTime         time.Duration `json:"max_life_time"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	EnableMetrics       bool          `json:"enable_metrics"`
	PreAllocate         bool          `json:"pre_allocate"`
	PerGoroutineLimit   int           `json:"per_goroutine_limit"`
	Debug               bool          `json:"debug"`
	BatchGetMax         int           `json:"batch_get_max"`
	EnableAutoScale     bool          `json:"enable_auto_scale"`
	TargetUtilization   float64       `json:"target_utilization"`
	ScaleUpFactor       float64       `json:"scale_up_factor"`
	ScaleDownFactor     float64       `json:"scale_down_factor"`
	ScaleInterval       time.Duration `json:"scale_interval"`
	WorkerIdleTimeout   time.Duration `json:"worker_idle_timeout"`
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		MaxSize:             1024,
		MinSize:             32,
		GetTimeout:          100 * time.Millisecond,
		MaxIdleTime:         30 * time.Second,
		MaxLifeTime:         5 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		EnableMetrics:       true,
		PreAllocate:         true,
		PerGoroutineLimit:   0,
		Debug:               false,
		BatchGetMax:         64,
		EnableAutoScale:     false,
		TargetUtilization:   0.7,
		ScaleUpFactor:       1.5,
		ScaleDownFactor:     0.7,
		ScaleInterval:       30 * time.Second,
		WorkerIdleTimeout:   10 * time.Second,
	}
}

// DebugLogger 调试日志接口。
type DebugLogger interface {
	Printf(format string, v ...interface{})
}

// defaultDebugLogger 默认调试日志实现。
type defaultDebugLogger struct{}

func (l *defaultDebugLogger) Printf(format string, v ...interface{}) {}

// getGoroutineID 获取当前Goroutine的ID。
func getGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var id uint64
	for i := 0; i < n; i++ {
		if buf[i] == ' ' {
			for j := i + 1; j < n; j++ {
				if buf[j] >= '0' && buf[j] <= '9' {
					id = id*10 + uint64(buf[j]-'0')
				} else {
					break
				}
			}
			break
		}
	}
	return id
}
