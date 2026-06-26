// Package unix 提供基于 Unix 域套接字的传输层通信能力。
// 客户端和服务器的设计专注于可靠的消息收发，不包含连接管理逻辑。
// 【注意】本包不提供连接池，每次 Send 都会新建连接。
// 如需连接复用，请使用上层 pool.ConnPool 包装。
package unix

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// UnixClient Unix域套接字客户端（传输层）。
type UnixClient struct {
	socketPath     string
	timeout        time.Duration
	dialTimeout    time.Duration
	maxMessageSize uint32
	closed         uint32
	mu             sync.RWMutex
	stats          ClientStats
	lastError      atomic.Value
}

// ClientStats 客户端统计信息。
type ClientStats struct {
	TotalRequests   uint64 `json:"total_requests"`
	SuccessRequests uint64 `json:"success_requests"`
	FailedRequests  uint64 `json:"failed_requests"`
	TimeoutRequests uint64 `json:"timeout_requests"`
	AvgLatency      uint64 `json:"avg_latency_ns"`
	LastError       string `json:"last_error"`
	LastActive      int64  `json:"last_active"`
}

// NewUnixClient 创建 Unix 客户端。
func NewUnixClient(socketPath string, options ...ClientOption) (*UnixClient, error) {
	if _, err := os.Stat(socketPath); os.IsNotExist(err) {
		return nil, errors.New("socket file not found: " + socketPath)
	}

	client := &UnixClient{
		socketPath:     socketPath,
		timeout:        10 * time.Second,
		dialTimeout:    3 * time.Second,
		maxMessageSize: 10 * 1024 * 1024,
	}
	client.lastError.Store("")

	for _, opt := range options {
		opt(client)
	}

	return client, nil
}

// Send 发送原始数据并接收响应。
func (c *UnixClient) Send(ctx context.Context, data []byte) ([]byte, error) {
	if atomic.LoadUint32(&c.closed) == 1 {
		return nil, errors.New("client is closed")
	}

	select {
	case <-ctx.Done():
		c.incrementError(ctx.Err())
		c.lastError.Store(ctx.Err().Error())
		return nil, ctx.Err()
	default:
	}

	start := time.Now()
	atomic.AddUint64(&c.stats.TotalRequests, 1)

	conn, err := net.DialTimeout("unix", c.socketPath, c.dialTimeout)
	if err != nil {
		c.incrementError(err)
		c.lastError.Store(err.Error())
		return nil, err
	}
	defer conn.Close()

	var cancel context.CancelFunc
	if c.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	length := uint32(len(data))
	lengthBytes := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}
	if err := c.writeWithContext(ctx, conn, lengthBytes); err != nil {
		c.incrementError(err)
		c.lastError.Store(err.Error())
		return nil, err
	}

	if err := c.writeWithContext(ctx, conn, data); err != nil {
		c.incrementError(err)
		c.lastError.Store(err.Error())
		return nil, err
	}

	resp, err := c.readResponse(ctx, conn)
	if err != nil {
		c.incrementError(err)
		c.lastError.Store(err.Error())
		return nil, err
	}

	if len(resp) > 0 && resp[0] == 0xFF {
		errMsg := string(resp[1:])
		err = errors.New(errMsg)
		c.incrementError(err)
		c.lastError.Store(errMsg)
		return nil, err
	}

	atomic.AddUint64(&c.stats.SuccessRequests, 1)
	latency := uint64(time.Since(start).Nanoseconds())
	if latency == 0 {
		latency = 1
	}

	for {
		oldAvg := atomic.LoadUint64(&c.stats.AvgLatency)
		successCount := atomic.LoadUint64(&c.stats.SuccessRequests)
		if successCount == 0 {
			break
		}
		if successCount == 1 {
			if atomic.CompareAndSwapUint64(&c.stats.AvgLatency, oldAvg, latency) {
				break
			}
		} else {
			newAvg := (oldAvg*(successCount-1) + latency) / successCount
			if atomic.CompareAndSwapUint64(&c.stats.AvgLatency, oldAvg, newAvg) {
				break
			}
		}
	}
	atomic.StoreInt64(&c.stats.LastActive, time.Now().UnixNano())

	return resp, nil
}

// incrementError 根据错误类型增加失败或超时计数。
func (c *UnixClient) incrementError(err error) {
	atomic.AddUint64(&c.stats.FailedRequests, 1)

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		atomic.AddUint64(&c.stats.TimeoutRequests, 1)
		return
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		atomic.AddUint64(&c.stats.TimeoutRequests, 1)
	}
}

// writeWithContext 带上下文检查的写入操作。
func (c *UnixClient) writeWithContext(ctx context.Context, conn net.Conn, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	}
	_, err := conn.Write(data)
	return err
}

// readResponse 读取完整响应。
func (c *UnixClient) readResponse(ctx context.Context, conn net.Conn) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	lengthBytes := make([]byte, 4)
	if err := c.readFullWithContext(ctx, conn, lengthBytes); err != nil {
		return nil, err
	}

	length := uint32(lengthBytes[0])<<24 | uint32(lengthBytes[1])<<16 |
		uint32(lengthBytes[2])<<8 | uint32(lengthBytes[3])

	if length > c.maxMessageSize {
		return nil, errors.New("response too large")
	}

	data := make([]byte, length)
	if err := c.readFullWithContext(ctx, conn, data); err != nil {
		return nil, err
	}
	return data, nil
}

// readFullWithContext 带上下文检查的完整读取。
func (c *UnixClient) readFullWithContext(ctx context.Context, conn net.Conn, buf []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	_, err := io.ReadFull(conn, buf)
	return err
}

// Close 关闭客户端。
func (c *UnixClient) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil
	}
	return nil
}

// Stats 获取统计信息。
func (c *UnixClient) Stats() ClientStats {
	lastErr, _ := c.lastError.Load().(string)
	return ClientStats{
		TotalRequests:   atomic.LoadUint64(&c.stats.TotalRequests),
		SuccessRequests: atomic.LoadUint64(&c.stats.SuccessRequests),
		FailedRequests:  atomic.LoadUint64(&c.stats.FailedRequests),
		TimeoutRequests: atomic.LoadUint64(&c.stats.TimeoutRequests),
		AvgLatency:      atomic.LoadUint64(&c.stats.AvgLatency),
		LastError:       lastErr,
		LastActive:      atomic.LoadInt64(&c.stats.LastActive),
	}
}

// IsClosed 检查客户端是否已关闭。
func (c *UnixClient) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// ClientOption 客户端配置选项。
type ClientOption func(*UnixClient)

// WithTimeout 设置客户端默认超时时间。
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *UnixClient) { c.timeout = timeout }
}

// WithDialTimeout 设置建立连接的超时时间。
func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(c *UnixClient) { c.dialTimeout = timeout }
}

// WithClientMaxMessageSize 设置客户端允许接收的最大消息大小。
func WithClientMaxMessageSize(size uint32) ClientOption {
	return func(c *UnixClient) { c.maxMessageSize = size }
}
