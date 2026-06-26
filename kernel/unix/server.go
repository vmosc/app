// Package unix 提供基于 Unix 域套接字的传输层通信能力。
package unix

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const maxErrorMsgLen = 1024

// UnixServer Unix域套接字服务器。
type UnixServer struct {
	socketPath           string
	listener             net.Listener
	handler              func(context.Context, []byte) ([]byte, error)
	errorHandler         func(error)
	returnDetailedErrors bool
	maxMessageSize       uint32
	readTimeout          time.Duration
	handlerTimeout       time.Duration
	running              uint32
	wg                   sync.WaitGroup
	stopChan             chan struct{}
	activeConns          sync.Map
	stats                ServerStats
	lastError            atomic.Value
}

// ServerStats 服务器统计信息。
type ServerStats struct {
	TotalRequests     uint64 `json:"total_requests"`
	ActiveRequests    uint64 `json:"active_requests"`
	SuccessRequests   uint64 `json:"success_requests"`
	FailedRequests    uint64 `json:"failed_requests"`
	AvgProcessTime    uint64 `json:"avg_process_time_ns"`
	ActiveConnections int32  `json:"active_connections"`
	LastError         string `json:"last_error"`
	StartTime         int64  `json:"start_time"`
}

// NewUnixServer 创建Unix服务器。
func NewUnixServer(socketPath string, options ...ServerOption) *UnixServer {
	server := &UnixServer{
		socketPath:           socketPath,
		stopChan:             make(chan struct{}),
		maxMessageSize:       10 * 1024 * 1024,
		readTimeout:          30 * time.Second,
		handlerTimeout:       10 * time.Second,
		returnDetailedErrors: false,
	}
	server.lastError.Store("")
	for _, opt := range options {
		opt(server)
	}
	return server
}

// Serve 启动服务器。
func (s *UnixServer) Serve(handler func(context.Context, []byte) ([]byte, error)) error {
	if !atomic.CompareAndSwapUint32(&s.running, 0, 1) {
		return errors.New("server already running")
	}
	s.handler = handler
	atomic.StoreInt64(&s.stats.StartTime, time.Now().UnixNano())

	dir := filepath.Dir(s.socketPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	_ = os.Remove(s.socketPath)

	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return err
	}
	if err := os.Chmod(s.socketPath, 0666); err != nil {
		_ = listener.Close()
		return err
	}
	s.listener = listener

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// acceptLoop 接受连接循环。
func (s *UnixServer) acceptLoop() {
	defer s.wg.Done()
	var backoff time.Duration
	const maxBackoff = 1 * time.Second

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if atomic.LoadUint32(&s.running) == 0 {
				return
			}
			if s.errorHandler != nil {
				s.errorHandler(err)
			}
			if backoff == 0 {
				backoff = 10 * time.Millisecond
			} else {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			select {
			case <-time.After(backoff):
			case <-s.stopChan:
				return
			}
			continue
		}
		backoff = 0

		atomic.AddInt32(&s.stats.ActiveConnections, 1)
		s.activeConns.Store(conn, struct{}{})
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个连接。
func (s *UnixServer) handleConnection(conn net.Conn) {
	defer func() {
		atomic.AddInt32(&s.stats.ActiveConnections, -1)
		s.activeConns.Delete(conn)
		_ = conn.Close()
		s.wg.Done()
	}()

	for atomic.LoadUint32(&s.running) == 1 {
		_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))

		req, err := s.readRequest(conn)
		if err != nil {
			if s.errorHandler != nil && !errors.Is(err, io.EOF) {
				s.errorHandler(err)
			}
			return
		}

		atomic.AddUint64(&s.stats.TotalRequests, 1)
		atomic.AddUint64(&s.stats.ActiveRequests, 1)

		start := time.Now()
		if !s.processRequest(conn, req, start) {
			return
		}
	}
}

// readRequest 读取完整请求。
func (s *UnixServer) readRequest(conn net.Conn) ([]byte, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, lengthBytes); err != nil {
		return nil, err
	}
	length := uint32(lengthBytes[0])<<24 | uint32(lengthBytes[1])<<16 |
		uint32(lengthBytes[2])<<8 | uint32(lengthBytes[3])

	if length > s.maxMessageSize {
		return nil, errors.New("request too large")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	return data, nil
}

// processRequest 处理单个请求。
func (s *UnixServer) processRequest(conn net.Conn, req []byte, start time.Time) bool {
	defer atomic.AddUint64(&s.stats.ActiveRequests, ^uint64(0))

	var resp []byte
	var err error

	if s.handler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.handlerTimeout)
		defer cancel()
		resp, err = s.handler(ctx, req)
	} else {
		resp = []byte("handler not available")
		err = errors.New("handler not available")
	}

	if err != nil && s.errorHandler != nil {
		s.errorHandler(err)
	}

	var responseData []byte
	if err != nil {
		atomic.AddUint64(&s.stats.FailedRequests, 1)
		s.lastError.Store(err.Error())
		if s.returnDetailedErrors {
			errMsg := err.Error()
			if len(errMsg) > maxErrorMsgLen {
				errMsg = errMsg[:maxErrorMsgLen] + "...(truncated)"
			}
			responseData = make([]byte, len(errMsg)+1)
			responseData[0] = 0xFF
			copy(responseData[1:], []byte(errMsg))
		} else {
			genericMsg := "internal server error"
			responseData = make([]byte, len(genericMsg)+1)
			responseData[0] = 0xFF
			copy(responseData[1:], []byte(genericMsg))
		}
	} else {
		atomic.AddUint64(&s.stats.SuccessRequests, 1)
		responseData = resp
	}

	if err := s.sendResponse(conn, responseData); err != nil {
		if s.errorHandler != nil {
			s.errorHandler(err)
		}
		atomic.AddUint64(&s.stats.FailedRequests, 1)
		return false
	}

	processTime := uint64(time.Since(start).Nanoseconds())
	for {
		oldAvg := atomic.LoadUint64(&s.stats.AvgProcessTime)
		totalRequests := atomic.LoadUint64(&s.stats.TotalRequests)
		if totalRequests == 0 {
			break
		}
		if totalRequests == 1 {
			if atomic.CompareAndSwapUint64(&s.stats.AvgProcessTime, oldAvg, processTime) {
				break
			}
		} else {
			newAvg := (oldAvg*(totalRequests-1) + processTime) / totalRequests
			if atomic.CompareAndSwapUint64(&s.stats.AvgProcessTime, oldAvg, newAvg) {
				break
			}
		}
	}

	return true
}

// sendResponse 发送响应。
func (s *UnixServer) sendResponse(conn net.Conn, data []byte) error {
	length := uint32(len(data))
	packet := make([]byte, 4+length)
	packet[0] = byte(length >> 24)
	packet[1] = byte(length >> 16)
	packet[2] = byte(length >> 8)
	packet[3] = byte(length)
	copy(packet[4:], data)

	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.Write(packet)
	return err
}

// Stop 停止服务器。
func (s *UnixServer) Stop() error {
	if !atomic.CompareAndSwapUint32(&s.running, 1, 0) {
		return nil
	}
	close(s.stopChan)
	if s.listener != nil {
		_ = s.listener.Close()
	}

	s.activeConns.Range(func(key, value interface{}) bool {
		if conn, ok := key.(net.Conn); ok {
			_ = conn.Close()
		}
		return true
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}

	_ = os.Remove(s.socketPath)
	return nil
}

// Stats 获取服务器统计信息。
func (s *UnixServer) Stats() ServerStats {
	lastErr, _ := s.lastError.Load().(string)
	return ServerStats{
		TotalRequests:     atomic.LoadUint64(&s.stats.TotalRequests),
		ActiveRequests:    atomic.LoadUint64(&s.stats.ActiveRequests),
		SuccessRequests:   atomic.LoadUint64(&s.stats.SuccessRequests),
		FailedRequests:    atomic.LoadUint64(&s.stats.FailedRequests),
		AvgProcessTime:    atomic.LoadUint64(&s.stats.AvgProcessTime),
		ActiveConnections: atomic.LoadInt32(&s.stats.ActiveConnections),
		LastError:         lastErr,
		StartTime:         atomic.LoadInt64(&s.stats.StartTime),
	}
}

// IsRunning 检查服务器是否在运行。
func (s *UnixServer) IsRunning() bool {
	return atomic.LoadUint32(&s.running) == 1
}

// ServerOption 服务器配置选项。
type ServerOption func(*UnixServer)

// WithErrorHandler 设置错误回调函数。
func WithErrorHandler(handler func(error)) ServerOption {
	return func(s *UnixServer) { s.errorHandler = handler }
}

// WithReturnDetailedErrors 设置是否向客户端返回详细的错误消息。
func WithReturnDetailedErrors(returnDetailed bool) ServerOption {
	return func(s *UnixServer) { s.returnDetailedErrors = returnDetailed }
}

// WithServerMaxMessageSize 设置服务器允许接收的最大消息大小。
func WithServerMaxMessageSize(size uint32) ServerOption {
	return func(s *UnixServer) { s.maxMessageSize = size }
}

// WithReadTimeout 设置每个连接读取请求的超时时间。
func WithReadTimeout(timeout time.Duration) ServerOption {
	return func(s *UnixServer) { s.readTimeout = timeout }
}

// WithHandlerTimeout 设置调用 handler 的超时时间。
func WithHandlerTimeout(timeout time.Duration) ServerOption {
	return func(s *UnixServer) { s.handlerTimeout = timeout }
}
