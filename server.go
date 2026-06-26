package app

import (
	"app/kernel/codec"
	"app/kernel/log"
	"app/kernel/pool"
	"app/kernel/unix"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Server 服务端结构体。
type Server struct {
	unixServer *unix.UnixServer
	workerPool *pool.WorkerPool
	codec      codec.Codec
	handlers   map[string]HandlerFunc
	mu         sync.RWMutex
	closed     int32
	metrics    *ServerMetrics
	config     *Config
	stopCh     chan struct{}
}

// HandlerFunc 业务处理函数签名。
type HandlerFunc func(ctx context.Context, req *Message) (*Message, error)

// ServerMetrics 服务端指标。
type ServerMetrics struct {
	TotalRequests    uint64
	SuccessRequests  uint64
	FailedRequests   uint64
	TimeoutRequests  uint64
	RejectedRequests uint64
	QueueLength      uint64
	AvgProcessTime   uint64
	mu               sync.RWMutex
}

// NewServer 创建并启动服务端。
func NewServer(socketPath string, codecType string, workerCount, queueSize int, idleTimeout time.Duration, cfg *Config) (*Server, error) {
	log.Info("server: creating server", "socket", socketPath, "codec", codecType, "workers", workerCount, "queue_size", queueSize)

	if workerCount <= 0 {
		workerCount = runtime.GOMAXPROCS(0)
		log.Info("server: workerCount set to GOMAXPROCS", "workers", workerCount)
	}

	var cdc codec.Codec
	switch codecType {
	case "json":
		cdc = codec.JSON
	case "binary":
		cdc = codec.Binary
	default:
		return nil, fmt.Errorf("unsupported codec type: %s", codecType)
	}

	poolCfg := pool.DefaultConfig()
	poolCfg.MinSize = workerCount
	poolCfg.MaxSize = workerCount * 2
	poolCfg.WorkerIdleTimeout = idleTimeout

	wp := pool.NewWorkerPoolWithConfig("app-worker", workerCount, queueSize, poolCfg)
	us := unix.NewUnixServer(socketPath,
		unix.WithErrorHandler(func(err error) {
			log.Error("server: unix server error", "err", err)
		}),
		unix.WithReturnDetailedErrors(true),
		unix.WithHandlerTimeout(30*time.Second),
	)

	srv := &Server{
		unixServer: us,
		workerPool: wp,
		codec:      cdc,
		handlers:   make(map[string]HandlerFunc),
		metrics:    &ServerMetrics{},
		config:     cfg,
		stopCh:     make(chan struct{}),
	}

	if err := us.Serve(srv.handleConnection); err != nil {
		log.Error("server: failed to start unix server", "err", err)
		return nil, err
	}

	go srv.collectMetrics()
	log.Info("server: created successfully")
	return srv, nil
}

func (s *Server) registerHandler(method string, handler HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = handler
	log.Debug("server: registered handler", "method", method)
}

// handleConnection 处理连接 - 将 ctx 传递给 worker task
func (s *Server) handleConnection(ctx context.Context, reqData []byte) ([]byte, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, errors.New("server is closed")
	}

	if s.config.rateLimitConfig.enabled {
		qLen := s.workerPool.Stats().WaitCount
		atomic.StoreUint64(&s.metrics.QueueLength, qLen)
		maxQueue := uint64(s.config.rateLimitConfig.maxQueueLen)
		if maxQueue > 0 && qLen >= maxQueue {
			atomic.AddUint64(&s.metrics.RejectedRequests, 1)
			log.Warn("server: request rejected due to queue full",
				"queue_len", qLen, "max_queue", maxQueue)
			return nil, errors.New("server busy, request rejected")
		}
	}

	atomic.AddUint64(&s.metrics.TotalRequests, 1)
	start := time.Now()

	var reqMsg Message
	var err error
	if s.codec.Type() == "binary" {
		err = reqMsg.UnmarshalBinary(reqData)
	} else {
		err = s.codec.Decode(reqData, &reqMsg)
	}
	if err != nil {
		log.Error("server: decode request failed", "err", err, "size", len(reqData))
		atomic.AddUint64(&s.metrics.FailedRequests, 1)
		return nil, err
	}

	s.mu.RLock()
	handler, ok := s.handlers[reqMsg.Method]
	s.mu.RUnlock()
	if !ok {
		atomic.AddUint64(&s.metrics.FailedRequests, 1)
		return nil, fmt.Errorf("method not found: %s", reqMsg.Method)
	}

	resultCh := make(chan *Message, 1)
	errCh := make(chan error, 1)

	// 将 ctx 传递给 task，使客户端超时/取消能传递到 handler
	task := pool.NewFuncTask(reqMsg.ID, func(taskCtx context.Context) error {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("handler panic: %v", r)
			}
		}()
		// 使用传入的 ctx（与客户端请求关联）
		resp, err := handler(ctx, &reqMsg)
		if err != nil {
			log.Error("server: handle request failed", "err", err, "msg_id", reqMsg.ID)
			errCh <- err
			return err
		}
		resultCh <- resp
		return nil
	})

	if err := s.workerPool.TrySubmit(task); err != nil {
		atomic.AddUint64(&s.metrics.RejectedRequests, 1)
		atomic.AddUint64(&s.metrics.FailedRequests, 1)
		if err == pool.ErrTaskRejected {
			return nil, errors.New("server busy, request rejected")
		}
		return nil, err
	}

	select {
	case resp := <-resultCh:
		if resp == nil {
			atomic.AddUint64(&s.metrics.FailedRequests, 1)
			return nil, errors.New("handler returned nil response")
		}
		var respData []byte
		if s.codec.Type() == "binary" {
			respData, err = resp.MarshalBinary()
		} else {
			respData, err = s.codec.Encode(resp)
		}
		if err != nil {
			log.Error("server: encode response failed", "err", err, "msg_id", reqMsg.ID)
			atomic.AddUint64(&s.metrics.FailedRequests, 1)
			return nil, err
		}
		atomic.AddUint64(&s.metrics.SuccessRequests, 1)
		totalTime := time.Since(start)
		s.metrics.mu.Lock()
		oldAvg := atomic.LoadUint64(&s.metrics.AvgProcessTime)
		successCount := atomic.LoadUint64(&s.metrics.SuccessRequests)
		if successCount > 0 {
			newAvg := (oldAvg*(successCount-1) + uint64(totalTime.Nanoseconds())) / successCount
			atomic.StoreUint64(&s.metrics.AvgProcessTime, newAvg)
		}
		s.metrics.mu.Unlock()
		return respData, nil

	case err := <-errCh:
		atomic.AddUint64(&s.metrics.FailedRequests, 1)
		return nil, err

	case <-ctx.Done():
		log.Warn("server: request timeout or cancelled", "msg_id", reqMsg.ID, "method", reqMsg.Method)
		atomic.AddUint64(&s.metrics.TimeoutRequests, 1)
		atomic.AddUint64(&s.metrics.FailedRequests, 1)
		return nil, ctx.Err()
	}
}

func (s *Server) collectMetrics() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			queueLen := s.workerPool.Stats().WaitCount
			atomic.StoreUint64(&s.metrics.QueueLength, queueLen)
			log.Info("server: metrics",
				"total", atomic.LoadUint64(&s.metrics.TotalRequests),
				"success", atomic.LoadUint64(&s.metrics.SuccessRequests),
				"failed", atomic.LoadUint64(&s.metrics.FailedRequests),
				"timeout", atomic.LoadUint64(&s.metrics.TimeoutRequests),
				"rejected", atomic.LoadUint64(&s.metrics.RejectedRequests),
				"queue_len", queueLen,
				"queue_cap", s.workerPool.Stats().MaxCapacity,
				"avg_process_ms", atomic.LoadUint64(&s.metrics.AvgProcessTime)/1e6)
		case <-s.stopCh:
			return
		}
	}
}

// Stop 停止服务端。
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	log.Info("server: stopping server")
	close(s.stopCh)
	if s.unixServer != nil {
		_ = s.unixServer.Stop()
	}
	if s.workerPool != nil {
		_ = s.workerPool.Close()
	}
	log.Info("server: stopped successfully")
	return nil
}

// GetMetrics 返回指标快照。
func (s *Server) GetMetrics() map[string]any {
	return map[string]any{
		"total_requests":    atomic.LoadUint64(&s.metrics.TotalRequests),
		"success_requests":  atomic.LoadUint64(&s.metrics.SuccessRequests),
		"failed_requests":   atomic.LoadUint64(&s.metrics.FailedRequests),
		"timeout_requests":  atomic.LoadUint64(&s.metrics.TimeoutRequests),
		"rejected_requests": atomic.LoadUint64(&s.metrics.RejectedRequests),
		"queue_length":      atomic.LoadUint64(&s.metrics.QueueLength),
		"queue_capacity":    s.workerPool.Stats().MaxCapacity,
		"avg_process_ms":    atomic.LoadUint64(&s.metrics.AvgProcessTime) / 1e6,
	}
}
