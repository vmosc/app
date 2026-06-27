package app

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmosc/app/kernel/codec"
	"github.com/vmosc/app/kernel/log"
	"github.com/vmosc/app/kernel/pool"
)

// 全局客户端缓存 - key 使用 socketPath 而非 service，避免多实例覆盖
var clientCache sync.Map

func getClient(key string) (*Client, bool) {
	val, ok := clientCache.Load(key)
	if !ok {
		return nil, false
	}
	return val.(*Client), true
}

func setClient(key string, client *Client) {
	clientCache.Store(key, client)
}

func deleteClient(key string) {
	clientCache.Delete(key)
}

// methodCache 缓存服务的方法列表，减少 ListMethod 调用
var methodCache sync.Map

type methodCacheEntry struct {
	methods    map[string]bool
	expireTime time.Time
}

// Client 客户端结构体。
type Client struct {
	connPool   *pool.ConnPool
	codec      codec.Codec
	socketPath string
	closed     int32
}

// NewClient 创建客户端。
func NewClient(socketPath string, codecType string, cfg pool.ConnPoolConfig) (*Client, error) {
	log.Info("client: creating client", "socket", socketPath, "codec", codecType)

	var cdc codec.Codec
	switch codecType {
	case "json":
		cdc = codec.JSON
	case "binary":
		cdc = codec.Binary
	default:
		return nil, fmt.Errorf("unsupported codec type: %s", codecType)
	}

	dial := func() (pool.Conn, error) {
		log.Debug("client: dialing new connection", "socket", socketPath)
		conn, err := net.DialTimeout("unix", socketPath, cfg.GetTimeout)
		if err != nil {
			log.Error("client: dial server failed", "err", err)
			return nil, err
		}
		return &connWrapper{Conn: conn, socketPath: socketPath}, nil
	}

	closeConn := func(c pool.Conn) { _ = c.Close() }
	checkAlive := func(c pool.Conn) bool {
		if cw, ok := c.(*connWrapper); ok {
			return cw.IsAlive()
		}
		return false
	}

	connPool := pool.NewConnPool(dial, closeConn, checkAlive, cfg)
	log.Info("client: created successfully", "max_idle", cfg.MaxIdle, "max_active", cfg.MaxActive)

	return &Client{
		connPool:   connPool,
		codec:      cdc,
		socketPath: socketPath,
	}, nil
}

// Send 底层发送方法。
func (c *Client) Send(ctx context.Context, reqMsg *Message) (*Message, error) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return nil, fmt.Errorf("client is closed")
	}

	log.Debug("client: sending request", "msg_id", reqMsg.ID, "method", reqMsg.Method)

	conn, err := c.connPool.GetWithContext(ctx)
	if err != nil {
		log.Error("client: get connection failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}
	defer func() {
		if putErr := c.connPool.Put(conn); putErr != nil {
			log.Error("client: put connection failed", "err", putErr, "msg_id", reqMsg.ID)
		}
	}()

	var data []byte
	if c.codec.Type() == "binary" {
		data, err = reqMsg.MarshalBinary()
	} else {
		data, err = c.codec.Encode(reqMsg)
	}
	if err != nil {
		log.Error("client: encode request failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}

	length := uint32(len(data))
	header := []byte{byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)}
	netConn, ok := conn.(*connWrapper)
	if !ok {
		return nil, fmt.Errorf("invalid connection type")
	}
	if err := c.writeWithContext(ctx, netConn.Conn, header); err != nil {
		log.Error("client: write header failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}
	if err := c.writeWithContext(ctx, netConn.Conn, data); err != nil {
		log.Error("client: write body failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}

	respData, err := c.readResponse(ctx, netConn.Conn)
	if err != nil {
		log.Error("client: read response failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}
	defer PutBuffer(respData)

	if len(respData) > 0 && respData[0] == 0xFF {
		errMsg := string(respData[1:])
		if errMsg == "" {
			errMsg = "server error"
		}
		return nil, errors.New(errMsg)
	}

	var respMsg Message
	if c.codec.Type() == "binary" {
		err = respMsg.UnmarshalBinary(respData)
	} else {
		err = c.codec.Decode(respData, &respMsg)
	}
	if err != nil {
		log.Error("client: decode response failed", "err", err, "msg_id", reqMsg.ID)
		return nil, err
	}
	return &respMsg, nil
}

// Call 公开方法，支持服务发现。
func (c *Client) Call(serviceAndMethod string, param any) (*Message, error) {
	parts := strings.SplitN(serviceAndMethod, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format: need 'service/method', got %s", serviceAndMethod)
	}
	service, method := parts[0], parts[1]

	if service == "registry" {
		if registryClient, ok := getClient("registry"); ok {
			return c.invoke(registryClient, method, param)
		}
		return nil, errors.New("registry client not available")
	}

	registry := GetApp().GetRegistry()
	if registry == nil {
		return nil, errors.New("registry not available")
	}

	// 发现服务
	endpoints, err := registry.Discover(context.Background(), service)
	if err != nil || len(endpoints) == 0 {
		return nil, fmt.Errorf("service %s not found", service)
	}
	endpoint := endpoints[0]

	// 确定缓存 key：使用 socketPath 而非 service，避免多实例覆盖
	cacheKey := endpoint.Address

	// 从缓存获取或创建客户端
	var targetClient *Client
	if cached, ok := getClient(cacheKey); ok {
		targetClient = cached
	} else {
		switch endpoint.Type {
		case "unix":
			targetClient, err = NewClient(endpoint.Address, c.codec.Type(), pool.ConnPoolConfig{
				MaxIdle:     10,
				MaxActive:   100,
				GetTimeout:  5 * time.Second,
				MaxIdleTime: 30 * time.Second,
				MaxLifeTime: 5 * time.Minute,
			})
		case "remote":
			gwEndpoints, err := registry.Discover(context.Background(), "gateway")
			if err != nil || len(gwEndpoints) == 0 {
				return nil, errors.New("gateway not found")
			}
			gwAddr := gwEndpoints[0].Address
			targetClient, err = NewClient(gwAddr, c.codec.Type(), pool.ConnPoolConfig{
				MaxIdle:     10,
				MaxActive:   100,
				GetTimeout:  5 * time.Second,
				MaxIdleTime: 30 * time.Second,
				MaxLifeTime: 5 * time.Minute,
			})
			if err != nil {
				return nil, err
			}
			remoteParam := map[string]any{
				"target_service": service,
				"target_method":  method,
				"params":         param,
			}
			return c.invoke(targetClient, "Forward", remoteParam)
		default:
			return nil, fmt.Errorf("unsupported endpoint type: %s", endpoint.Type)
		}
		if err != nil {
			return nil, err
		}
		setClient(cacheKey, targetClient)
	}

	// 检查方法是否存在（使用缓存）
	if err := c.checkMethodExistsWithCache(targetClient, service, method); err != nil {
		return nil, err
	}
	return c.invoke(targetClient, method, param)
}

// checkMethodExistsWithCache 带缓存的方法检查
func (c *Client) checkMethodExistsWithCache(targetClient *Client, service, method string) error {
	// 先从缓存读取
	cacheKey := targetClient.socketPath + ":" + service
	if entry, ok := methodCache.Load(cacheKey); ok {
		e := entry.(methodCacheEntry)
		if time.Now().Before(e.expireTime) {
			if e.methods[method] {
				return nil
			}
			return fmt.Errorf("method %s not found in service %s (cached)", method, service)
		}
		// 缓存已过期，删除
		methodCache.Delete(cacheKey)
	}

	// 缓存未命中或已过期，查询服务
	req := &Message{
		ID:        generateID(),
		Method:    "ListMethod",
		Body:      []byte{},
		Timestamp: time.Now().UnixNano(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := targetClient.Send(ctx, req)
	if err != nil {
		log.Debug("ListMethod not supported, skip method check", "service", service, "err", err)
		// 服务不支持 ListMethod，缓存空结果（短时间）
		methodCache.Store(cacheKey, methodCacheEntry{
			methods:    make(map[string]bool),
			expireTime: time.Now().Add(30 * time.Second),
		})
		return nil
	}

	var methods []string
	if err := c.codec.Decode(resp.Body, &methods); err != nil {
		parts := strings.Split(string(resp.Body), ",")
		for _, p := range parts {
			if m := strings.TrimSpace(p); m != "" {
				methods = append(methods, m)
			}
		}
	}

	// 构建方法 map
	methodMap := make(map[string]bool)
	for _, m := range methods {
		methodMap[m] = true
	}
	// 缓存结果，5分钟过期
	methodCache.Store(cacheKey, methodCacheEntry{
		methods:    methodMap,
		expireTime: time.Now().Add(5 * time.Minute),
	})

	if methodMap[method] {
		return nil
	}
	return fmt.Errorf("method %s not found in service %s", method, service)
}

func (c *Client) invoke(client *Client, method string, param any) (*Message, error) {
	var (
		body []byte
		err  error
	)
	if param == nil {
		body = []byte{}
	} else if c.codec.Type() == "binary" {
		if bm, ok := param.(encoding.BinaryMarshaler); ok {
			body, err = bm.MarshalBinary()
		} else {
			body, err = json.Marshal(param)
		}
		if err != nil {
			return nil, err
		}
	} else {
		body, err = c.codec.Encode(param)
		if err != nil {
			return nil, err
		}
	}
	req := &Message{
		ID:        generateID(),
		Method:    method,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Send(ctx, req)
}

func (c *Client) writeWithContext(ctx context.Context, conn net.Conn, data []byte) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	}
	_, err := conn.Write(data)
	return err
}

func (c *Client) readResponse(ctx context.Context, conn net.Conn) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
	if length > 10*1024*1024 {
		return nil, fmt.Errorf("response too large: %d bytes", length)
	}
	data := GetBuffer(int(length))
	if cap(data) < int(length) {
		data = make([]byte, length)
	} else {
		data = data[:length]
	}
	if _, err := io.ReadFull(conn, data); err != nil {
		PutBuffer(data)
		return nil, err
	}
	return data, nil
}

func (c *Client) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}
	log.Info("client: closing client")
	deleteClient(c.socketPath)
	return nil
}

type connWrapper struct {
	net.Conn
	socketPath string
}

func (w *connWrapper) IsAlive() bool {
	deadline := time.Now().Add(1 * time.Millisecond)
	if err := w.SetWriteDeadline(deadline); err != nil {
		return false
	}
	_, err := w.Write([]byte{})
	_ = w.SetWriteDeadline(time.Time{})
	if err == nil {
		return true
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	log.Debug("connection: alive check failed", "err", err)
	return false
}
