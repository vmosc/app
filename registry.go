package app

import (
	"app/kernel/log"
	"app/kernel/pool"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	registryService  = "registry"
	methodRegister   = "Register"
	methodHeartbeat  = "Heartbeat"
	methodDeregister = "Deregister"
	methodDiscover   = "Discover"
)

// ServiceEndpoint 服务端点。
type ServiceEndpoint struct {
	Type     string            // "unix" 或 "remote"
	Address  string            // 地址
	Metadata map[string]string // 额外信息（如 serviceType）
}

// Registry 注册中心接口。
type Registry interface {
	Register(ctx context.Context, info *ServiceInfo) error
	Deregister(ctx context.Context, serviceID string) error
	Discover(ctx context.Context, serviceName string) ([]*ServiceEndpoint, error)
}

// ServiceInfo 服务注册信息。
type ServiceInfo struct {
	ID       string
	Name     string
	Address  string
	Metadata map[string]string
}

type ServiceState int32

const (
	StateStarting ServiceState = iota
	StateRunning
	StateStopping
)

func (s ServiceState) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	default:
		return "unknown"
	}
}

// RegistryClient 注册中心客户端。
type RegistryClient struct {
	client            *Client
	serviceName       string
	serviceType       string
	state             int32
	stopCh            chan struct{}
	wg                sync.WaitGroup
	mu                sync.Mutex
	running           bool
	heartbeatFailures int32
	lastHeartbeatTime int64
	registering       int32
}

var _ Registry = (*RegistryClient)(nil)

// NewRegistryClient 创建注册中心客户端。
func NewRegistryClient(socketPath string, codecType string, serviceName string, serviceType string) (*RegistryClient, error) {
	log.Info("registry: creating registry client", "socket", socketPath, "service", serviceName)

	cfg := pool.DefaultConnPoolConfig()
	cfg.MaxActive = 2
	cfg.MaxIdle = 1
	cfg.GetTimeout = 5 * time.Second

	client, err := NewClient(socketPath, codecType, cfg)
	if err != nil {
		log.Error("registry: create client failed", "err", err)
		return nil, err
	}
	setClient("registry", client)

	return &RegistryClient{
		client:      client,
		serviceName: serviceName,
		serviceType: serviceType,
		state:       int32(StateStarting),
		stopCh:      make(chan struct{}),
	}, nil
}

// Register 注册服务。
func (rc *RegistryClient) Register(ctx context.Context, info *ServiceInfo) error {
	if !atomic.CompareAndSwapInt32(&rc.registering, 0, 1) {
		return errors.New("register already in progress")
	}
	defer atomic.StoreInt32(&rc.registering, 0)

	id := generateID()
	if rc.serviceType != "" {
		id = rc.serviceType + "-" + id
	}
	params := map[string]any{
		"name":   rc.serviceName,
		"type":   rc.serviceType,
		"state":  ServiceState(atomic.LoadInt32(&rc.state)).String(),
		"id":     id,
		"socket": rc.client.socketPath,
	}
	if info != nil {
		if info.ID != "" {
			params["id"] = info.ID
		}
		if info.Name != "" {
			params["name"] = info.Name
		}
		if info.Address != "" {
			params["socket"] = info.Address
		}
		for k, v := range info.Metadata {
			params[k] = v
		}
	}
	app := GetApp()
	if app != nil && app.config != nil {
		params["root"] = app.config.rootDir
	} else {
		params["root"] = ""
	}
	_, err := rc.client.Call(registryService+"/"+methodRegister, params)
	if err != nil {
		log.Error("registry: register failed", "err", err, "service", rc.serviceName)
		return err
	}
	atomic.StoreInt32(&rc.state, int32(StateRunning))
	log.Info("registry: registered successfully", "service", rc.serviceName)
	return nil
}

// Deregister 注销服务。
func (rc *RegistryClient) Deregister(ctx context.Context, serviceID string) error {
	atomic.StoreInt32(&rc.state, int32(StateStopping))
	params := map[string]any{
		"service": rc.serviceName,
		"id":      serviceID,
		"state":   StateStopping.String(),
	}
	_, err := rc.client.Call(registryService+"/"+methodDeregister, params)
	if err != nil {
		log.Error("registry: deregister failed", "err", err, "service", rc.serviceName)
		return err
	}
	log.Info("registry: deregistered successfully", "service", rc.serviceName)
	return nil
}

// Discover 服务发现 - 改用 JSON 解析
func (rc *RegistryClient) Discover(ctx context.Context, serviceName string) ([]*ServiceEndpoint, error) {
	params := map[string]any{"service": serviceName}
	resp, err := rc.client.Call(registryService+"/"+methodDiscover, params)
	if err != nil {
		log.Error("registry: discover failed", "err", err, "service", serviceName)
		return nil, err
	}

	var endpoints []*ServiceEndpoint
	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("no endpoints found for service %s", serviceName)
	}

	if err := json.Unmarshal(resp.Body, &endpoints); err != nil {
		// 尝试兼容旧格式：如果是数组字符串，尝试解析
		log.Warn("registry: JSON unmarshal failed, trying alternative format", "err", err, "body", string(resp.Body))
		var strings []string
		if err2 := json.Unmarshal(resp.Body, &strings); err2 == nil {
			for _, s := range strings {
				endpoints = append(endpoints, &ServiceEndpoint{
					Type:    "unix",
					Address: s,
				})
			}
		} else {
			return nil, fmt.Errorf("discover response parse failed: %w, body: %s", err, string(resp.Body))
		}
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints found for service %s", serviceName)
	}

	log.Debug("registry: discovered", "service", serviceName, "count", len(endpoints))
	return endpoints, nil
}

// Heartbeat 心跳。
func (rc *RegistryClient) Heartbeat(ctx context.Context) error {
	state := ServiceState(atomic.LoadInt32(&rc.state))
	params := map[string]any{
		"service": rc.serviceName,
		"state":   state.String(),
	}
	_, err := rc.client.Call(registryService+"/"+methodHeartbeat, params)
	if err != nil {
		failures := atomic.AddInt32(&rc.heartbeatFailures, 1)
		log.Error("registry: heartbeat failed", "err", err, "failures", failures)
		if failures >= 3 {
			log.Warn("registry: heartbeat连续失败，尝试重新注册")
			regCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if regErr := rc.Register(regCtx, nil); regErr != nil {
				log.Error("registry: re-register failed", "err", regErr)
			} else {
				atomic.StoreInt32(&rc.heartbeatFailures, 0)
				log.Info("registry: re-register succeeded")
			}
		}
		return err
	}
	if atomic.LoadInt32(&rc.heartbeatFailures) > 0 {
		atomic.StoreInt32(&rc.heartbeatFailures, 0)
		log.Info("registry: heartbeat recovered")
	}
	atomic.StoreInt64(&rc.lastHeartbeatTime, time.Now().UnixNano())
	return nil
}

// Unregister 兼容旧名称。
func (rc *RegistryClient) Unregister(ctx context.Context) error {
	return rc.Deregister(ctx, "")
}

// StartHeartbeat 启动心跳。
func (rc *RegistryClient) StartHeartbeat(interval time.Duration) {
	rc.mu.Lock()
	if rc.running {
		rc.mu.Unlock()
		return
	}
	rc.running = true
	rc.mu.Unlock()
	atomic.StoreInt32(&rc.state, int32(StateRunning))
	log.Info("registry: starting heartbeat", "service", rc.serviceName, "interval", interval)
	rc.wg.Add(1)
	go func() {
		defer rc.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				_ = rc.Heartbeat(ctx)
				cancel()
			case <-rc.stopCh:
				log.Info("registry: heartbeat stopped")
				return
			}
		}
	}()
}

// Stop 停止注册中心客户端。
func (rc *RegistryClient) Stop() {
	rc.mu.Lock()
	if !rc.running {
		rc.mu.Unlock()
		return
	}
	rc.running = false
	close(rc.stopCh)
	rc.mu.Unlock()
	rc.wg.Wait()
	log.Info("registry: client stopped", "service", rc.serviceName)
}

// GetState 返回当前状态。
func (rc *RegistryClient) GetState() ServiceState {
	return ServiceState(atomic.LoadInt32(&rc.state))
}

// GetRawClient 返回底层客户端。
func (rc *RegistryClient) GetRawClient() *Client {
	return rc.client
}

// WatchRegistrySocket 监听 REGISTRY_SOCKET 环境变量变化。
func WatchRegistrySocket(
	getConfig func() (registrySocket, codecType, serviceName, serviceType string, heartbeatInterval int64),
	onUpdate func(newClient *RegistryClient),
	stopCh <-chan struct{},
	interval time.Duration,
) {
	checkAndUpdate(getConfig, onUpdate)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			checkAndUpdate(getConfig, onUpdate)
		case <-stopCh:
			return
		}
	}
}

func checkAndUpdate(
	getConfig func() (registrySocket, codecType, serviceName, serviceType string, heartbeatInterval int64),
	onUpdate func(newClient *RegistryClient),
) {
	newSocket := os.Getenv("REGISTRY_SOCKET")
	if newSocket == "" {
		return
	}
	oldSocket, codecType, serviceName, serviceType, heartbeatInterval := getConfig()
	if oldSocket == "" {
		rc, err := NewRegistryClient(newSocket, codecType, serviceName, serviceType)
		if err != nil {
			log.Error("registry: create client failed", "err", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := rc.Register(ctx, nil); err != nil {
			log.Error("registry: register failed", "err", err)
			return
		}
		rc.StartHeartbeat(time.Duration(heartbeatInterval))
		onUpdate(rc)
		log.Info("registry: enabled", "socket", newSocket)
		return
	}
	if newSocket == oldSocket {
		return
	}
	log.Warn("registry: socket changed", "old", oldSocket, "new", newSocket)
	rc, err := NewRegistryClient(newSocket, codecType, serviceName, serviceType)
	if err != nil {
		log.Error("registry: create client failed", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rc.Register(ctx, nil); err != nil {
		log.Error("registry: register failed", "err", err)
		return
	}
	rc.StartHeartbeat(time.Duration(heartbeatInterval))
	onUpdate(rc)
	log.Info("registry: updated", "socket", newSocket)
}
