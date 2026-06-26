package app

import (
	"app/kernel/log"
	"app/kernel/pool"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// App 应用主结构。
type App struct {
	config   *Config
	client   *Client
	server   *Server
	registry *RegistryClient
	stopOnce sync.Once
	stopChan chan struct{}
	mu       sync.RWMutex

	businessConfig map[string]any
	bizMu          sync.RWMutex
	serviceType    string
}

var (
	instance *App
	once     sync.Once
)

// getInstance 私有函数，获取全局单例（仅内部使用）。
func getInstance() *App {
	once.Do(func() {
		instance = &App{
			stopChan: make(chan struct{}),
		}
	})
	return instance
}

// GetApp 获取全局单例（不设置 serviceType，由 Init 传入）。
func GetApp() *App {
	return getInstance()
}

// cleanServiceName 清理服务名：去除路径、扩展名和多余的后缀。
func cleanServiceName(name string) string {
	base := filepath.Base(name)
	base = strings.TrimSuffix(base, ".exe")
	if idx := strings.Index(base, "."); idx > 0 {
		base = base[:idx]
	}
	return base
}

// Init 初始化应用，serviceType 在此传入。
func (a *App) Init(serviceType string) error {
	log.Info("app: initializing...", "service_type", serviceType)

	// 深拷贝默认配置
	cfg := defaultConfig
	a.mu.Lock()
	a.config = &cfg
	a.mu.Unlock()

	// 设置 serviceType
	a.serviceType = serviceType
	if a.serviceType != "" {
		a.config.serviceType = a.serviceType
	}

	// 设置服务名（清理后的可执行文件名）
	if a.config.serviceName == "" {
		progName := cleanServiceName(os.Args[0])
		a.config.serviceName = progName
	}

	// 设置 rootDir 为当前工作目录（如果未配置）
	if a.config.rootDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			log.Error("app: failed to get working directory", "err", err)
			return err
		}
		a.config.rootDir = wd
	}

	// 自动推导日志目录
	if a.config.logDir == "" {
		a.config.logDir = filepath.Join(a.config.rootDir, "log")
	}
	if err := os.MkdirAll(a.config.logDir, 0755); err != nil {
		log.Error("app: failed to create log directory", "err", err)
		return err
	}

	// 生成 socket 路径
	id := generateID()
	suffix := id
	if len(id) > 8 {
		suffix = id[len(id)-8:]
	}
	socketBase := a.config.serviceName
	if a.config.serviceType != "" {
		socketBase = a.config.serviceType
	}
	socketFileName := fmt.Sprintf("%s_%s.sock", socketBase, suffix)
	a.config.socketPath = filepath.Join(a.config.rootDir, socketFileName)
	log.Info("app: auto-generated socket path", "socket", a.config.socketPath)

	// 加载业务配置
	bizCfg, err := loadBusinessConfig(a.config.rootDir)
	if err != nil {
		log.Error("app: failed to load business config", "err", err)
		bizCfg = make(map[string]any)
	}
	a.bizMu.Lock()
	a.businessConfig = bizCfg
	a.bizMu.Unlock()

	// 对象池初始化
	if a.config.objectPoolConfig.maxSize > 0 {
		create := func() (any, error) { return make([]byte, 0, defaultPoolBufferCap), nil }
		validate := func(obj any) bool { return true }
		destroy := func(obj any) {}
		cfg := a.config.objectPoolConfig
		initObjectPool(create, validate, destroy,
			cfg.maxSize, cfg.minSize,
			time.Duration(cfg.getTimeout),
			time.Duration(cfg.maxIdleTime),
			time.Duration(cfg.maxLifeTime),
			time.Duration(cfg.healthCheckInterval),
			cfg.enableAutoScale)
		log.Info("app: object pool initialized")
	}

	// 创建客户端
	client, err := NewClient(a.config.socketPath, a.config.codecType, pool.ConnPoolConfig{
		MaxIdle:     a.config.connPoolConfig.maxIdle,
		MaxActive:   a.config.connPoolConfig.maxActive,
		GetTimeout:  time.Duration(a.config.connPoolConfig.getTimeout),
		MaxIdleTime: time.Duration(a.config.connPoolConfig.maxIdleTime),
		MaxLifeTime: time.Duration(a.config.connPoolConfig.maxLifeTime),
	})
	if err != nil {
		log.Error("app: create client failed", "err", err)
		return err
	}
	a.client = client

	// 创建服务端
	server, err := NewServer(a.config.socketPath, a.config.codecType,
		a.config.workerPoolConfig.workerCount,
		a.config.workerPoolConfig.queueSize,
		time.Duration(a.config.workerPoolConfig.idleTimeout),
		a.config)
	if err != nil {
		log.Error("app: create server failed", "err", err)
		return err
	}
	a.server = server

	// 同步延迟注册的方法
	syncDeferredHandlers(a.server)

	// 同步创建注册中心客户端
	if envSocket := os.Getenv("REGISTRY_SOCKET"); envSocket != "" {
		rc, err := NewRegistryClient(envSocket, a.config.codecType, a.config.serviceName, a.config.serviceType)
		if err != nil {
			log.Error("app: create registry client failed", "err", err)
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := rc.Register(ctx, nil); err != nil {
				log.Error("app: register to registry failed", "err", err)
			}
			cancel()
			rc.StartHeartbeat(time.Duration(a.config.heartbeatInterval))
			a.mu.Lock()
			a.registry = rc
			a.config.registrySocket = envSocket
			a.mu.Unlock()
			log.Info("app: registry client initialized synchronously")
		}
	}

	// 启动后台协程定期检查环境变量变化
	if a.config.envCheckInterval > 0 {
		go func() {
			getConfig := func() (registrySocket, codecType, serviceName, serviceType string, heartbeatInterval int64) {
				a.mu.RLock()
				defer a.mu.RUnlock()
				return a.config.registrySocket, a.config.codecType, a.config.serviceName, a.config.serviceType, a.config.heartbeatInterval
			}
			onUpdate := func(newClient *RegistryClient) {
				a.mu.Lock()
				defer a.mu.Unlock()
				if a.registry != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = a.registry.Deregister(ctx, "")
					cancel()
					a.registry.Stop()
				}
				a.registry = newClient
				a.config.registrySocket = newClient.client.socketPath
			}
			WatchRegistrySocket(getConfig, onUpdate, a.stopChan, time.Duration(a.config.envCheckInterval))
		}()
		log.Info("app: registry env checker started")
	}

	log.Info("app: initialized successfully")
	return nil
}

// ReloadConfig 热重载业务配置。
func (a *App) ReloadConfig() error {
	log.Info("app: reload business config")
	bizCfg, err := loadBusinessConfig(a.config.rootDir)
	if err != nil {
		log.Error("app: reload business config failed", "err", err)
		return err
	}
	a.bizMu.Lock()
	a.businessConfig = bizCfg
	a.bizMu.Unlock()
	log.Info("app: business config reloaded")
	return nil
}

// Run 启动应用，阻塞直到收到信号。
func (a *App) Run() error {
	log.Info("app: running, waiting for signal...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Info("app: shutdown signal received")
	return a.Shutdown()
}

// Shutdown 优雅关闭。
func (a *App) Shutdown() error {
	a.stopOnce.Do(func() {
		log.Info("app: starting graceful shutdown...")
		close(a.stopChan)

		if a.registry != nil {
			log.Info("app: step 1/4 - unregistering service")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = a.registry.Deregister(ctx, "")
			cancel()
			a.registry.Stop()
			log.Info("app: service unregistered")
		}

		if a.server != nil {
			log.Info("app: step 2/4 - stopping server")
			_ = a.server.Stop()
			log.Info("app: server stopped")
		}

		if a.client != nil {
			log.Info("app: step 3/4 - closing client")
			_ = a.client.Close()
			log.Info("app: client closed")
		}

		a.mu.Lock()
		a.server = nil
		a.client = nil
		a.registry = nil
		a.config = nil
		a.mu.Unlock()

		log.Info("app: step 4/4 - shutdown completed")
	})
	return nil
}

// Client 返回客户端实例。
func (a *App) Client() *Client {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.client
}

// GetRegistry 返回注册中心客户端。
func (a *App) GetRegistry() Registry {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.registry
}

// GetMetrics 获取应用指标。
func (a *App) GetMetrics() map[string]any {
	a.mu.RLock()
	defer a.mu.RUnlock()

	metrics := make(map[string]any)
	if a.server != nil {
		metrics["server"] = a.server.GetMetrics()
	}
	if a.registry != nil {
		metrics["registry"] = map[string]any{"state": a.registry.GetState().String()}
	}
	if objectPool != nil {
		stats := objectPool.Stats()
		metrics["object_pool"] = map[string]any{
			"active": stats.ActiveCount,
			"idle":   stats.IdleCount,
			"total":  stats.CreateCount,
		}
	}
	if a.config != nil {
		metrics["config"] = map[string]any{
			"codec":              a.config.codecType,
			"socket":             a.config.socketPath,
			"queue_size":         a.config.workerPoolConfig.queueSize,
			"rate_limit_enabled": a.config.rateLimitConfig.enabled,
		}
	}
	return metrics
}

// ========== 业务配置接口 ==========
func (a *App) GetBusinessConfig(key string) (any, bool) {
	a.bizMu.RLock()
	defer a.bizMu.RUnlock()
	val, ok := a.businessConfig[key]
	return val, ok
}

func (a *App) SetBusinessConfig(key string, value any) error {
	a.bizMu.Lock()
	defer a.bizMu.Unlock()
	if a.businessConfig == nil {
		a.businessConfig = make(map[string]any)
	}
	a.businessConfig[key] = value
	return saveBusinessConfig(a.config.rootDir, a.businessConfig)
}

func (a *App) DeleteBusinessConfig(key string) error {
	a.bizMu.Lock()
	defer a.bizMu.Unlock()
	delete(a.businessConfig, key)
	return saveBusinessConfig(a.config.rootDir, a.businessConfig)
}

func (a *App) GetAllBusinessConfig() map[string]any {
	a.bizMu.RLock()
	defer a.bizMu.RUnlock()
	copy := make(map[string]any, len(a.businessConfig))
	for k, v := range a.businessConfig {
		copy[k] = v
	}
	return copy
}

// ========== 包级函数（供 api 转发）==========
func GetBusinessConfig(key string) (any, bool) {
	return GetApp().GetBusinessConfig(key)
}
func SetBusinessConfig(key string, value any) error {
	return GetApp().SetBusinessConfig(key, value)
}
func DeleteBusinessConfig(key string) error {
	return GetApp().DeleteBusinessConfig(key)
}
func GetAllBusinessConfig() map[string]any {
	return GetApp().GetAllBusinessConfig()
}

// ========== 日志 ==========
func LogDebug(v ...any) { log.Debug(v...) }
func LogInfo(v ...any)  { log.Info(v...) }
func LogWarn(v ...any)  { log.Warn(v...) }
func LogError(v ...any) { log.Error(v...) }
