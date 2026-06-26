package app

import (
	"app/kernel/config"
	"app/kernel/log"
	"os"
	"path/filepath"
)

// Config 框架私有配置（所有字段私有）。
type Config struct {
	rootDir    string
	socketPath string
	configPath string
	logDir     string
	codecType  string

	registrySocket    string
	heartbeatInterval int64
	envCheckInterval  int64
	serviceName       string
	serviceType       string

	workerPoolConfig struct {
		workerCount int
		queueSize   int
		idleTimeout int64
	}

	connPoolConfig struct {
		maxIdle     int
		maxActive   int
		getTimeout  int64
		maxIdleTime int64
		maxLifeTime int64
	}

	objectPoolConfig ObjectPoolConfig

	rateLimitConfig struct {
		enabled     bool
		maxQPS      int
		maxQueueLen int
	}
}

// ObjectPoolConfig 对象池配置。
type ObjectPoolConfig struct {
	maxSize             int
	minSize             int
	getTimeout          int64
	maxIdleTime         int64
	maxLifeTime         int64
	healthCheckInterval int64
	enableAutoScale     bool
}

var defaultConfig = Config{
	rootDir:           "",
	socketPath:        "",
	configPath:        "",
	logDir:            "log",
	codecType:         "binary",
	registrySocket:    "",
	heartbeatInterval: int64(30 * 1e9),
	envCheckInterval:  int64(10 * 60 * 1e9),
	serviceName:       "",
	serviceType:       "",

	workerPoolConfig: struct {
		workerCount int
		queueSize   int
		idleTimeout int64
	}{
		workerCount: 0,
		queueSize:   1024,
		idleTimeout: int64(10 * 1e9),
	},

	connPoolConfig: struct {
		maxIdle     int
		maxActive   int
		getTimeout  int64
		maxIdleTime int64
		maxLifeTime int64
	}{
		maxIdle:     10,
		maxActive:   100,
		getTimeout:  int64(30 * 1e9),
		maxIdleTime: int64(5 * 60 * 1e9),
		maxLifeTime: int64(30 * 60 * 1e9),
	},

	objectPoolConfig: ObjectPoolConfig{
		maxSize:             1024,
		minSize:             32,
		getTimeout:          int64(100 * 1e6),
		maxIdleTime:         int64(30 * 1e9),
		maxLifeTime:         int64(5 * 60 * 1e9),
		healthCheckInterval: int64(30 * 1e9),
		enableAutoScale:     false,
	},

	rateLimitConfig: struct {
		enabled     bool
		maxQPS      int
		maxQueueLen int
	}{
		enabled:     false,
		maxQPS:      1000,
		maxQueueLen: 100,
	},
}

// loadBusinessConfig 从指定 rootDir 下的 config.yaml 加载业务配置。
func loadBusinessConfig(rootDir string) (map[string]any, error) {
	if rootDir == "" {
		var err error
		rootDir, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	cfgPath := filepath.Join(rootDir, "config.yaml")

	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		return make(map[string]any), nil
	}

	mgr := config.NewYAML()
	mgr.Init(cfgPath)

	var cfg map[string]any
	if err := mgr.Load(&cfg); err != nil {
		log.Error("config: failed to load business config", "err", err)
		return nil, err
	}
	return cfg, nil
}

// saveBusinessConfig 保存业务配置到指定 rootDir 下的 config.yaml。
func saveBusinessConfig(rootDir string, cfg map[string]any) error {
	if rootDir == "" {
		var err error
		rootDir, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	cfgPath := filepath.Join(rootDir, "config.yaml")

	mgr := config.NewYAML()
	mgr.Init(cfgPath)
	return mgr.Save(cfg)
}
