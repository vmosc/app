package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vmosc/app/kernel/config"
)

type KernelTestConfig struct {
	Server struct {
		Port int    `json:"port" yaml:"port"`
		Host string `json:"host" yaml:"host"`
	} `json:"server" yaml:"server"`
	Features []string `json:"features" yaml:"features"`
	Database struct {
		Host     string `json:"host" yaml:"host"`
		Port     int    `json:"port" yaml:"port"`
		Username string `json:"username" yaml:"username"`
		Password string `json:"password" yaml:"password"`
	} `json:"database" yaml:"database"`
	Logging struct {
		Level  string `json:"level" yaml:"level"`
		Output string `json:"output" yaml:"output"`
	} `json:"logging" yaml:"logging"`
	Metrics struct {
		Enabled bool     `json:"enabled" yaml:"enabled"`
		Tags    []string `json:"tags" yaml:"tags"`
	} `json:"metrics" yaml:"metrics"`
}

func setupTestDir(t *testing.T) (string, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return tmpDir, func() { os.RemoveAll(tmpDir) }
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write file %s: %v", path, err)
	}
}

func TestJSONManager_Init(t *testing.T) {
	mgr := config.NewJSON()
	mgr.Init("")
	custom := "/custom/path.json"
	mgr.Init(custom)
}

func TestJSONManager_Load_SingleFile(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.json")
	content := `{
		"server": {"port": 8080, "host": "localhost"},
		"features": ["log", "metrics"],
		"database": {"host": "db.example.com", "port": 5432}
	}`
	writeFile(t, file, content)

	mgr := config.NewJSON()
	mgr.Init(file)

	var cfg KernelTestConfig
	if err := mgr.Load(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8080 || cfg.Server.Host != "localhost" {
		t.Errorf("Server = %+v, want {8080 localhost}", cfg.Server)
	}
	if len(cfg.Features) != 2 || cfg.Features[0] != "log" {
		t.Errorf("Features = %v, want [log metrics]", cfg.Features)
	}
}

func TestJSONManager_Load_MultiFile(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	base := filepath.Join(tmpDir, "00-base.json")
	writeFile(t, base, `{
		"server": {"port": 8080, "host": "localhost"},
		"database": {"host": "db.example.com", "port": 5432},
		"logging": {"level": "info", "output": "stdout"}
	}`)

	override := filepath.Join(tmpDir, "01-override.json")
	writeFile(t, override, `{
		"server": {"port": 9090},
		"database": {"username": "admin", "password": "secret"},
		"metrics": {"enabled": true, "tags": ["prod"]}
	}`)

	mgr := config.NewJSON()
	mgr.Init(tmpDir)

	var cfg KernelTestConfig
	if err := mgr.Load(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 9090 || cfg.Server.Host != "localhost" {
		t.Errorf("Server = %+v, want {9090 localhost}", cfg.Server)
	}
	if cfg.Database.Username != "admin" || cfg.Database.Password != "secret" {
		t.Errorf("Database credentials missing")
	}
	if !cfg.Metrics.Enabled || len(cfg.Metrics.Tags) != 1 || cfg.Metrics.Tags[0] != "prod" {
		t.Errorf("Metrics = %+v, want {true [prod]}", cfg.Metrics)
	}
}

func TestJSONManager_Save(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.json")
	mgr := config.NewJSON()
	mgr.Init(file)

	cfg := &KernelTestConfig{}
	cfg.Server.Port = 8080
	cfg.Server.Host = "localhost"
	cfg.Features = []string{"log", "metrics"}

	if err := mgr.Save(cfg); err != nil {
		t.Fatal(err)
	}

	var loaded KernelTestConfig
	if err := mgr.Load(&loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Server.Port != 8080 || loaded.Server.Host != "localhost" {
		t.Errorf("After save/load, got %+v", loaded.Server)
	}
}

func TestJSONManager_Reload(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.json")
	writeFile(t, file, `{"server": {"port": 8080}}`)

	mgr := config.NewJSON()
	mgr.Init(file)

	var cfg KernelTestConfig
	if err := mgr.Reload(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("After reload, port = %d, want 8080", cfg.Server.Port)
	}

	writeFile(t, file, `{"server": {"port": 9090}}`)
	if err := mgr.Reload(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("After second reload, port = %d, want 9090", cfg.Server.Port)
	}
}

func TestYAMLManager_Init(t *testing.T) {
	mgr := config.NewYAML()
	mgr.Init("")
	custom := "/custom/path.yaml"
	mgr.Init(custom)
}

func TestYAMLManager_Load_SingleFile(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.yaml")
	content := `
server:
  port: 8080
  host: localhost
features:
  - log
  - metrics
database:
  host: db.example.com
  port: 5432
`
	writeFile(t, file, content)

	mgr := config.NewYAML()
	mgr.Init(file)

	var cfg KernelTestConfig
	if err := mgr.Load(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8080 || cfg.Server.Host != "localhost" {
		t.Errorf("Server = %+v, want {8080 localhost}", cfg.Server)
	}
	if len(cfg.Features) != 2 || cfg.Features[0] != "log" {
		t.Errorf("Features = %v, want [log metrics]", cfg.Features)
	}
}

func TestYAMLManager_Load_MultiFile(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	base := filepath.Join(tmpDir, "00-base.yaml")
	writeFile(t, base, `
server:
  port: 8080
  host: localhost
database:
  host: db.example.com
  port: 5432
logging:
  level: info
  output: stdout
`)

	override := filepath.Join(tmpDir, "01-override.yml")
	writeFile(t, override, `
server:
  port: 9090
database:
  username: admin
  password: secret
metrics:
  enabled: true
  tags:
    - prod
`)

	mgr := config.NewYAML()
	mgr.Init(tmpDir)

	var cfg KernelTestConfig
	if err := mgr.Load(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 9090 || cfg.Server.Host != "localhost" {
		t.Errorf("Server = %+v, want {9090 localhost}", cfg.Server)
	}
	if cfg.Database.Username != "admin" || cfg.Database.Password != "secret" {
		t.Errorf("Database credentials missing")
	}
	if !cfg.Metrics.Enabled || len(cfg.Metrics.Tags) != 1 || cfg.Metrics.Tags[0] != "prod" {
		t.Errorf("Metrics = %+v, want {true [prod]}", cfg.Metrics)
	}
}

func TestYAMLManager_Save(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.yaml")
	mgr := config.NewYAML()
	mgr.Init(file)

	cfg := &KernelTestConfig{}
	cfg.Server.Port = 8080
	cfg.Server.Host = "localhost"
	cfg.Features = []string{"log", "metrics"}

	if err := mgr.Save(cfg); err != nil {
		t.Fatal(err)
	}

	var loaded KernelTestConfig
	if err := mgr.Load(&loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.Server.Port != 8080 || loaded.Server.Host != "localhost" {
		t.Errorf("After save/load, got %+v", loaded.Server)
	}
}

func TestYAMLManager_Reload(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	file := filepath.Join(tmpDir, "config.yaml")
	writeFile(t, file, "server: {port: 8080}")

	mgr := config.NewYAML()
	mgr.Init(file)

	var cfg KernelTestConfig
	if err := mgr.Reload(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("After reload, port = %d, want 8080", cfg.Server.Port)
	}

	writeFile(t, file, "server: {port: 9090}")
	if err := mgr.Reload(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("After second reload, port = %d, want 9090", cfg.Server.Port)
	}
}

func TestManagerErrors(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	t.Run("non-existent file", func(t *testing.T) {
		mgr := config.NewJSON()
		mgr.Init("/nonexistent/path.json")
		var cfg KernelTestConfig
		if err := mgr.Load(&cfg); err == nil {
			t.Error("Load with non-existent file should return error")
		}
	})

	t.Run("empty directory", func(t *testing.T) {
		mgr := config.NewYAML()
		mgr.Init(tmpDir)
		var cfg KernelTestConfig
		err := mgr.Load(&cfg)
		if err != nil {
			t.Errorf("Load from empty dir should succeed, got error: %v", err)
		}
	})

	t.Run("save to directory", func(t *testing.T) {
		mgr := config.NewJSON()
		mgr.Init(tmpDir)
		var cfg KernelTestConfig
		err := mgr.Save(&cfg)
		if err != nil {
			t.Errorf("Save to directory should succeed, got error: %v", err)
		}
		// 检查是否创建了默认文件
		files, _ := os.ReadDir(tmpDir)
		found := false
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".json") {
				found = true
				break
			}
		}
		if !found {
			t.Error("Save to directory did not create a config file")
		}
	})

	t.Run("atomic write cleanup", func(t *testing.T) {
		file := filepath.Join(tmpDir, "atomic.json")
		mgr := config.NewJSON()
		mgr.Init(file)

		cfg := &KernelTestConfig{}
		cfg.Server.Port = 1234

		if err := mgr.Save(cfg); err != nil {
			t.Fatal(err)
		}
		tmpFile := file + ".tmp"
		if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
			t.Errorf("Temporary file %s was not cleaned up", tmpFile)
		}
	})
}
