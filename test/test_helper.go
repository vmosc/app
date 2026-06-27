package test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmosc/app/api"
	"github.com/vmosc/app/kernel/config"
)

var shortTempSeq uint64

// shortTempDir 创建一个短路径的临时目录，解决 Windows 下 Unix socket 路径过长问题。
func shortTempDir(t testing.TB) string {
	shortBase := filepath.Join("tmp", "apptest")
	if runtime.GOOS == "windows" {
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("failed to get working directory: %v", err)
		}
		drive := filepath.VolumeName(wd)
		if drive == "" && len(wd) >= 2 && wd[1] == ':' {
			drive = wd[:2]
		}
		if drive == "" {
			drive = "C:"
		}
		driveRoot := drive + string(os.PathSeparator)
		shortBase = filepath.Join(driveRoot, "tmp", "apptest")
	}
	if err := os.MkdirAll(shortBase, 0755); err != nil {
		t.Fatalf("failed to create short base dir %s: %v", shortBase, err)
	}

	safeName := strings.ReplaceAll(t.Name(), "/", "_")
	safeName = strings.ReplaceAll(safeName, "\\", "_")
	safeName = strings.ReplaceAll(safeName, ":", "")
	seq := atomic.AddUint64(&shortTempSeq, 1)
	testDir := filepath.Join(shortBase, fmt.Sprintf("%s_%d", safeName, seq))

	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("failed to create test dir %s: %v", testDir, err)
	}
	return testDir
}

// StartTestServer 启动一个简单的 echo 测试服务器，用于客户端测试。
func StartTestServer(t testing.TB, socketPath string) net.Listener {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to start test server: %v", err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleTestConnection(conn)
		}
	}()
	return listener
}

// handleTestConnection 处理单个连接，将接收到的数据原样返回。
func handleTestConnection(conn net.Conn) {
	defer conn.Close()
	for {
		header := make([]byte, 4)
		if _, err := conn.Read(header); err != nil {
			return
		}
		length := uint32(header[0])<<24 | uint32(header[1])<<16 |
			uint32(header[2])<<8 | uint32(header[3])
		data := make([]byte, length)
		if _, err := conn.Read(data); err != nil {
			return
		}
		conn.Write(header)
		conn.Write(data)
	}
}

// StartBenchServer 启动一个用于基准测试的 echo 服务器。
func StartBenchServer(b *testing.B, socketPath string) net.Listener {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		b.Fatalf("failed to start bench server: %v", err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleTestConnection(conn)
		}
	}()
	return listener
}

// CreateTestBusinessConfig 创建测试用的业务配置文件（config.yaml），用于应用初始化。
func CreateTestBusinessConfig(t testing.TB, configData map[string]any) {
	mgr := config.NewYAML()
	mgr.Init("config.yaml")
	if err := mgr.Save(configData); err != nil {
		t.Fatalf("failed to create test business config: %v", err)
	}
}

// FindSocketFile 在当前目录查找生成的 .sock 文件，用于测试客户端连接。
func FindSocketFile(t testing.TB) string {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sock") {
			return entry.Name()
		}
	}
	t.Fatal("no socket file found")
	return ""
}

// StartTestApp 初始化测试应用并返回应用实例和清理函数。
// 它会创建临时目录、配置文件和 socket，并等待服务端就绪。
func StartTestApp(t testing.TB) (*api.App, func()) {
	tmpDir := shortTempDir(t)
	origDir, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmp dir %s: %v", tmpDir, err)
	}

	t.Cleanup(func() {
		_ = os.Chdir(origDir)
		_ = os.RemoveAll(tmpDir)
	})

	CreateTestBusinessConfig(t, make(map[string]any))

	oldArgs0 := os.Args[0]
	os.Args[0] = "s"
	defer func() { os.Args[0] = oldArgs0 }()

	app := api.NewApp()
	if err := app.Init("test"); err != nil {
		t.Fatalf("app init failed: %v", err)
	}

	socketPath := FindSocketFile(t)
	deadline := time.Now().Add(15 * time.Second)
	var lastStatErr error
	var lastDialErr error
	for {
		if time.Now().After(deadline) {
			t.Fatalf("server not ready after 15 seconds (socket: %s, stat_err: %v, dial_err: %v)",
				socketPath, lastStatErr, lastDialErr)
		}
		if _, err := os.Stat(socketPath); err != nil {
			lastStatErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
		if err != nil {
			lastDialErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		conn.Close()
		break
	}

	cleanup := func() { app.Shutdown() }
	return app, cleanup
}

// CheckGoroutineLeak 检查是否有 goroutine 泄露。
// before 是测试开始前的 goroutine 数量，margin 是允许的波动上限。
// 用法：在测试开始时记录 before := runtime.NumGoroutine()，并在 defer 中调用本函数。
func CheckGoroutineLeak(t testing.TB, before int, margin int) {
	t.Helper()
	// 强制 GC，让可回收的 goroutine 尽快结束
	runtime.GC()
	// 给 goroutine 一点时间退出
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+margin {
		t.Errorf("possible goroutine leak: before=%d, after=%d (margin=%d)", before, after, margin)
	}
}
