// app/test/log_test.go
package test

import (
	"app/kernel/log"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	_ "unsafe" // for go:linkname
)

//go:linkname handleError app/kernel/log.handleError
func handleError(err error)

// 每个测试开始前重置日志系统，避免重复初始化错误
func resetLog() {
	_ = log.Reset()
}

// TestLogInitSingleFile 测试单文件模式初始化
func TestLogInitSingleFile(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	err := log.Init(logFile)
	if err != nil {
		t.Fatal(err)
	}

	log.Info("hello")
	// 确保写入完成并关闭文件
	_ = log.Reset()
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "INFO hello") {
		t.Errorf("unexpected content: %s", data)
	}
}

// TestLogInitMultiFile 测试多文件模式初始化
func TestLogInitMultiFile(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	err := log.Init(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	log.Info("info msg")
	log.Error("error msg")
	log.MetricInc("test.metric")

	// 关闭文件以释放句柄
	_ = log.Reset()

	expectedFiles := []string{"log-", "error-", "metric-"}
	for _, prefix := range expectedFiles {
		matches, _ := filepath.Glob(filepath.Join(tmpDir, prefix+"*.log"))
		if len(matches) == 0 {
			t.Errorf("file with prefix %s not created", prefix)
		}
	}
}

// TestLogDoubleInit 测试重复初始化应返回错误
func TestLogDoubleInit(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	err := log.Init(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	err = log.Init(tmpDir)
	if err == nil {
		t.Error("second Init should return error")
	}
	_ = log.Reset()
}

// TestLogErrorCode 测试带错误码的错误日志
func TestLogErrorCode(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	log.Init(tmpDir)

	log.ErrorCode(500, "internal error")

	_ = log.Reset()

	matches, _ := filepath.Glob(filepath.Join(tmpDir, "error-*.log"))
	if len(matches) == 0 {
		t.Fatal("error log not created")
	}
	data, _ := os.ReadFile(matches[0])
	expected := "code=500 internal error"
	if !strings.Contains(string(data), expected) {
		t.Errorf("expected %q, got %s", expected, data)
	}
}

// TestLogMetricTiming 测试耗时指标
func TestLogMetricTiming(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	log.Init(tmpDir)

	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	log.MetricTiming("test", time.Since(start))

	_ = log.Reset()

	matches, _ := filepath.Glob(filepath.Join(tmpDir, "metric-*.log"))
	if len(matches) == 0 {
		t.Fatal("metric log not created")
	}
	data, _ := os.ReadFile(matches[0])
	if !strings.Contains(string(data), "timing test") {
		t.Errorf("missing timing metric: %s", data)
	}
}

// TestLogStartTiming 测试 StartTiming 辅助函数
func TestLogStartTiming(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	log.Init(tmpDir)

	func() {
		defer log.StartTiming("func")()
		time.Sleep(5 * time.Millisecond)
	}()

	_ = log.Reset()

	matches, _ := filepath.Glob(filepath.Join(tmpDir, "metric-*.log"))
	if len(matches) == 0 {
		t.Fatal("metric log not created")
	}
	data, _ := os.ReadFile(matches[0])
	if !strings.Contains(string(data), "timing func") {
		t.Errorf("missing timing metric: %s", data)
	}
}

// TestLogRotation 测试日志切割。
// 使用亚分钟级间隔，配合 timeToSpan 的秒级精度，快速验证轮转逻辑。
func TestLogRotation(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	cfg := log.Config{
		LogPath:          tmpDir,
		RotationInterval: 100 * time.Millisecond,
		RetentionPeriod:  0,
	}
	err := log.InitWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		log.Info("rotation test")
		time.Sleep(110 * time.Millisecond)

		files, _ := filepath.Glob(filepath.Join(tmpDir, "log-*.log"))
		if len(files) >= 2 {
			break
		}
	}

	_ = log.Reset()

	files, _ := filepath.Glob(filepath.Join(tmpDir, "log-*.log"))
	if len(files) < 2 {
		t.Errorf("expected at least 2 log files after rotation, got %d", len(files))
	}
}

// TestLogCleanup 测试日志清理（使用文件 ModTime）
func TestLogCleanup(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	cfg := log.Config{
		LogPath:          tmpDir,
		RotationInterval: 1 * time.Second,
		RetentionPeriod:  2 * time.Second,
	}
	err := log.InitWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 创建一个旧文件（模拟过期）
	oldTime := time.Now().Add(-10 * time.Minute)
	oldFile := filepath.Join(tmpDir, "log-old.log")
	if err := os.WriteFile(oldFile, []byte("old log"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Skip("Chtimes not supported, skipping cleanup test")
	}

	// 手动触发清理（不会死锁）
	log.CleanupNow()

	// 检查旧文件是否被删除
	if _, err := os.Stat(oldFile); err == nil {
		t.Error("old log file should have been cleaned up, but still exists")
	}
	_ = log.Reset()
}

// TestLogConcurrentWrites 测试并发写入
func TestLogConcurrentWrites(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	log.Init(tmpDir)

	var wg sync.WaitGroup
	n := 100
	wg.Add(n * 4)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			log.Debug("debug", id)
		}(i)
		go func(id int) {
			defer wg.Done()
			log.Info("info", id)
		}(i)
		go func(id int) {
			defer wg.Done()
			log.Error("error", id)
		}(i)
		go func(id int) {
			defer wg.Done()
			log.MetricInc("metric")
		}(i)
	}
	wg.Wait()
	_ = log.Reset()

	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	for _, f := range files {
		data, _ := os.ReadFile(f)
		lines := bytes.Count(data, []byte{'\n'})
		if lines < n {
			t.Errorf("file %s has only %d lines, expected at least %d", f, lines, n)
		}
	}
}

// TestLogErrorHandler 测试错误处理回调。
// 直接调用未导出的 handleError 来触发回调，避免依赖平台特定的文件写入失败场景。
func TestLogErrorHandler(t *testing.T) {
	resetLog()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := t.TempDir()
	log.Init(tmpDir)

	called := false
	mu := sync.Mutex{}

	log.SetErrorHandler(func(err error) {
		mu.Lock()
		called = true
		mu.Unlock()
		t.Logf("error handler called: %v", err)
	})

	// 通过 linkname 调用内部的 handleError，模拟日志写入失败
	handleError(errors.New("simulated write error"))

	if !called {
		t.Error("error handler was not called on simulated error")
	}
	_ = log.Reset()
}

// 性能基准测试
func BenchmarkLogInfo(b *testing.B) {
	_ = log.Reset()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := b.TempDir()
	if err := log.Init(tmpDir); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		log.Info("benchmark message")
	}
	_ = log.Reset()
}

func BenchmarkLogConcurrentInfo(b *testing.B) {
	_ = log.Reset()
	defer func() { _ = log.Reset(); _ = log.Init("./test_logs") }()
	tmpDir := b.TempDir()
	if err := log.Init(tmpDir); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			log.Info("parallel benchmark")
		}
	})
	_ = log.Reset()
}
