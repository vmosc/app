// app/test/app_test.go
package test

import (
	"app/api"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestAppIntegration 整体集成测试。
func TestAppIntegration(t *testing.T) {
	t.Run("完整生命周期测试", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterFunc(testEchoHandler, "test.echo")

		metrics := app.GetMetrics()
		if metrics == nil {
			t.Error("metrics is nil")
		}
		if _, ok := metrics["server"]; !ok {
			t.Error("server metrics not found")
		}

		api.LogInfo("test: integration test completed")
	})

	t.Run("优雅关闭测试", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		testID := fmt.Sprintf("shutdown_%d", time.Now().UnixNano())
		api.LogInfo("test: starting shutdown test", "test_id", testID)

		api.RegisterFunc(testEchoHandler, "test.echo")

		shutdownDone := make(chan struct{})
		go func() {
			time.Sleep(500 * time.Millisecond)
			err := app.Shutdown()
			if err != nil {
				t.Errorf("shutdown failed: %v", err)
			}
			close(shutdownDone)
		}()

		select {
		case <-shutdownDone:
			api.LogInfo("test: shutdown completed", "test_id", testID)
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown timeout")
		}
	})
}

// TestAppConcurrency 并发安全测试（业务配置读写）。
func TestAppConcurrency(t *testing.T) {
	t.Run("并发配置读写", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		var wg sync.WaitGroup
		concurrency := 50

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = api.GetConfig("any.key")
			}()
		}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				metrics := app.GetMetrics()
				if metrics == nil {
					t.Error("metrics is nil")
				}
			}()
		}
		wg.Wait()
		api.LogInfo("test: concurrent config access passed")
	})
}

// testEchoHandler 测试用的 echo 处理器。
func testEchoHandler(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{
		ID:        req.ID,
		Version:   req.Version,
		Timestamp: time.Now().UnixNano(),
		Method:    "test.echo.response",
		Metadata:  req.Metadata,
		Body:      req.Body,
	}, nil
}
