package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmosc/app/api"
)

// TestServerRateLimit 测试服务端限流逻辑（需要配置 enabled）。
// 注意：当前默认配置 rateLimitConfig.enabled = false，此测试验证限流关闭时正常放行。
func TestServerRateLimit(t *testing.T) {
	app, cleanup := StartTestApp(t)
	defer cleanup()

	api.RegisterFunc(func(ctx context.Context, req *api.Message) (*api.Message, error) {
		return &api.Message{ID: req.ID, Body: []byte("ok")}, nil
	}, "test.nolimit")

	time.Sleep(100 * time.Millisecond)

	client := app.Client()

	// 并发发送大量请求，验证限流未开启时不会拒绝
	var wg sync.WaitGroup
	concurrency := 100
	successCount := int32(0)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			req := &api.Message{
				ID:     fmt.Sprintf("rate-%d", idx),
				Method: "test.nolimit",
				Body:   []byte("test"),
			}
			_, err := client.Send(ctx, req)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if successCount != int32(concurrency) {
		t.Errorf("expected %d successes, got %d (rate limit should be disabled)", concurrency, successCount)
	}

	metrics := app.GetMetrics()
	if serverMetrics, ok := metrics["server"].(map[string]any); ok {
		t.Logf("server metrics after rate test: %+v", serverMetrics)
	}
}
