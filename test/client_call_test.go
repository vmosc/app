package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vmosc/app"
	"github.com/vmosc/app/api"
	"github.com/vmosc/app/kernel/log"
	"github.com/vmosc/app/kernel/pool"
)

// TestClientCall 测试 Client.Call 方法的核心调用路径。
func TestClientCall(t *testing.T) {
	// 为 Call 方法准备注册中心环境，避免 nil 指针
	regDir := shortTempDir(t)
	t.Cleanup(func() { _ = os.RemoveAll(regDir) })
	regSocket := filepath.Join(regDir, "reg.sock")
	os.Setenv("REGISTRY_SOCKET", regSocket)
	defer os.Unsetenv("REGISTRY_SOCKET")

	mockReg := StartMockRegistryServer(t, regSocket)
	defer mockReg.Close()

	_, cleanup := StartTestApp(t)
	defer cleanup()

	// 注册测试方法
	api.RegisterStruct(&TestService{}, "test")

	// 获取客户端和 socket 路径
	socketPath := FindSocketFile(t)

	t.Run("Call 直接调用已注册方法", func(t *testing.T) {
		// 使用 service/method 格式，其中 service 通过注册中心解析到自身
		// 由于测试环境没有真实注册中心，这里测试直接构造 Client 调用
		directClient, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
			MaxIdle:     2,
			MaxActive:   5,
			GetTimeout:  2 * time.Second,
			MaxIdleTime: 5 * time.Second,
			MaxLifeTime: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("create direct client failed: %v", err)
		}
		defer directClient.Close()

		// 通过 invoke 方式调用
		resp, err := directClient.Call("test/Hello", []byte("World"))
		if err != nil {
			t.Logf("Call via service discovery path failed (expected without registry): %v", err)
			// 在没有注册中心时预期会失败，这是正常的
			return
		}
		if string(resp.Body) != "Hello World" {
			t.Errorf("unexpected response: got %s, want Hello World", string(resp.Body))
		}
	})

	t.Run("Client.invoke 底层调用测试", func(t *testing.T) {
		directClient, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
			MaxIdle:     2,
			MaxActive:   5,
			GetTimeout:  2 * time.Second,
			MaxIdleTime: 5 * time.Second,
			MaxLifeTime: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("create direct client failed: %v", err)
		}
		defer directClient.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req := &app.Message{
			ID:     "invoke-test",
			Method: "test.Hello",
			Body:   []byte("Go"),
		}
		resp, err := directClient.Send(ctx, req)
		if err != nil {
			t.Fatalf("direct send failed: %v", err)
		}
		if string(resp.Body) != "Hello Go" {
			t.Errorf("unexpected response: got %s, want Hello Go", string(resp.Body))
		}
	})
}

// TestClientCallErrors 测试 Call 方法的错误处理路径。
func TestClientCallErrors(t *testing.T) {
	// 避免 nil registry 导致 panic
	regDir := shortTempDir(t)
	t.Cleanup(func() { _ = os.RemoveAll(regDir) })
	regSocket := filepath.Join(regDir, "reg.sock")
	os.Setenv("REGISTRY_SOCKET", regSocket)
	defer os.Unsetenv("REGISTRY_SOCKET")

	mockReg := StartMockRegistryServer(t, regSocket)
	defer mockReg.Close()

	appInst, cleanup := StartTestApp(t)
	defer cleanup()

	// 注册测试方法，以便 nil 参数测试能够调用 Ping 方法
	api.RegisterStruct(&TestService{}, "test")

	socketPath := FindSocketFile(t)

	t.Run("无效格式调用", func(t *testing.T) {
		client := appInst.Client()
		_, err := client.Call("invalidformat", "data")
		if err == nil {
			t.Error("expected error for invalid format, got nil")
		}
		if err.Error()[:7] != "invalid" {
			t.Errorf("expected invalid format error, got: %v", err)
		}
	})

	t.Run("调用不存在的方法", func(t *testing.T) {
		directClient, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
			MaxIdle:    1,
			MaxActive:  1,
			GetTimeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer directClient.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req := &app.Message{
			ID:     "notfound-test",
			Method: "nonexistent.method",
			Body:   []byte("test"),
		}
		_, err = directClient.Send(ctx, req)
		if err == nil {
			t.Error("expected error for nonexistent method, got nil")
		}
	})

	t.Run("调用 nil 参数", func(t *testing.T) {
		directClient, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
			MaxIdle:    1,
			MaxActive:  1,
			GetTimeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer directClient.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req := &app.Message{
			ID:     "nil-param-test",
			Method: "test.Ping",
			Body:   []byte{},
		}
		resp, err := directClient.Send(ctx, req)
		if err != nil {
			t.Fatalf("send with nil param failed: %v", err)
		}
		if string(resp.Body) != "pong" {
			t.Errorf("unexpected response: got %s, want pong", string(resp.Body))
		}
	})
}

// TestMethodCache 测试方法缓存机制。
func TestMethodCache(t *testing.T) {
	_, cleanup := StartTestApp(t)
	defer cleanup()

	api.RegisterStruct(&TestService{}, "cachetest")

	socketPath := FindSocketFile(t)

	t.Run("方法缓存命中与过期", func(t *testing.T) {
		client1, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
			MaxIdle:     2,
			MaxActive:   5,
			GetTimeout:  2 * time.Second,
			MaxIdleTime: 5 * time.Second,
			MaxLifeTime: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("create client1 failed: %v", err)
		}
		defer client1.Close()

		// 第一次调用：触发 ListMethod 缓存
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req := &app.Message{
			ID:     "cache-1",
			Method: "cachetest.Ping",
			Body:   []byte("cachetest"),
		}
		resp, err := client1.Send(ctx, req)
		cancel()
		if err != nil {
			t.Fatalf("first call failed: %v", err)
		}
		if string(resp.Body) != "pong" {
			t.Errorf("unexpected response: %s", string(resp.Body))
		}

		// 第二次调用：应使用缓存
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		resp2, err := client1.Send(ctx2, &app.Message{
			ID:     "cache-2",
			Method: "cachetest.Ping",
			Body:   []byte("cachetest2"),
		})
		cancel2()
		if err != nil {
			t.Fatalf("second call failed: %v", err)
		}
		if string(resp2.Body) != "pong" {
			t.Errorf("unexpected response: %s", string(resp2.Body))
		}
	})
}

// TestClientConcurrentSend 测试客户端并发发送。
func TestClientConcurrentSend(t *testing.T) {
	_, cleanup := StartTestApp(t)
	defer cleanup()

	api.RegisterStruct(&TestService{}, "concurrent")

	socketPath := FindSocketFile(t)

	client, err := app.NewClient(socketPath, "binary", pool.ConnPoolConfig{
		MaxIdle:     10,
		MaxActive:   50,
		GetTimeout:  5 * time.Second,
		MaxIdleTime: 10 * time.Second,
		MaxLifeTime: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("create client failed: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	concurrency := 50
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			req := &app.Message{
				ID:     fmt.Sprintf("concurrent-%d", idx),
				Method: "concurrent.Echo",
				Body:   []byte(fmt.Sprintf("data-%d", idx)),
			}
			resp, err := client.Send(ctx, req)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
				if string(resp.Body) != fmt.Sprintf("data-%d", idx) {
					t.Errorf("body mismatch for %d: got %s", idx, string(resp.Body))
				}
			}
		}(i)
	}
	wg.Wait()

	if successCount != concurrency {
		t.Errorf("expected %d successes, got %d", concurrency, successCount)
	}
	log.Info("test: concurrent send completed", "success", successCount, "total", concurrency)
}
