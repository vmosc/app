package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmosc/app"
	"github.com/vmosc/app/kernel/log"
	"github.com/vmosc/app/kernel/pool"
)

func TestClient(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := tmpDir + "/client.sock"

	mockServer := StartTestServer(t, socketPath)
	defer mockServer.Close()

	t.Run("客户端创建与发送", func(t *testing.T) {
		testID := "client_send"

		cfg := pool.ConnPoolConfig{
			MaxIdle:     2,
			MaxActive:   5,
			GetTimeout:  2 * time.Second,
			MaxIdleTime: 5 * time.Second,
			MaxLifeTime: 10 * time.Second,
		}

		client, err := app.NewClient(socketPath, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		reqMsg := &app.Message{
			ID:        testID,
			Version:   1,
			Timestamp: time.Now().UnixNano(),
			Method:    "test.method",
			Metadata:  map[string]string{"key": "value"},
			Body:      []byte("test data"),
		}

		resp, err := client.Send(ctx, reqMsg)
		if err != nil {
			t.Fatalf("send request failed: %v", err)
		}

		if resp == nil {
			t.Fatal("response is nil")
		}
		if resp.ID != reqMsg.ID {
			t.Errorf("response ID mismatch: got %s, want %s", resp.ID, reqMsg.ID)
		}
		if string(resp.Body) != "test data" {
			t.Errorf("response body mismatch: got %s, want %s", resp.Body, "test data")
		}

		log.Info("test: client send test passed", "test_id", testID)
	})

	t.Run("客户端超时测试", func(t *testing.T) {
		delaySocket := tmpDir + "/delay.sock"
		delayServer := startDelayServer(t, delaySocket, 200*time.Millisecond)
		defer delayServer.Close()

		cfg := pool.ConnPoolConfig{
			MaxIdle:    1,
			MaxActive:  1,
			GetTimeout: 1 * time.Second,
		}

		client, err := app.NewClient(delaySocket, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		reqMsg := &app.Message{
			ID:     "timeout_test",
			Method: "test.timeout",
			Body:   []byte("timeout test"),
		}

		_, err = client.Send(ctx, reqMsg)
		if err == nil {
			t.Error("expected timeout error, got nil")
		} else {
			log.Info("test: timeout error as expected", "err", err)
		}
	})

	t.Run("客户端连接池复用测试", func(t *testing.T) {
		testID := "conn_pool_reuse"

		cfg := pool.ConnPoolConfig{
			MaxIdle:    2,
			MaxActive:  2,
			GetTimeout: 2 * time.Second,
		}

		client, err := app.NewClient(socketPath, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer client.Close()

		var wg sync.WaitGroup
		successCount := int32(0)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				reqMsg := &app.Message{
					ID:     fmt.Sprintf("%s_%d", testID, idx),
					Method: "test.echo",
					Body:   []byte(fmt.Sprintf("data_%d", idx)),
				}

				_, err := client.Send(ctx, reqMsg)
				if err == nil {
					atomic.AddInt32(&successCount, 1)
				}
			}(i)
		}

		wg.Wait()

		if successCount != 10 {
			t.Errorf("expected 10 successes, got %d", successCount)
		}

		log.Info("test: connection pool reuse test passed", "test_id", testID)
	})

	t.Run("客户端连接探活测试", func(t *testing.T) {
		testID := "alive_check"

		cfg := pool.ConnPoolConfig{
			MaxIdle:    1,
			MaxActive:  1,
			GetTimeout: 2 * time.Second,
		}

		client, err := app.NewClient(socketPath, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		reqMsg := &app.Message{
			ID:     testID,
			Method: "test.alive",
			Body:   []byte("alive test"),
		}

		_, err = client.Send(ctx, reqMsg)
		if err != nil {
			t.Errorf("send failed: %v", err)
		}

		log.Info("test: alive check test passed", "test_id", testID)
	})

	t.Run("客户端连接断开后恢复测试", func(t *testing.T) {
		// 模拟服务端断开，验证客户端后续请求仍能恢复（依赖连接池重建）
		recoverySocket := tmpDir + "/recovery.sock"
		srv := StartTestServer(t, recoverySocket)
		defer srv.Close()

		cfg := pool.ConnPoolConfig{
			MaxIdle:    1,
			MaxActive:  1,
			GetTimeout: 2 * time.Second,
		}

		client, err := app.NewClient(recoverySocket, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		defer client.Close()

		// 第一次请求应成功
		req := &app.Message{ID: "recovery-1", Method: "test.echo", Body: []byte("data1")}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Send(ctx, req)
		cancel()
		if err != nil {
			t.Fatalf("first send failed: %v", err)
		}

		// 关闭并重启服务端，模拟断连
		srv.Close()
		time.Sleep(50 * time.Millisecond)
		srv2 := StartTestServer(t, recoverySocket)
		defer srv2.Close()

		// 第二次请求应能自动恢复（连接池会剔除失效连接并重连）
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = client.Send(ctx2, &app.Message{ID: "recovery-2", Method: "test.echo", Body: []byte("data2")})
		cancel2()
		if err != nil {
			t.Errorf("second send after reconnect failed: %v", err)
		}
	})
}

func startDelayServer(t testing.TB, socketPath string, delay time.Duration) net.Listener {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to start delay server: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
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
				time.Sleep(delay)
				conn.Write(header)
				conn.Write(data)
			}()
		}
	}()
	return listener
}
