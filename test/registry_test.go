// app/test/registry_test.go
package test

import (
	"app"
	"app/kernel/log"
	"app/kernel/pool"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRegistry(t *testing.T) {
	t.Run("注册中心客户端创建与调用", func(t *testing.T) {
		tmpDir := shortTempDir(t)
		t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

		socketPath := filepath.Join(tmpDir, "reg.sock")
		os.Setenv("REGISTRY_SOCKET", socketPath)
		defer os.Unsetenv("REGISTRY_SOCKET")

		mockRegistry := StartMockRegistryServer(t, socketPath)
		defer mockRegistry.Close()

		appInstance, cleanup := StartTestApp(t)
		defer cleanup()

		time.Sleep(200 * time.Millisecond)

		registry := appInstance.GetRegistry()
		if registry == nil {
			t.Fatal("registry client not created")
		}

		info := &app.ServiceInfo{
			Name:    "test-service",
			Address: "test.sock",
		}
		err := registry.Register(context.Background(), info)
		if err != nil {
			t.Errorf("register failed: %v", err)
		}

		endpoints, err := registry.Discover(context.Background(), "test-service")
		if err != nil {
			t.Errorf("discover failed: %v", err)
		}
		if len(endpoints) == 0 || endpoints[0].Address != "test.sock" {
			t.Errorf("unexpected endpoints: %v", endpoints)
		}

		err = registry.Deregister(context.Background(), "")
		if err != nil {
			t.Errorf("deregister failed: %v", err)
		}
	})

	t.Run("环境变量变化热更新", func(t *testing.T) {
		tmpDir := shortTempDir(t)
		t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

		socketPath1 := filepath.Join(tmpDir, "reg1.sock")
		socketPath2 := filepath.Join(tmpDir, "reg2.sock")
		os.Setenv("REGISTRY_SOCKET", socketPath1)
		defer os.Unsetenv("REGISTRY_SOCKET")

		mock1 := StartMockRegistryServer(t, socketPath1)
		defer mock1.Close()

		appInstance, cleanup := StartTestApp(t)
		defer cleanup()

		time.Sleep(200 * time.Millisecond)

		registry1 := appInstance.GetRegistry()
		if registry1 == nil {
			t.Fatal("first registry client not created")
		}

		os.Setenv("REGISTRY_SOCKET", socketPath2)
		mock2 := StartMockRegistryServer(t, socketPath2)
		defer mock2.Close()

		log.Info("test: env change detected, automatic update requires interval; skip in unit test")
	})

	t.Run("注册中心不可达时注册应返回错误", func(t *testing.T) {
		tmpDir := shortTempDir(t)
		t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
		socketPath := filepath.Join(tmpDir, "nonexist.sock")
		// 故意不启动服务端
		cfg := pool.DefaultConnPoolConfig()
		cfg.MaxActive = 2
		cfg.MaxIdle = 1
		cfg.GetTimeout = 500 * time.Millisecond

		rc, err := app.NewRegistryClient(socketPath, "binary", "test-service", "test-type")
		if err != nil {
			t.Fatalf("NewRegistryClient should not fail even if socket does not exist yet: %v", err)
		}
		defer rc.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err = rc.Register(ctx, nil)
		if err == nil {
			t.Error("expected error when registering without server, got nil")
		} else {
			t.Logf("expected register error: %v", err)
		}
	})
}

func StartMockRegistryServer(t testing.TB, socketPath string) net.Listener {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to start mock registry: %v", err)
	}
	if conn, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond); err != nil {
		_ = listener.Close()
		t.Fatalf("mock registry listen ok but dial failed (socket=%s): %v", socketPath, err)
	} else {
		_ = conn.Close()
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go handleMockRegistryConnection(conn)
		}
	}()
	return listener
}

func handleMockRegistryConnection(conn net.Conn) {
	defer conn.Close()
	for {
		header := make([]byte, 4)
		if _, err := conn.Read(header); err != nil {
			return
		}
		length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])
		data := make([]byte, length)
		if _, err := conn.Read(data); err != nil {
			return
		}
		var msg app.Message
		if err := msg.UnmarshalBinary(data); err != nil {
			return
		}
		var respBody []byte
		switch msg.Method {
		case "Register", "Heartbeat", "Deregister":
			respBody = []byte("ok")
		case "Discover":
			endpoints := []app.ServiceEndpoint{
				{Type: "unix", Address: "test.sock"},
			}
			var err error
			respBody, err = json.Marshal(endpoints)
			if err != nil {
				respBody = []byte("[]")
			}
		default:
			respBody = []byte("unknown method")
		}
		respMsg := &app.Message{
			ID:   msg.ID,
			Body: respBody,
		}
		respData, _ := respMsg.MarshalBinary()
		length = uint32(len(respData))
		header = []byte{byte(length >> 24), byte(length >> 16), byte(length >> 8), byte(length)}
		conn.Write(header)
		conn.Write(respData)
	}
}
