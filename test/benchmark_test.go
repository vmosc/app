package test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmosc/app"
	"github.com/vmosc/app/api"
	"github.com/vmosc/app/kernel/pool"
)

func BenchmarkMessageEncode(b *testing.B) {
	msg := &app.Message{
		ID:        "bench_test",
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Method:    "bench.method",
		Metadata:  map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		Body:      make([]byte, 1024),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := msg.MarshalBinary()
		if err != nil {
			b.Fatalf("marshal failed: %v", err)
		}
	}
}

func BenchmarkMessageDecode(b *testing.B) {
	msg := &app.Message{
		ID:        "bench_test",
		Version:   1,
		Timestamp: time.Now().UnixNano(),
		Method:    "bench.method",
		Metadata:  map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		Body:      make([]byte, 1024),
	}
	data, err := msg.MarshalBinary()
	if err != nil {
		b.Fatalf("marshal failed: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			b.Fatalf("unmarshal failed: %v", err)
		}
	}
}

func BenchmarkClientSend(b *testing.B) {
	tmpDir := b.TempDir()
	socketPath := tmpDir + "/bench.sock"
	server := StartBenchServer(b, socketPath)
	defer server.Close()

	cfg := pool.ConnPoolConfig{
		MaxIdle:     10,
		MaxActive:   50,
		GetTimeout:  5 * time.Second,
		MaxIdleTime: 30 * time.Second,
		MaxLifeTime: 60 * time.Second,
	}
	client, err := app.NewClient(socketPath, "binary", cfg)
	if err != nil {
		b.Fatalf("create client failed: %v", err)
	}
	defer client.Close()

	msg := &app.Message{
		ID:     "bench",
		Method: "bench.echo",
		Body:   []byte("benchmark data"),
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := client.Send(ctx, msg)
			cancel()
			if err != nil {
				b.Errorf("send failed: %v", err)
			}
		}
	})
}

func BenchmarkConcurrentRequests(b *testing.B) {
	tmpDir := b.TempDir()
	origDir, _ := os.Getwd()
	defer os.Chdir(origDir)
	os.Chdir(tmpDir)

	CreateTestBusinessConfig(b, make(map[string]any))

	api.RegisterFunc(func(ctx context.Context, req *api.Message) (*api.Message, error) {
		return &api.Message{ID: req.ID}, nil
	}, "test.fast")

	appInst := api.NewApp()
	if err := appInst.Init("bench"); err != nil {
		b.Fatalf("init failed: %v", err)
	}
	defer appInst.Shutdown()

	time.Sleep(100 * time.Millisecond)

	socketPath := FindSocketFile(b)

	clientCfg := pool.ConnPoolConfig{
		MaxIdle:    20,
		MaxActive:  100,
		GetTimeout: 5 * time.Second,
	}
	client, err := app.NewClient(socketPath, "binary", clientCfg)
	if err != nil {
		b.Fatalf("create client failed: %v", err)
	}
	defer client.Close()

	msg := &api.Message{
		ID:     "bench",
		Method: "test.fast",
		Body:   []byte("bench"),
	}
	var successCount int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := client.Send(ctx, msg)
			cancel()
			if err == nil {
				atomic.AddInt64(&successCount, 1)
			}
		}
	})
	b.ReportMetric(float64(successCount), "success_count")
}

func BenchmarkMemoryAllocation(b *testing.B) {
	b.Run("消息创建", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = &app.Message{
				ID:        fmt.Sprintf("msg_%d", i),
				Version:   1,
				Timestamp: time.Now().UnixNano(),
				Method:    "test.method",
				Metadata:  map[string]string{"key": "value"},
				Body:      make([]byte, 1024),
			}
		}
	})
	b.Run("消息编解码", func(b *testing.B) {
		msg := &app.Message{
			ID:       "test",
			Method:   "test.method",
			Metadata: map[string]string{"key": "value"},
			Body:     make([]byte, 1024),
		}
		for i := 0; i < b.N; i++ {
			data, _ := msg.MarshalBinary()
			var decoded app.Message
			_ = decoded.UnmarshalBinary(data)
		}
	})
}

// 新增基准测试：对象池不同大小对比
func BenchmarkObjectPool_DifferentSizes(b *testing.B) {
	sizes := []struct {
		min int
		max int
	}{
		{1, 10},
		{10, 100},
		{100, 1000},
	}
	for _, s := range sizes {
		b.Run(fmt.Sprintf("min%d-max%d", s.min, s.max), func(b *testing.B) {
			create := func() (interface{}, error) { return make([]byte, 1024), nil }
			validate := func(obj interface{}) bool { return true }
			destroy := func(obj interface{}) {}
			cfg := pool.DefaultConfig()
			cfg.MinSize = s.min
			cfg.MaxSize = s.max
			p := pool.NewLockFreeObjectPool(create, validate, destroy, cfg)
			defer p.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					obj, _ := p.Get()
					_ = p.Put(obj)
				}
			})
		})
	}
}

// 基准测试：直接分配 vs 对象池
func BenchmarkDirectAllocation(b *testing.B) {
	b.Run("直接分配", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 1024)
		}
	})
	b.Run("对象池", func(b *testing.B) {
		create := func() (interface{}, error) { return make([]byte, 1024), nil }
		validate := func(obj interface{}) bool { return true }
		destroy := func(obj interface{}) {}
		cfg := pool.DefaultConfig()
		cfg.MinSize = 10
		cfg.MaxSize = 100
		p := pool.NewLockFreeObjectPool(create, validate, destroy, cfg)
		defer p.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				obj, _ := p.Get()
				_ = p.Put(obj)
			}
		})
	})
}

// 基准测试：不同工作池并发度
func BenchmarkWorkerPool_DifferentConcurrency(b *testing.B) {
	for _, workers := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			cfg := pool.DefaultConfig()
			cfg.MinSize = workers
			cfg.MaxSize = workers * 2
			wp := pool.NewWorkerPoolWithConfig("bench", workers, b.N, cfg)
			defer wp.Close()

			task := pool.NewFuncTask("bench", func(ctx context.Context) error { return nil })

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = wp.Submit(task)
				}
			})
		})
	}
}
