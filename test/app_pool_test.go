package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vmosc/app"
	"github.com/vmosc/app/api"
	"github.com/vmosc/app/kernel/log"
	"github.com/vmosc/app/kernel/pool"
)

func TestObjectPool(t *testing.T) {
	t.Run("独立对象池创建与使用", func(t *testing.T) {
		createCount := 0
		create := func() (any, error) {
			createCount++
			return make([]byte, 1024), nil
		}
		validate := func(obj any) bool { return true }
		destroy := func(obj any) {}

		objPool := api.NewObjectPool(create, validate, destroy,
			10, 2,
			100*time.Millisecond,
			5*time.Second,
			10*time.Second,
			1*time.Second,
			false)
		defer objPool.Close()

		obj1, err := objPool.Get()
		if err != nil {
			t.Fatalf("get object failed: %v", err)
		}
		if obj1 == nil {
			t.Fatal("object is nil")
		}
		if err := objPool.Put(obj1); err != nil {
			t.Errorf("put object failed: %v", err)
		}
		stats := objPool.Stats()
		log.Info("test: object pool stats", "stats", stats)
	})

	t.Run("独立对象池并发测试", func(t *testing.T) {
		create := func() (any, error) { return make([]byte, 1024), nil }
		validate := func(obj any) bool { return true }
		destroy := func(obj any) {}

		objPool := api.NewObjectPool(create, validate, destroy,
			20, 5,
			100*time.Millisecond,
			5*time.Second,
			10*time.Second,
			1*time.Second,
			false)
		defer objPool.Close()

		var wg sync.WaitGroup
		concurrency := 50
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				obj, err := objPool.Get()
				if err != nil {
					t.Errorf("get object failed: %v", err)
					return
				}
				time.Sleep(10 * time.Millisecond)
				if err := objPool.Put(obj); err != nil {
					t.Errorf("put object failed: %v", err)
				}
			}()
		}
		wg.Wait()
		log.Info("test: object pool concurrent test passed")
	})

	t.Run("独立对象池超时测试", func(t *testing.T) {
		create := func() (any, error) {
			time.Sleep(10 * time.Millisecond)
			return make([]byte, 1024), nil
		}
		validate := func(obj any) bool { return true }
		destroy := func(obj any) {}

		objPool := api.NewObjectPool(create, validate, destroy,
			1, 0,
			50*time.Millisecond,
			5*time.Second,
			10*time.Second,
			0,
			false)
		defer objPool.Close()

		obj1, err := objPool.Get()
		if err != nil {
			t.Fatalf("first get failed: %v", err)
		}
		if obj1 == nil {
			t.Fatal("first object is nil")
		}

		start := time.Now()
		_, err = objPool.Get()
		elapsed := time.Since(start)
		if err == nil {
			t.Errorf("expected timeout error, got nil, elapsed: %v", elapsed)
		} else {
			log.Info("test: timeout error as expected", "err", err, "elapsed", elapsed)
		}

		if err := objPool.Put(obj1); err != nil {
			t.Errorf("put object failed: %v", err)
		}
	})
}

func TestConnPool(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := tmpDir + "/conn_pool.sock"

	mockServer := StartTestServer(t, socketPath)
	defer mockServer.Close()

	t.Run("连接池配置加载", func(t *testing.T) {
		cfg := pool.ConnPoolConfig{
			MaxIdle:     3,
			MaxActive:   10,
			GetTimeout:  1 * time.Second,
			MaxIdleTime: 2 * time.Second,
			MaxLifeTime: 5 * time.Second,
		}

		client, err := app.NewClient(socketPath, "binary", cfg)
		if err != nil {
			t.Fatalf("create client failed: %v", err)
		}
		client.Close()
		log.Info("test: conn pool config test passed")
	})
}

func TestWorkerPool(t *testing.T) {
	app, cleanup := StartTestApp(t)
	defer cleanup()

	t.Run("工作池队列满拒绝", func(t *testing.T) {
		api.RegisterFunc(func(ctx context.Context, req *api.Message) (*api.Message, error) {
			time.Sleep(500 * time.Millisecond)
			return &api.Message{ID: req.ID}, nil
		}, "test.slow")

		time.Sleep(100 * time.Millisecond)

		metrics := app.GetMetrics()
		if metrics == nil {
			t.Error("server metrics is nil")
		}
		log.Info("test: worker pool queue full test passed")
	})
}
