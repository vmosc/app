package test

import (
	"context"
	"testing"
	"time"

	"github.com/vmosc/app/api"
	"github.com/vmosc/app/kernel/log"
)

type TestService struct{}

func (s TestService) Hello(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{
		ID:     req.ID,
		Method: "Hello.response",
		Body:   []byte("Hello " + string(req.Body)),
	}, nil
}

func (s TestService) Ping(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{
		ID:     req.ID,
		Method: "Ping.response",
		Body:   []byte("pong"),
	}, nil
}

func (s TestService) Echo(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{
		ID:     req.ID,
		Method: "Echo.response",
		Body:   req.Body,
	}, nil
}

type SimpleService struct{}

func (s SimpleService) Status(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{
		ID:   req.ID,
		Body: []byte("ok"),
	}, nil
}

type ServiceA struct{}

func (s ServiceA) Action(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{Body: []byte("from A")}, nil
}

type ServiceB struct{}

func (s ServiceB) Action(ctx context.Context, req *api.Message) (*api.Message, error) {
	return &api.Message{Body: []byte("from B")}, nil
}

func TestServer(t *testing.T) {
	t.Run("函数注册测试", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterFunc(func(ctx context.Context, req *api.Message) (*api.Message, error) {
			return &api.Message{ID: req.ID, Body: []byte("pong")}, nil
		}, "test.ping")

		time.Sleep(100 * time.Millisecond)

		methods := api.ListMethods()
		found := false
		for _, m := range methods {
			if m == "test.ping" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("method test.ping not registered, methods: %v", methods)
		}

		client := app.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		req := &api.Message{
			ID:     "ping-req",
			Method: "test.ping",
			Body:   []byte("ping"),
		}

		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("call test.ping failed: %v", err)
		}
		if string(resp.Body) != "pong" {
			t.Errorf("unexpected response: got %s, want pong", string(resp.Body))
		}
	})

	t.Run("结构体方法注册测试-带前缀", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterStruct(&TestService{}, "test")

		time.Sleep(100 * time.Millisecond)

		methods := api.ListMethods()
		expectedMethods := []string{"test.Hello", "test.Ping", "test.Echo"}
		for _, expected := range expectedMethods {
			found := false
			for _, m := range methods {
				if m == expected {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("method %s not registered, methods: %v", expected, methods)
			}
		}

		client := app.Client()

		t.Run("调用 Hello 方法", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := &api.Message{
				ID:     "hello-req-1",
				Method: "test.Hello",
				Body:   []byte("World"),
			}
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatalf("call Hello failed: %v", err)
			}
			if string(resp.Body) != "Hello World" {
				t.Errorf("unexpected response: got %s, want Hello World", string(resp.Body))
			}
			if resp.Method != "Hello.response" {
				t.Errorf("unexpected method: got %s, want Hello.response", resp.Method)
			}
		})

		t.Run("调用 Ping 方法", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := &api.Message{
				ID:     "ping-req-1",
				Method: "test.Ping",
				Body:   []byte("anything"),
			}
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatalf("call Ping failed: %v", err)
			}
			if string(resp.Body) != "pong" {
				t.Errorf("unexpected response: got %s, want pong", string(resp.Body))
			}
		})

		t.Run("调用 Echo 方法", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			testData := []byte("echo test data")
			req := &api.Message{
				ID:     "echo-req-1",
				Method: "test.Echo",
				Body:   testData,
			}
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatalf("call Echo failed: %v", err)
			}
			if string(resp.Body) != string(testData) {
				t.Errorf("unexpected response: got %s, want %s", string(resp.Body), string(testData))
			}
		})
	})

	t.Run("结构体方法注册测试-无前缀", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterStruct(&SimpleService{}, "")

		time.Sleep(100 * time.Millisecond)

		methods := api.ListMethods()
		found := false
		for _, m := range methods {
			if m == "Status" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("method Status not registered without prefix, methods: %v", methods)
		}

		client := app.Client()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req := &api.Message{
			ID:     "status-req",
			Method: "Status",
			Body:   []byte("check"),
		}
		resp, err := client.Send(ctx, req)
		if err != nil {
			t.Fatalf("call Status failed: %v", err)
		}
		if string(resp.Body) != "ok" {
			t.Errorf("unexpected response: got %s, want ok", string(resp.Body))
		}
	})

	t.Run("结构体方法注册测试-重复方法名", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterStruct(&ServiceA{}, "a")
		api.RegisterStruct(&ServiceB{}, "b")

		time.Sleep(100 * time.Millisecond)

		client := app.Client()

		t.Run("调用 a.Action", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := &api.Message{
				ID:     "a-req",
				Method: "a.Action",
				Body:   []byte("call"),
			}
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatalf("call a.Action failed: %v", err)
			}
			if string(resp.Body) != "from A" {
				t.Errorf("unexpected response: got %s, want from A", string(resp.Body))
			}
		})

		t.Run("调用 b.Action", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := &api.Message{
				ID:     "b-req",
				Method: "b.Action",
				Body:   []byte("call"),
			}
			resp, err := client.Send(ctx, req)
			if err != nil {
				t.Fatalf("call b.Action failed: %v", err)
			}
			if string(resp.Body) != "from B" {
				t.Errorf("unexpected response: got %s, want from B", string(resp.Body))
			}
		})
	})

	t.Run("服务端 Panic 恢复测试", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		api.RegisterFunc(func(ctx context.Context, req *api.Message) (*api.Message, error) {
			panic("test panic")
		}, "test.panic")

		time.Sleep(100 * time.Millisecond)

		metrics := app.GetMetrics()
		if metrics == nil {
			t.Error("server metrics is nil after panic")
		}
		log.Info("test: panic recovery test passed")
	})
}
