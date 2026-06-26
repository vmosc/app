// app/test/unix_test.go
package test

import (
	"app/kernel/unix"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUnixClientServer(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "test.sock")

	server := unix.NewUnixServer(socketPath)
	testHandler := func(ctx context.Context, req []byte) ([]byte, error) {
		return append([]byte("echo: "), req...), nil
	}
	err := server.Serve(testHandler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	testData := []byte("hello unix")
	ctx := context.Background()
	resp, err := client.Send(ctx, testData)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	expected := []byte("echo: hello unix")
	if !bytes.Equal(resp, expected) {
		t.Errorf("Response mismatch: got %q, want %q", resp, expected)
	}
}

func TestUnixClientTimeout(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "timeout.sock")

	server := unix.NewUnixServer(socketPath)
	slowHandler := func(ctx context.Context, req []byte) ([]byte, error) {
		time.Sleep(2 * time.Second)
		return req, nil
	}
	err := server.Serve(slowHandler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Send(ctx, []byte("test"))
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	stats := client.Stats()
	if stats.TimeoutRequests != 1 {
		t.Errorf("Expected 1 timeout, got %d", stats.TimeoutRequests)
	}
	if stats.FailedRequests != 1 {
		t.Errorf("Expected 1 failed, got %d", stats.FailedRequests)
	}
}

func TestUnixClientContextCancel(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "cancel.sock")

	server := unix.NewUnixServer(socketPath)
	slowHandler := func(ctx context.Context, req []byte) ([]byte, error) {
		time.Sleep(2 * time.Second)
		return req, nil
	}
	err := server.Serve(slowHandler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.Send(ctx, []byte("test"))
	if err == nil {
		t.Error("Expected context canceled error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}

	stats := client.Stats()
	if stats.FailedRequests != 1 {
		t.Errorf("Expected 1 failed request, got %d", stats.FailedRequests)
	}
	if stats.TimeoutRequests != 1 {
		t.Errorf("Expected 1 timeout/cancel, got %d", stats.TimeoutRequests)
	}
}

func TestUnixLargeMessage(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "large.sock")

	server := unix.NewUnixServer(socketPath)
	echoHandler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(echoHandler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	largeData := make([]byte, 5*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	resp, err := client.Send(context.Background(), largeData)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if len(resp) != len(largeData) {
		t.Errorf("Response size mismatch: got %d, want %d", len(resp), len(largeData))
	}
	if !bytes.Equal(resp, largeData) {
		t.Error("Response data mismatch")
	}
}

func TestUnixMessageTooLarge(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "toolarge.sock")

	server := unix.NewUnixServer(socketPath)
	echoHandler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(echoHandler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(10*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	largeData := make([]byte, 20*1024*1024)
	_, err = client.Send(context.Background(), largeData)
	if err == nil {
		t.Error("Expected error for too large message, got nil")
	}
}

func TestUnixServerStop(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "stop.sock")

	server := unix.NewUnixServer(socketPath)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		time.Sleep(500 * time.Millisecond)
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	go func() {
		client.Send(context.Background(), []byte("test"))
	}()

	time.Sleep(100 * time.Millisecond)

	err = server.Stop()
	if err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	if _, err := os.Stat(socketPath); err == nil {
		t.Error("Socket file still exists after server stop")
	}

	_, err = unix.NewUnixClient(socketPath)
	if err == nil {
		t.Error("Expected error when connecting to stopped server, got nil")
	}
}

func TestUnixClientStats(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "stats.sock")

	server := unix.NewUnixServer(socketPath)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		if bytes.Equal(req, []byte("error")) {
			return nil, errors.New("simulated error")
		}
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Send(context.Background(), []byte("ok"))
	if err != nil {
		t.Errorf("Success request failed: %v", err)
	}
	_, err = client.Send(context.Background(), []byte("error"))
	if err == nil {
		t.Error("Error request should fail")
	}

	time.Sleep(10 * time.Millisecond)

	stats := client.Stats()
	if stats.TotalRequests != 2 {
		t.Errorf("TotalRequests = %d, want 2", stats.TotalRequests)
	}
	if stats.SuccessRequests != 1 {
		t.Errorf("SuccessRequests = %d, want 1", stats.SuccessRequests)
	}
	if stats.FailedRequests != 1 {
		t.Errorf("FailedRequests = %d, want 1", stats.FailedRequests)
	}
	if stats.AvgLatency == 0 {
		t.Error("AvgLatency should not be 0")
	}
	if stats.LastActive == 0 {
		t.Error("LastActive should not be 0")
	}
}

func TestUnixServerStats(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "serverstats.sock")

	server := unix.NewUnixServer(socketPath)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		time.Sleep(50 * time.Millisecond)
		if bytes.Equal(req, []byte("error")) {
			return nil, errors.New("handler error")
		}
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	client.Send(context.Background(), []byte("ok1"))
	client.Send(context.Background(), []byte("ok2"))
	client.Send(context.Background(), []byte("error"))

	time.Sleep(100 * time.Millisecond)

	stats := server.Stats()
	if stats.TotalRequests != 3 {
		t.Errorf("TotalRequests = %d, want 3", stats.TotalRequests)
	}
	if stats.SuccessRequests != 2 {
		t.Errorf("SuccessRequests = %d, want 2", stats.SuccessRequests)
	}
	if stats.FailedRequests != 1 {
		t.Errorf("FailedRequests = %d, want 1", stats.FailedRequests)
	}
	if stats.AvgProcessTime == 0 {
		t.Error("AvgProcessTime should not be 0")
	}
	if stats.StartTime == 0 {
		t.Error("StartTime should not be 0")
	}
	if stats.ActiveConnections < 0 {
		t.Error("ActiveConnections should not be negative")
	}
}

func TestUnixConcurrentClients(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "concurrent.sock")

	server := unix.NewUnixServer(socketPath)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	const clientCount = 5
	const requestsPerClient = 10
	errChan := make(chan error, clientCount*requestsPerClient)

	for i := 0; i < clientCount; i++ {
		go func(clientID int) {
			client, err := unix.NewUnixClient(socketPath, unix.WithTimeout(5*time.Second))
			if err != nil {
				errChan <- err
				return
			}
			defer client.Close()

			for j := 0; j < requestsPerClient; j++ {
				data := []byte{byte(clientID), byte(j)}
				_, err := client.Send(context.Background(), data)
				errChan <- err
			}
		}(i)
	}

	for i := 0; i < clientCount*requestsPerClient; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Request failed: %v", err)
		}
	}
}

func TestUnixClientClose(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "clientclose.sock")

	server := unix.NewUnixServer(socketPath)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	_, err = client.Send(context.Background(), []byte("test"))
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if !client.IsClosed() {
		t.Error("Client should be marked as closed")
	}

	_, err = client.Send(context.Background(), []byte("test"))
	if err == nil {
		t.Error("Send after close should fail")
	}
}

func TestUnixErrorHandler(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "errorhandler.sock")

	errorChan := make(chan error, 10)
	errorHandler := func(err error) {
		errorChan <- err
	}

	server := unix.NewUnixServer(socketPath,
		unix.WithErrorHandler(errorHandler),
		unix.WithReturnDetailedErrors(true),
	)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		if bytes.Equal(req, []byte("error")) {
			return nil, errors.New("custom handler error")
		}
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Send(context.Background(), []byte("ok"))
	if err != nil {
		t.Errorf("Normal request failed: %v", err)
	}

	_, err = client.Send(context.Background(), []byte("error"))
	if err == nil {
		t.Error("Expected error request to fail")
	}

	select {
	case e := <-errorChan:
		if e.Error() != "custom handler error" {
			t.Errorf("Expected error 'custom handler error', got '%v'", e)
		}
	case <-time.After(1 * time.Second):
		t.Error("Error handler not called")
	}
}

func TestUnixCustomMaxMessageSize(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "maxsize.sock")

	server := unix.NewUnixServer(socketPath,
		unix.WithServerMaxMessageSize(1*1024*1024),
	)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	client, err := unix.NewUnixClient(socketPath,
		unix.WithClientMaxMessageSize(1*1024*1024),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	smallData := make([]byte, 512*1024)
	_, err = client.Send(context.Background(), smallData)
	if err != nil {
		t.Errorf("Small message failed: %v", err)
	}

	largeData := make([]byte, 2*1024*1024)
	_, err = client.Send(context.Background(), largeData)
	if err == nil {
		t.Error("Expected error for too large message, got nil")
	}
}

func TestUnixCustomReadTimeout(t *testing.T) {
	socketPath := filepath.Join(os.TempDir(), "readtimeout.sock")

	errorChan := make(chan error, 1)

	server := unix.NewUnixServer(socketPath,
		unix.WithReadTimeout(500*time.Millisecond),
		unix.WithErrorHandler(func(err error) {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				errorChan <- err
			}
		}),
	)
	handler := func(ctx context.Context, req []byte) ([]byte, error) {
		return req, nil
	}
	err := server.Serve(handler)
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	reqData := []byte("hello")
	length := uint32(len(reqData))
	lengthBytes := []byte{
		byte(length >> 24),
		byte(length >> 16),
		byte(length >> 8),
		byte(length),
	}
	if _, err := conn.Write(lengthBytes); err != nil {
		t.Fatalf("Write length failed: %v", err)
	}
	if _, err := conn.Write(reqData); err != nil {
		t.Fatalf("Write data failed: %v", err)
	}

	respLenBytes := make([]byte, 4)
	if _, err := io.ReadFull(conn, respLenBytes); err != nil {
		t.Fatalf("Read response length failed: %v", err)
	}
	respLen := uint32(respLenBytes[0])<<24 | uint32(respLenBytes[1])<<16 |
		uint32(respLenBytes[2])<<8 | uint32(respLenBytes[3])
	respData := make([]byte, respLen)
	if _, err := io.ReadFull(conn, respData); err != nil {
		t.Fatalf("Read response data failed: %v", err)
	}
	if !bytes.Equal(respData, reqData) {
		t.Errorf("Response mismatch: got %q, want %q", respData, reqData)
	}

	time.Sleep(600 * time.Millisecond)

	_, err = conn.Read(make([]byte, 1))
	if err == nil {
		t.Error("Expected read error after read timeout, got nil")
	}

	select {
	case <-errorChan:
	case <-time.After(1 * time.Second):
		t.Error("Read timeout error not captured")
	}
}
