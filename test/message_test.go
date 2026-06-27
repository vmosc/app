package test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vmosc/app"
	"github.com/vmosc/app/kernel/log"
)

func TestMessage(t *testing.T) {
	t.Run("二进制编解码-正常流程", func(t *testing.T) {
		testID := "binary_encode_decode"
		original := &app.Message{
			ID:        testID,
			Version:   1,
			Timestamp: time.Now().UnixNano(),
			Method:    "test.method",
			Metadata:  map[string]string{"trace_id": "123456", "user": "testuser"},
			Body:      []byte("hello world"),
		}
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if decoded.ID != original.ID {
			t.Errorf("ID mismatch: got %s, want %s", decoded.ID, original.ID)
		}
		if decoded.Version != original.Version {
			t.Errorf("Version mismatch: got %d, want %d", decoded.Version, original.Version)
		}
		if decoded.Method != original.Method {
			t.Errorf("Method mismatch: got %s, want %s", decoded.Method, original.Method)
		}
		if string(decoded.Body) != string(original.Body) {
			t.Errorf("Body mismatch: got %s, want %s", decoded.Body, original.Body)
		}
		if !reflect.DeepEqual(decoded.Metadata, original.Metadata) {
			t.Errorf("Metadata mismatch: got %v, want %v", decoded.Metadata, original.Metadata)
		}
		log.Info("test: binary encode/decode passed", "test_id", testID)
	})

	t.Run("空消息编解码", func(t *testing.T) {
		original := &app.Message{}
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal empty message failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal empty message failed: %v", err)
		}
		if decoded.ID != "" {
			t.Errorf("expected empty ID, got %s", decoded.ID)
		}
		if decoded.Version != 0 {
			t.Errorf("expected version 0, got %d", decoded.Version)
		}
		if decoded.Method != "" {
			t.Errorf("expected empty Method, got %s", decoded.Method)
		}
		if decoded.Metadata != nil && len(decoded.Metadata) > 0 {
			t.Errorf("expected nil/empty Metadata, got %v", decoded.Metadata)
		}
		if len(decoded.Body) != 0 {
			t.Errorf("expected empty Body, got %d bytes", len(decoded.Body))
		}
		log.Info("test: empty message encode/decode passed")
	})

	t.Run("大数据量编解码", func(t *testing.T) {
		largeBody := make([]byte, 1024*1024)
		for i := range largeBody {
			largeBody[i] = byte(i % 256)
		}
		original := &app.Message{
			ID:        "large_test",
			Version:   1,
			Timestamp: time.Now().UnixNano(),
			Method:    "test.large",
			Metadata:  map[string]string{"size": "1MB"},
			Body:      largeBody,
		}
		start := time.Now()
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal large message failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal large message failed: %v", err)
		}
		elapsed := time.Since(start)
		log.Info("test: large message encode/decode", "size", len(data), "elapsed_ms", elapsed.Milliseconds())
		if len(decoded.Body) != len(original.Body) {
			t.Errorf("Body size mismatch: got %d, want %d", len(decoded.Body), len(original.Body))
		}
	})

	t.Run("编解码缓存刷新测试", func(t *testing.T) {
		msg := &app.Message{ID: "cache_test", Method: "test.cache"}
		data1, err := msg.MarshalBinary()
		if err != nil {
			t.Fatalf("first marshal failed: %v", err)
		}
		app.RefreshMessageCodec()
		data2, err := msg.MarshalBinary()
		if err != nil {
			t.Fatalf("second marshal failed: %v", err)
		}
		if string(data1) != string(data2) {
			t.Error("cache refresh changed encoding result")
		}
		log.Info("test: codec cache refresh test passed")
	})

	t.Run("异常数据反序列化", func(t *testing.T) {
		invalidData := []byte{0x00, 0x00, 0x00, 0x05, 0x68, 0x65}
		var msg app.Message
		if err := msg.UnmarshalBinary(invalidData); err == nil {
			t.Error("expected error for truncated data, got nil")
		}
		if err := msg.UnmarshalBinary([]byte{}); err == nil {
			t.Error("expected error for empty data, got nil")
		}
		badHeader := []byte{0x00, 0x00, 0x10, 0x00, 0x01, 0x02}
		if err := msg.UnmarshalBinary(badHeader); err == nil {
			t.Error("expected error for length mismatch, got nil")
		}
		log.Info("test: invalid data unmarshal test passed")
	})

	t.Run("大量Metadata编解码", func(t *testing.T) {
		metadata := make(map[string]string, 200)
		for i := 0; i < 200; i++ {
			metadata[fmt.Sprintf("key-%03d", i)] = fmt.Sprintf("value-%03d", i)
		}
		original := &app.Message{
			ID:       "meta-test",
			Method:   "test.meta",
			Metadata: metadata,
		}
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal with 200 metadata entries failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if len(decoded.Metadata) != 200 {
			t.Errorf("expected 200 metadata entries, got %d", len(decoded.Metadata))
		}
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key-%03d", i)
			expected := fmt.Sprintf("value-%03d", i)
			if decoded.Metadata[key] != expected {
				t.Errorf("metadata[%s] = %s, want %s", key, decoded.Metadata[key], expected)
			}
		}
		log.Info("test: large metadata encode/decode passed")
	})

	t.Run("特殊字符Metadata", func(t *testing.T) {
		metadata := map[string]string{
			"key with space": "value\nwith\nnewlines",
			"中文键":            "中文值",
			"emoji-key😀":     "value-with-emoji🎉",
			"":               "empty-key",
			"key-with-empty": "",
			"very-long-key-" + strings.Repeat("x", 500): "very-long-value-" + strings.Repeat("y", 500),
		}
		original := &app.Message{
			ID:       "special-meta",
			Method:   "test.meta",
			Metadata: metadata,
		}
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal special metadata failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal special metadata failed: %v", err)
		}
		if !reflect.DeepEqual(decoded.Metadata, metadata) {
			t.Errorf("metadata mismatch after special chars round-trip")
		}
		log.Info("test: special characters metadata encode/decode passed")
	})

	t.Run("nil Metadata 编解码", func(t *testing.T) {
		original := &app.Message{
			ID:       "nil-meta",
			Method:   "test.meta",
			Metadata: nil,
		}
		data, err := original.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal nil metadata failed: %v", err)
		}
		var decoded app.Message
		if err := decoded.UnmarshalBinary(data); err != nil {
			t.Fatalf("unmarshal nil metadata failed: %v", err)
		}
		// 注意：nil map 经过序列化/反序列化后可能变为空 map（而非 nil），
		// 这是编解码的正常行为，不要求严格为 nil，只需保证无元素。
		if len(decoded.Metadata) != 0 {
			t.Errorf("expected nil or empty Metadata after decode, got %v", decoded.Metadata)
		}
	})
}
