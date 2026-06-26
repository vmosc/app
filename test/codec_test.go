// app/test/codec_test.go
package test

import (
	"app/kernel/codec"
	"bytes"
	"encoding"
	"errors"
	"testing"
)

type testMessage struct {
	ID   string
	Body []byte
}

func (m *testMessage) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if len(m.ID) > 65535 {
		return nil, errors.New("ID too long")
	}
	buf.WriteByte(byte(len(m.ID) >> 8))
	buf.WriteByte(byte(len(m.ID)))
	buf.WriteString(m.ID)
	if len(m.Body) > 1<<32-1 {
		return nil, errors.New("body too long")
	}
	buf.WriteByte(byte(len(m.Body) >> 24))
	buf.WriteByte(byte(len(m.Body) >> 16))
	buf.WriteByte(byte(len(m.Body) >> 8))
	buf.WriteByte(byte(len(m.Body)))
	buf.Write(m.Body)
	return buf.Bytes(), nil
}

func (m *testMessage) UnmarshalBinary(data []byte) error {
	if len(data) < 2 {
		return errors.New("data too short")
	}
	idLen := int(data[0])<<8 | int(data[1])
	if len(data) < 2+idLen+4 {
		return errors.New("data too short")
	}
	m.ID = string(data[2 : 2+idLen])
	pos := 2 + idLen
	bodyLen := int(data[pos])<<24 | int(data[pos+1])<<16 | int(data[pos+2])<<8 | int(data[pos+3])
	pos += 4
	if len(data) < pos+bodyLen {
		return errors.New("data too short")
	}
	m.Body = make([]byte, bodyLen)
	copy(m.Body, data[pos:pos+bodyLen])
	return nil
}

var _ encoding.BinaryMarshaler = (*testMessage)(nil)
var _ encoding.BinaryUnmarshaler = (*testMessage)(nil)

func TestJSONEncodeDecode(t *testing.T) {
	type user struct {
		Name string
		Age  int
	}
	u := user{"Alice", 30}

	data, err := codec.JSON.Encode(u)
	if err != nil {
		t.Fatal(err)
	}

	var u2 user
	err = codec.JSON.Decode(data, &u2)
	if err != nil {
		t.Fatal(err)
	}
	if u2 != u {
		t.Errorf("expected %+v, got %+v", u, u2)
	}
}

func TestJSONEncodeBatch(t *testing.T) {
	inputs := []interface{}{
		map[string]int{"a": 1},
		map[string]int{"b": 2},
	}
	results, err := codec.JSON.EncodeBatch(inputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, data := range results {
		var m map[string]int
		if err := codec.JSON.Decode(data, &m); err != nil {
			t.Errorf("batch[%d] decode failed: %v", i, err)
		}
	}
}

func TestJSONEncodeBatchError(t *testing.T) {
	ch := make(chan int)
	inputs := []interface{}{
		map[string]int{"ok": 1},
		ch,
		map[string]int{"also": 2},
	}
	_, err := codec.JSON.EncodeBatch(inputs)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBinaryEncodeDecode(t *testing.T) {
	msg := &testMessage{ID: "123", Body: []byte("hello")}

	data, err := codec.Binary.Encode(msg)
	if err != nil {
		t.Fatal(err)
	}

	var msg2 testMessage
	err = codec.Binary.Decode(data, &msg2)
	if err != nil {
		t.Fatal(err)
	}
	if msg2.ID != msg.ID || !bytes.Equal(msg2.Body, msg.Body) {
		t.Errorf("expected %+v, got %+v", msg, &msg2)
	}
}

func TestBinaryEncodeError(t *testing.T) {
	_, err := codec.Binary.Encode(struct{}{})
	if err != codec.ErrNotBinaryMarshaler {
		t.Errorf("expected ErrNotBinaryMarshaler, got %v", err)
	}
}

func TestBinaryDecodeError(t *testing.T) {
	var dummy int
	err := codec.Binary.Decode([]byte{}, &dummy)
	if err != codec.ErrNotBinaryUnmarshaler {
		t.Errorf("expected ErrNotBinaryUnmarshaler, got %v", err)
	}
}

func TestBinaryEncodeBatch(t *testing.T) {
	msg1 := &testMessage{ID: "1", Body: []byte("a")}
	msg2 := &testMessage{ID: "2", Body: []byte("b")}
	inputs := []interface{}{msg1, msg2}

	results, err := codec.Binary.EncodeBatch(inputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	var m1, m2 testMessage
	if err := codec.Binary.Decode(results[0], &m1); err != nil {
		t.Error(err)
	}
	if err := codec.Binary.Decode(results[1], &m2); err != nil {
		t.Error(err)
	}
	if m1.ID != msg1.ID || !bytes.Equal(m1.Body, msg1.Body) {
		t.Errorf("expected %+v, got %+v", msg1, m1)
	}
	if m2.ID != msg2.ID || !bytes.Equal(m2.Body, msg2.Body) {
		t.Errorf("expected %+v, got %+v", msg2, m2)
	}
}

func TestCodecGetRegistered(t *testing.T) {
	c, err := codec.Get("json")
	if err != nil {
		t.Fatal(err)
	}
	if c.Type() != "json" {
		t.Errorf("expected json, got %s", c.Type())
	}

	c, err = codec.Get("binary")
	if err != nil {
		t.Fatal(err)
	}
	if c.Type() != "binary" {
		t.Errorf("expected binary, got %s", c.Type())
	}
}

func TestCodecGetNotFound(t *testing.T) {
	_, err := codec.Get("nonexistent")
	if err != codec.ErrCodecNotFound {
		t.Errorf("expected ErrCodecNotFound, got %v", err)
	}
}

func TestCodecMustGet(t *testing.T) {
	c := codec.MustGet("json")
	if c.Type() != "json" {
		t.Errorf("expected json, got %s", c.Type())
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nonexistent codec")
		}
	}()
	codec.MustGet("nonexistent")
}

func TestCodecParseParams(t *testing.T) {
	data := []byte(`{"foo":"bar","num":42}`)
	params, err := codec.ParseParams(data)
	if err != nil {
		t.Fatal(err)
	}
	if params["foo"] != "bar" || params["num"] != float64(42) {
		t.Errorf("unexpected params: %+v", params)
	}

	emptyParams, err := codec.ParseParams(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyParams) != 0 {
		t.Errorf("expected empty map, got %+v", emptyParams)
	}

	// 非 JSON 字符串现在会被尝试解析为 key=value 格式，对于无效输入返回空 map 而不报错
	params, err = codec.ParseParams([]byte(`invalid`))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(params) != 0 {
		t.Errorf("expected empty map for invalid input, got %+v", params)
	}
}

func TestCodecReleaseParams(t *testing.T) {
	codec.ReleaseParams(map[string]interface{}{})
}
