package codec

import (
	"encoding"
	"errors"
)

// binaryCodec 是二进制编解码器的具体实现。
type binaryCodec struct{}

var _ Codec = (*binaryCodec)(nil)

// Binary 是二进制编解码器的公共实例，可直接使用。
//
// 【注意】使用 Binary 编解码器时，v 必须实现 encoding.BinaryMarshaler / BinaryUnmarshaler 接口。
// 该实现不保证并发安全，调用方需自行确保同一对象不会在多个 goroutine 中同时编码/解码。
var Binary Codec = &binaryCodec{}

func init() {
	Register(Binary)
}

// Type 返回类型标识 "binary"。
func (c *binaryCodec) Type() string {
	return "binary"
}

// Encode 要求 v 实现 encoding.BinaryMarshaler，调用其 MarshalBinary 方法。
// 若 v 未实现该接口，返回 ErrNotBinaryMarshaler。
func (c *binaryCodec) Encode(v interface{}) ([]byte, error) {
	m, ok := v.(encoding.BinaryMarshaler)
	if !ok {
		return nil, ErrNotBinaryMarshaler
	}
	return m.MarshalBinary()
}

// Decode 要求 v 实现 encoding.BinaryUnmarshaler，调用其 UnmarshalBinary 方法。
// v 必须是指向实现了该接口的类型的指针。
func (c *binaryCodec) Decode(data []byte, v interface{}) error {
	u, ok := v.(encoding.BinaryUnmarshaler)
	if !ok {
		return ErrNotBinaryUnmarshaler
	}
	return u.UnmarshalBinary(data)
}

// EncodeBatch 顺序编码（因 BinaryMarshaler 接口不保证并发安全）。
func (c *binaryCodec) EncodeBatch(v []interface{}) ([][]byte, error) {
	results := make([][]byte, len(v))
	for i, item := range v {
		m, ok := item.(encoding.BinaryMarshaler)
		if !ok {
			return nil, ErrNotBinaryMarshaler
		}
		data, err := m.MarshalBinary()
		if err != nil {
			return nil, err
		}
		results[i] = data
	}
	return results, nil
}

// 预定义错误，供调用方区分处理。
var (
	ErrNotBinaryMarshaler   = errors.New("binary: value does not implement encoding.BinaryMarshaler")
	ErrNotBinaryUnmarshaler = errors.New("binary: value does not implement encoding.BinaryUnmarshaler")
)
