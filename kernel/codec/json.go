package codec

import (
	"encoding/json"
)

// jsonCodec 是 JSON 编解码器的具体实现。
type jsonCodec struct{}

var _ Codec = (*jsonCodec)(nil)

// JSON 是 JSON 编解码器的公共实例，可直接使用。
var JSON Codec = &jsonCodec{}

func init() {
	Register(JSON)
}

// Type 返回类型标识 "json"。
func (c *jsonCodec) Type() string {
	return "json"
}

// Encode 使用标准库 json.Marshal 编码。
func (c *jsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Decode 使用标准库 json.Unmarshal 解码。
func (c *jsonCodec) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// EncodeBatch 顺序编码 JSON 切片。
func (c *jsonCodec) EncodeBatch(v []interface{}) ([][]byte, error) {
	results := make([][]byte, len(v))
	for i, item := range v {
		data, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		results[i] = data
	}
	return results, nil
}
