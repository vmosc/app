// Package codec 提供统一的编解码器接口、注册管理及辅助函数。
package codec

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
)

// Codec 是编解码器的核心接口。
type Codec interface {
	Type() string
	Encode(v interface{}) ([]byte, error)
	Decode(data []byte, v interface{}) error
	EncodeBatch(v []interface{}) ([][]byte, error)
}

var (
	registry = make(map[string]Codec)
	mu       sync.RWMutex
)

// Register 注册编解码器，通常在包的 init() 中调用。
func Register(c Codec) {
	mu.Lock()
	defer mu.Unlock()
	registry[c.Type()] = c
}

// Get 根据类型标识获取编解码器。
func Get(typ string) (Codec, error) {
	mu.RLock()
	c, ok := registry[typ]
	mu.RUnlock()
	if !ok {
		return nil, ErrCodecNotFound
	}
	return c, nil
}

// MustGet 获取编解码器，不存在时 panic。
func MustGet(typ string) Codec {
	c, err := Get(typ)
	if err != nil {
		panic(err)
	}
	return c
}

var ErrCodecNotFound = errors.New("codec: not found")

// ParseParams 解析参数数据为 map[string]interface{}。
// 支持 JSON 和简单的 key=value 格式（逗号分隔）
func ParseParams(data []byte) (map[string]interface{}, error) {
	if len(data) == 0 {
		return make(map[string]interface{}), nil
	}

	// 先尝试 JSON 解析
	var params map[string]interface{}
	if err := json.Unmarshal(data, &params); err == nil {
		return params, nil
	}

	// 如果不是 JSON，尝试解析为 key=value 格式
	str := string(data)
	if len(str) > 0 {
		result := make(map[string]interface{})
		parts := strings.Split(str, ",")
		for _, part := range parts {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				result[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
		if len(result) > 0 {
			return result, nil
		}
	}

	return make(map[string]interface{}), nil
}

// ReleaseParams 预留用于释放参数 map（便于对象池扩展）。
func ReleaseParams(_ map[string]interface{}) {}
