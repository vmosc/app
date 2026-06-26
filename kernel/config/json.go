package config

import (
	"encoding/json"
)

// JSONManager JSON 格式的配置管理器。
type JSONManager struct {
	BaseManager
}

// marshalIndent 包装 json.MarshalIndent 使其符合 BaseManager.marshal 签名。
func marshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// NewJSON 创建一个新的 JSON 配置管理器，默认路径为 "./config.json"。
func NewJSON() *JSONManager {
	m := &JSONManager{}
	m.BaseManager = BaseManager{
		path:      "./config.json",
		marshal:   marshalIndent,
		unmarshal: json.Unmarshal,
		getExtensions: func() []string {
			return []string{".json"}
		},
	}
	return m
}
