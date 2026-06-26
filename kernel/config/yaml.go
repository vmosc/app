package config

import (
	"gopkg.in/yaml.v3"
)

// YAMLManager YAML 格式的配置管理器。
type YAMLManager struct {
	BaseManager
}

// NewYAML 创建一个新的 YAML 配置管理器，默认路径为 "./config.yaml"。
func NewYAML() *YAMLManager {
	m := &YAMLManager{}
	m.BaseManager = BaseManager{
		path:      "./config.yaml",
		marshal:   yaml.Marshal,
		unmarshal: yaml.Unmarshal,
		getExtensions: func() []string {
			return []string{".yaml", ".yml"}
		},
	}
	return m
}
