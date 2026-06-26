// Package config 提供统一的配置管理接口，支持多种格式（JSON、YAML 等）。
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Manager 配置管理器接口。
type Manager interface {
	Init(path string)
	Load(v any) error
	Reload(v any) error
	Save(v any) error
}

// BaseManager 提供通用的配置管理基础能力。
type BaseManager struct {
	path string
	mu   sync.RWMutex

	marshal   func(v any) ([]byte, error)
	unmarshal func(data []byte, v any) error

	getExtensions func() []string
}

// Init 设置配置文件或目录路径。
func (m *BaseManager) Init(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if path != "" {
		m.path = path
	}
}

// Load 加载配置，支持单文件或目录合并。
func (m *BaseManager) Load(v any) error {
	m.mu.RLock()
	path := m.path
	m.mu.RUnlock()

	exts := m.getExtensions()
	files, err := collectFiles(path, exts)
	if err != nil {
		if errors.Is(err, errNoMatchingFiles) {
			// 目录为空，返回空配置
			return nil
		}
		return err
	}

	var merged map[string]any
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read file %s: %w", file, err)
		}
		var parsed map[string]any
		if err := m.unmarshal(data, &parsed); err != nil {
			return fmt.Errorf("unmarshal %s: %w", file, err)
		}
		merged = mergeMaps(merged, parsed)
	}

	b, err := m.marshal(merged)
	if err != nil {
		return fmt.Errorf("remarshal: marshal intermediate: %w", err)
	}
	if err := m.unmarshal(b, v); err != nil {
		return fmt.Errorf("remarshal: unmarshal to target: %w", err)
	}
	return nil
}

// Reload 是 Load 的别名。
func (m *BaseManager) Reload(v any) error {
	return m.Load(v)
}

// Save 将配置原子写入文件。
// 支持目录模式：如果 path 是目录，自动选择第一个匹配的文件，若无则创建默认文件。
// 注意：Save 成功后，会将 m.path 更新为具体的文件路径，以便后续 Load 直接读取该文件。
func (m *BaseManager) Save(v any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := m.path

	// 检查路径是否存在
	fi, err := os.Stat(path)
	if err == nil && fi.IsDir() {
		// 目录模式：找第一个匹配的文件
		exts := m.getExtensions()
		entries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("read config dir: %w", err)
		}
		var targetFile string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			for _, ext := range exts {
				if strings.HasSuffix(strings.ToLower(name), ext) {
					targetFile = filepath.Join(path, name)
					break
				}
			}
			if targetFile != "" {
				break
			}
		}
		if targetFile == "" {
			// 没有匹配的文件，创建默认文件
			defaultExt := exts[0]
			targetFile = filepath.Join(path, "config"+defaultExt)
		}
		// 更新 m.path 为具体的文件路径
		m.path = targetFile
		path = targetFile
	}

	// 确保父目录存在
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := m.marshal(v)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	tmpFile := path + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := os.Rename(tmpFile, path); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}

var errNoMatchingFiles = errors.New("no matching files found")

// collectFiles 收集指定路径下所有扩展名匹配的文件。
func collectFiles(path string, exts []string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("access path %s: %w", path, err)
	}

	if !info.IsDir() {
		for _, ext := range exts {
			if strings.HasSuffix(strings.ToLower(path), ext) {
				return []string{path}, nil
			}
		}
		return nil, fmt.Errorf("file %s does not have a valid extension (%v)", path, exts)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", path, err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		for _, ext := range exts {
			if strings.HasSuffix(strings.ToLower(name), ext) {
				files = append(files, filepath.Join(path, name))
				break
			}
		}
	}
	if len(files) == 0 {
		return nil, errNoMatchingFiles
	}
	sort.Strings(files)
	return files, nil
}

// mergeMaps 递归合并两个 map，将 src 合并到 dst 中并返回 dst。
func mergeMaps(dst, src map[string]any) map[string]any {
	if dst == nil {
		return src
	}
	for key, srcVal := range src {
		if dstVal, ok := dst[key]; ok {
			if dstMap, ok := dstVal.(map[string]any); ok {
				if srcMap, ok := srcVal.(map[string]any); ok {
					dst[key] = mergeMaps(dstMap, srcMap)
					continue
				}
			}
		}
		dst[key] = srcVal
	}
	return dst
}
