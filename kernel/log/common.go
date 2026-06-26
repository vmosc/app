// app/kernel/log/common.go
package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	globalLogger *Logger
	initMu       sync.Mutex

	errorHandler   func(err error)
	errorHandlerMu sync.RWMutex
)

// Config 日志配置结构。
type Config struct {
	LogPath          string        // 日志路径：如果是目录则多文件模式，如果以 .log 结尾则单文件模式
	RotationInterval time.Duration // 切割间隔，例如 24*time.Hour、1*time.Hour；0 表示不切割
	RetentionPeriod  time.Duration // 保留时长，例如 7*24*time.Hour；0 表示不清理
}

// Logger 核心结构。
type Logger struct {
	config      Config
	singleFile  bool
	currentSpan string
	commonFile  *os.File
	errorFile   *os.File
	metricFile  *os.File
	mu          sync.Mutex
	stopCleaner chan struct{}
	cleanerOnce sync.Once
	closeOnce   sync.Once
	cleanerWg   sync.WaitGroup
}

// SetErrorHandler 注册错误处理回调。
func SetErrorHandler(handler func(err error)) {
	errorHandlerMu.Lock()
	defer errorHandlerMu.Unlock()
	errorHandler = handler
}

// handleError 内部函数。
func handleError(err error) {
	if err == nil {
		return
	}
	errorHandlerMu.RLock()
	handler := errorHandler
	errorHandlerMu.RUnlock()
	if handler != nil {
		handler(err)
	}
}

// Init 初始化日志系统。
func Init(logPath string) error {
	return InitWithConfig(Config{
		LogPath:          logPath,
		RotationInterval: 24 * time.Hour,
		RetentionPeriod:  7 * 24 * time.Hour,
	})
}

// InitWithConfig 使用配置初始化日志系统。
func InitWithConfig(cfg Config) error {
	initMu.Lock()
	defer initMu.Unlock()

	if globalLogger != nil {
		return fmt.Errorf("log package already initialized")
	}

	if cfg.RotationInterval < 0 {
		return fmt.Errorf("rotation interval must be non-negative")
	}
	if cfg.RetentionPeriod < 0 {
		return fmt.Errorf("retention period must be non-negative")
	}

	singleFile := filepath.Ext(cfg.LogPath) == ".log"

	var commonFile, errorFile, metricFile *os.File
	var err error
	span := ""

	if singleFile {
		commonFile, err = openFile(cfg.LogPath)
		if err != nil {
			return fmt.Errorf("open single log file: %w", err)
		}
		errorFile = commonFile
		metricFile = commonFile
	} else {
		if err := os.MkdirAll(cfg.LogPath, 0755); err != nil {
			return fmt.Errorf("create log dir: %w", err)
		}
		span = timeToSpan(time.Now(), cfg.RotationInterval)
		commonFile, err = openFile(filepath.Join(cfg.LogPath, "log-"+span+".log"))
		if err != nil {
			return fmt.Errorf("open app log: %w", err)
		}
		errorFile, err = openFile(filepath.Join(cfg.LogPath, "error-"+span+".log"))
		if err != nil {
			commonFile.Close()
			return fmt.Errorf("open error log: %w", err)
		}
		metricFile, err = openFile(filepath.Join(cfg.LogPath, "metric-"+span+".log"))
		if err != nil {
			commonFile.Close()
			errorFile.Close()
			return fmt.Errorf("open metric log: %w", err)
		}
	}

	l := &Logger{
		config:      cfg,
		singleFile:  singleFile,
		currentSpan: span,
		commonFile:  commonFile,
		errorFile:   errorFile,
		metricFile:  metricFile,
		stopCleaner: make(chan struct{}),
	}

	if !singleFile && cfg.RetentionPeriod > 0 {
		l.startCleaner()
	}

	globalLogger = l
	return nil
}

// Reconfigure 热更新日志配置。
func Reconfigure(cfg Config) error {
	initMu.Lock()
	defer initMu.Unlock()

	if globalLogger == nil {
		return fmt.Errorf("log package not initialized")
	}

	globalLogger.mu.Lock()
	defer globalLogger.mu.Unlock()

	if cfg.LogPath != "" && cfg.LogPath != globalLogger.config.LogPath {
		return fmt.Errorf("LogPath change requires restart, current: %s, requested: %s",
			globalLogger.config.LogPath, cfg.LogPath)
	}

	if cfg.RotationInterval > 0 {
		globalLogger.config.RotationInterval = cfg.RotationInterval
	}
	if cfg.RetentionPeriod > 0 {
		globalLogger.config.RetentionPeriod = cfg.RetentionPeriod
	}

	return nil
}

// Reset 重置日志系统，关闭当前 logger 并等待清理协程退出。
func Reset() error {
	initMu.Lock()
	defer initMu.Unlock()
	if globalLogger == nil {
		return nil
	}
	err := globalLogger.Close()
	globalLogger.cleanerWg.Wait()
	globalLogger = nil
	return err
}

// CleanupNow 手动触发一次过期日志清理（用于测试）。
// 使用 TryLock 避免死锁，成功后调用无锁清理函数。
func CleanupNow() {
	initMu.Lock()
	l := globalLogger
	initMu.Unlock()
	if l == nil || l.singleFile {
		return
	}
	// 尝试获取锁，若获取失败则等待并重试（最多 10 次）
	for i := 0; i < 10; i++ {
		if l.mu.TryLock() {
			// 成功获取锁，执行清理，然后解锁
			l.cleanupLocked()
			l.mu.Unlock()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// 超时后记录错误
	handleError(fmt.Errorf("CleanupNow: unable to acquire lock after retries"))
}

// timeToSpan 根据切割间隔将时间转换为文件名中的字符串。
func timeToSpan(t time.Time, interval time.Duration) string {
	if interval <= 0 {
		return t.Format("2006-01-02")
	}
	switch {
	case interval%(24*time.Hour) == 0 && interval >= 24*time.Hour:
		return t.Format("2006-01-02")
	case interval%time.Hour == 0 && interval >= time.Hour:
		return t.Format("2006-01-02-15")
	case interval%time.Minute == 0 && interval >= time.Minute:
		return t.Format("2006-01-02-15-04")
	default:
		return t.Format("2006-01-02-15-04-05")
	}
}

func openFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
}

// Close 关闭所有打开的文件并停止清理协程。
func (l *Logger) Close() error {
	var err error
	l.closeOnce.Do(func() {
		if l.stopCleaner != nil {
			close(l.stopCleaner)
		}

		l.mu.Lock()
		defer l.mu.Unlock()

		if l.commonFile != nil {
			if e := l.commonFile.Close(); e != nil {
				err = e
			}
		}
		if l.errorFile != nil && l.errorFile != l.commonFile {
			if e := l.errorFile.Close(); e != nil {
				err = e
			}
		}
		if l.metricFile != nil && l.metricFile != l.commonFile && l.metricFile != l.errorFile {
			if e := l.metricFile.Close(); e != nil {
				err = e
			}
		}
	})
	return err
}

func getLogger() *Logger {
	if globalLogger == nil {
		panic("log package not initialized: must call Init or InitWithConfig in main")
	}
	return globalLogger
}

// rotateFilesIfNeeded 检查是否需要切割文件（必须在锁内调用）。
func (l *Logger) rotateFilesIfNeeded(now time.Time) error {
	if l.singleFile {
		return nil
	}
	newSpan := timeToSpan(now, l.config.RotationInterval)
	if newSpan == l.currentSpan {
		return nil
	}

	commonNew, err1 := openFile(filepath.Join(l.config.LogPath, "log-"+newSpan+".log"))
	errorNew, err2 := openFile(filepath.Join(l.config.LogPath, "error-"+newSpan+".log"))
	metricNew, err3 := openFile(filepath.Join(l.config.LogPath, "metric-"+newSpan+".log"))

	if err1 != nil || err2 != nil || err3 != nil {
		if commonNew != nil {
			commonNew.Close()
		}
		if errorNew != nil {
			errorNew.Close()
		}
		if metricNew != nil {
			metricNew.Close()
		}
		switch {
		case err1 != nil:
			return fmt.Errorf("rotate common: %w", err1)
		case err2 != nil:
			return fmt.Errorf("rotate error: %w", err2)
		default:
			return fmt.Errorf("rotate metric: %w", err3)
		}
	}

	if l.commonFile != nil {
		l.commonFile.Close()
	}
	if l.errorFile != nil && l.errorFile != l.commonFile {
		l.errorFile.Close()
	}
	if l.metricFile != nil && l.metricFile != l.commonFile && l.metricFile != l.errorFile {
		l.metricFile.Close()
	}

	l.commonFile = commonNew
	l.errorFile = errorNew
	l.metricFile = metricNew
	l.currentSpan = newSpan
	return nil
}

// output 写入一行日志。
func (l *Logger) output(w io.Writer, prefix string, v ...interface{}) {
	var rotateErr error
	var writeErr error

	l.mu.Lock()
	now := time.Now()
	if !l.singleFile {
		rotateErr = l.rotateFilesIfNeeded(now)
	}
	msg := fmt.Sprint(v...)
	line := fmt.Sprintf("%s %s %s\n", now.Format(time.RFC3339), prefix, msg)
	_, writeErr = w.Write([]byte(line))
	l.mu.Unlock()

	if rotateErr != nil {
		handleError(rotateErr)
	}
	if writeErr != nil {
		handleError(fmt.Errorf("write log failed: %w", writeErr))
	}
}

// startCleaner 启动后台清理协程。
func (l *Logger) startCleaner() {
	l.cleanerOnce.Do(func() {
		l.cleanerWg.Add(1)
		go func() {
			defer l.cleanerWg.Done()
			l.cleanupLoop()
		}()
	})
}

// cleanupLoop 定期扫描并删除过期文件。
func (l *Logger) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			l.cleanupLocked()
			l.mu.Unlock()
		case <-l.stopCleaner:
			return
		}
	}
}

// cleanupLocked 执行清理任务，使用文件 ModTime 判断过期。
// 调用者必须持有 l.mu 锁。
func (l *Logger) cleanupLocked() {
	if l.singleFile {
		return
	}
	entries, err := os.ReadDir(l.config.LogPath)
	if err != nil {
		handleError(fmt.Errorf("cleanup read dir: %w", err))
		return
	}

	now := time.Now()
	threshold := now.Add(-l.config.RetentionPeriod)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()

		// 只匹配日志文件
		if !strings.HasPrefix(name, "log-") && !strings.HasPrefix(name, "error-") && !strings.HasPrefix(name, "metric-") {
			continue
		}
		if !strings.HasSuffix(name, ".log") {
			continue
		}

		fullPath := filepath.Join(l.config.LogPath, name)

		// 使用文件 ModTime 判断过期
		info, err := os.Stat(fullPath)
		if err != nil {
			handleError(fmt.Errorf("cleanup stat %s: %w", fullPath, err))
			continue
		}

		// 检查是否是当前正在使用的文件（通过文件名中的 span 判断）
		if strings.Contains(name, l.currentSpan) {
			continue
		}

		if info.ModTime().Before(threshold) {
			if err := os.Remove(fullPath); err != nil {
				handleError(fmt.Errorf("cleanup remove %s: %w", fullPath, err))
			}
		}
	}
}
