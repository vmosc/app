package log

import (
	"fmt"
	"time"
)

// MetricInc 记录指标递增事件（写入指标日志文件）
// 例如：MetricInc("user.login")
func MetricInc(name string) {
	getLogger().output(getLogger().metricFile, "METRIC", fmt.Sprintf("inc %s", name))
}

// MetricTiming 记录耗时指标（写入指标日志文件）
// 例如：MetricTiming("http.request", time.Since(start))
func MetricTiming(name string, d time.Duration) {
	getLogger().output(getLogger().metricFile, "METRIC", fmt.Sprintf("timing %s %v", name, d))
}

// StartTiming 启动一个计时器，返回一个函数，调用该函数会自动记录耗时。
// 用法示例：
//
//	defer log.StartTiming("http.request")()
func StartTiming(name string) func() {
	start := time.Now()
	return func() {
		MetricTiming(name, time.Since(start))
	}
}
