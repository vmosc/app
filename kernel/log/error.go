package log

import "fmt"

// Error 记录错误级别日志（写入错误日志文件）
func Error(v ...interface{}) {
	getLogger().output(getLogger().errorFile, "ERROR", v...)
}

// ErrorCode 记录带有错误码的错误日志（写入错误日志文件）
// 例如：ErrorCode(500, "internal server error")
func ErrorCode(code int, msg string) {
	getLogger().output(getLogger().errorFile, "ERROR", fmt.Sprintf("code=%d %s", code, msg))
}
