package log

// Debug 记录调试级别日志（写入普通日志文件）
func Debug(v ...interface{}) {
	getLogger().output(getLogger().commonFile, "DEBUG", v...)
}

// Info 记录信息级别日志（写入普通日志文件）
func Info(v ...interface{}) {
	getLogger().output(getLogger().commonFile, "INFO", v...)
}

// Warn 记录警告级别日志（写入普通日志文件）
func Warn(v ...interface{}) {
	getLogger().output(getLogger().commonFile, "WARN", v...)
}
