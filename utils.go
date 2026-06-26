package app

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var counter uint64

// generateID 生成唯一 ID（用于消息 ID 和注册 ID）。
// 返回格式类似 "1734567890123456789-1-a1b2c3d4"，总长度通常为 8 位十六进制后缀。
func generateID() string {
	timestamp := time.Now().UnixNano()
	seq := atomic.AddUint64(&counter, 1)
	randBytes := make([]byte, 4)
	if _, err := rand.Read(randBytes); err != nil {
		return fmt.Sprintf("%d-%d", timestamp, seq)
	}
	randStr := hex.EncodeToString(randBytes) // 8 位十六进制
	return fmt.Sprintf("%d-%d-%s", timestamp, seq, randStr)
}
