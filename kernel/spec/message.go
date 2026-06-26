package spec

// Message 通信消息的基础数据结构
// 注意：这是纯数据结构，不包含任何方法或业务逻辑
// 性能优化：字段顺序按照内存对齐优化，减少填充字节
type Message struct {
	// 基础元信息 (8+1+16 = 25字节，按8字节对齐)
	ID        string  `json:"id"`        // 消息唯一标识
	Version   uint8   `json:"version"`   // 协议版本
	_         [7]byte `json:"-"`         // 手动填充，确保8字节对齐，JSON序列化时忽略
	Timestamp int64   `json:"timestamp"` // Unix时间戳（纳秒级）

	// 传输控制
	Method   string            `json:"method"`   // 调用方法名
	Metadata map[string]string `json:"metadata"` // 元数据（路由、追踪、认证等）

	// 载荷与状态
	Body []byte `json:"body"` // 消息体原始字节
}

// 协议版本常量
const (
	Version1 uint8 = 1
)
