// Package api 是业务层唯一入口，只做类型和函数转发。
// 业务层只能通过此包访问 app 功能，禁止直接引用 app 内部包。
package api

import (
	"app"
)

// ========== 类型转发 ==========
type (
	Message     = app.Message
	HandlerFunc = app.HandlerFunc
	Client      = app.Client
	App         = app.App
)

// ========== 函数转发 ==========
var (
	// NewApp 获取全局单例，需调用 Init(serviceType) 传入服务类型
	// 使用方式：api.NewApp().Init("crawler")
	NewApp = app.GetApp

	// DisableRegistry 禁用注册客户端，必须在 Init 之前调用
	// 用于注册中心自身或其他不需要注册到注册中心的服务
	// 使用方式：api.NewApp().DisableRegistry()
	DisableRegistry = app.GetApp().DisableRegistry

	// 方法注册
	RegisterStruct = app.RegisterStruct
	RegisterFunc   = app.RegisterFunc
	ListMethods    = app.ListMethods
	GetMethod      = app.GetMethod

	// 对象池管理
	NewObjectPool = app.NewObjectPool

	// 消息编解码缓存刷新
	RefreshMessageCodec = app.RefreshMessageCodec

	// 日志
	LogDebug = app.LogDebug
	LogInfo  = app.LogInfo
	LogWarn  = app.LogWarn
	LogError = app.LogError

	// 业务配置
	GetConfig    = app.GetBusinessConfig
	SetConfig    = app.SetBusinessConfig
	DeleteConfig = app.DeleteBusinessConfig
	GetAllConfig = app.GetAllBusinessConfig
)
