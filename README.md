# App — 微服务通信底座

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Test](https://github.com/your-org/app/actions/workflows/test.yml/badge.svg)](https://github.com/your-org/app/actions)

App 是一个轻量级、高性能的 **微服务基础框架**，专注于提供服务间通信和通用的服务注册/发现客户端能力。它并非一个完整的注册中心，而是每个微服务的 **底座基座**，让你可以快速构建稳定、可观测的微服务，同时也能在业务层基于基座实现真正的注册中心。

---

## ✨ 特性

- **高性能 RPC 通信**  
  支持 Unix Domain Socket 传输，内置二进制和 JSON 编解码器，可扩展自定义协议。

- **灵活的服务注册/发现客户端**  
  内置注册中心客户端，支持注册、心跳、注销、服务发现，可动态感知环境变化。

- **高效的资源池**  
  - 对象池：减少 GC 压力，支持预分配、健康检查、自动扩缩容  
  - 连接池：连接复用、探活、断线恢复  
  - 工作池：支持优先级调度、自动扩缩容、优雅关闭

- **完善的配置管理**  
  支持 JSON / YAML 多文件合并、热重载，业务配置与框架配置隔离。

- **生产级日志系统**  
  多级别日志、按时间自动切割、过期清理、并发安全。

- **内置可观测性**  
  暴露 Prometheus 指标，环形缓冲区存储历史统计数据，方便监控与诊断。

- **优雅启停**  
  按序注销服务 → 停止 server → 关闭 client，确保在途请求处理完毕。

- **易于扩展**  
  清晰的 `api` 包作为唯一入口，业务层可安全地基于基座开发新能力（如完整的注册中心）。

---

## 🚀 快速开始

### 安装

```bash
go get github.com/your-org/app
```

### 创建一个简单服务

```go
package main

import (
    "context"
    "app/api"
)

type EchoService struct{}

func (s *EchoService) Echo(ctx context.Context, req *api.Message) (*api.Message, error) {
    return &api.Message{
        ID:   req.ID,
        Body: []byte("Echo: " + string(req.Body)),
    }, nil
}

func main() {
    app := api.NewApp()

    // 初始化服务（serviceType 用于服务注册标识）
    if err := app.Init("echo-service"); err != nil {
        panic(err)
    }

    // 注册服务方法
    api.RegisterStruct(&EchoService{}, "echo")

    // 启动并等待信号
    if err := app.Run(); err != nil {
        panic(err)
    }
}
```

### 调用其他服务

```go
client := app.Client()

resp, err := client.Call("echo-service/Echo", []byte("Hello"))
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(resp.Body)) // 输出: Echo: Hello
```

---

## 📦 核心组件

| 组件 | 说明 |
|------|------|
| `app/api` | 业务层唯一入口，提供类型和函数转发 |
| `app/kernel/codec` | 编解码器（JSON、Binary）及注册管理 |
| `app/kernel/config` | 配置管理器（JSON、YAML 多文件合并） |
| `app/kernel/log` | 日志系统（多文件、轮转、清理） |
| `app/kernel/pool` | 对象池、连接池、工作池、指标收集器 |
| `app/kernel/unix` | Unix Domain Socket 传输层 |
| `app/kernel/spec` | 消息数据结构规范 |
| `app/...` | 框架主逻辑：App、Client、Server、RegistryClient、Message |

---

## 🔧 配置

业务配置文件 `config.yaml`（位于工作目录），示例：

```yaml
server:
  port: 8080
  host: "0.0.0.0"
database:
  host: "db.example.com"
  port: 5432
```

通过 API 读写配置：

```go
api.SetConfig("db.host", "localhost")
val, ok := api.GetConfig("db.host")
```

---

## 🧪 测试

所有测试均支持 `-race`，且无数据竞争。运行：

```bash
go test ./test/... -v -count=1 -race -cover -timeout 120s -bench=. -benchtime=1s
```

已覆盖功能、并发、边缘、稳定性、模糊测试及性能基准。

---

## 📊 性能

基准测试（部分）：

- 消息编码（1KB 载荷）: ~200 ns/op  
- 对象池并发存取（1M 次）: 0 错误，0 超时  
- 工作池处理 100K 任务: < 2.5s，零拒绝

---

## 🤝 贡献

欢迎提交 Issue / PR！请确保：

- 新功能有对应的测试用例
- 通过 `go test` 及 `-race` 检测
- 代码风格与现有代码一致

---

## 📄 许可证

MIT License © 2026 Your Name

---

## 🏗️ 架构简图

```
┌─────────────────────────────────────────────────────┐
│                    业务层 (Your App)                   │
│                        │                             │
│                   app/api (唯一入口)                  │
│                        │                             │
├────────────────────────┼─────────────────────────────┤
│                   App (框架核心)                      │
│     ┌──────────┬──────────┬──────────┬──────────┐   │
│     │  Client  │  Server  │ Registry │  Config  │   │
│     │  (通信)  │  (服务)  │  Client  │  (配置)  │   │
│     └──────────┴──────────┴──────────┴──────────┘   │
│                        │                             │
│     ┌──────────────────────────────────────────┐    │
│     │          kernel/ (基础设施)               │    │
│     │  codec · config · log · pool · unix      │    │
│     └──────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

---

**App** — 让微服务通信更简单、更可靠。