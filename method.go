package app

import (
	"app/kernel/log"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

// MethodInfo 方法信息。
type MethodInfo struct {
	Name       string
	MethodType reflect.Method
	FuncValue  reflect.Value
	IsFunc     bool
}

type MethodRegistry struct {
	mu      sync.RWMutex
	methods map[string]*MethodInfo
}

var globalMethodRegistry = &MethodRegistry{
	methods: make(map[string]*MethodInfo),
}

// 延迟注册表：在 server 未初始化时暂存 handler
var (
	deferredHandlers = make(map[string]HandlerFunc)
	deferredMu       sync.Mutex
)

// syncDeferredHandlers 将延迟注册的方法同步到 server（由 app.Init 调用）
func syncDeferredHandlers(server *Server) {
	deferredMu.Lock()
	defer deferredMu.Unlock()
	for method, handler := range deferredHandlers {
		server.registerHandler(method, handler)
		log.Debug("method: synced deferred handler", "method", method)
	}
	deferredHandlers = make(map[string]HandlerFunc)
}

// RegisterStruct 注册结构体的所有公开方法。
// 增加 Debug 日志提示跳过了哪些非公开方法
func RegisterStruct(ptr any, prefix string) {
	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		log.Error("RegisterStruct: expected pointer to struct")
		return
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		log.Error("RegisterStruct: expected struct")
		return
	}
	t := v.Type()
	log.Info("method: registering struct methods", "struct", t.Name(), "prefix", prefix)

	var skipped int
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		if method.PkgPath != "" {
			log.Debug("method: skipping unexported method", "struct", t.Name(), "method", method.Name)
			skipped++
			continue
		}
		methodName := method.Name
		if prefix != "" {
			methodName = prefix + "." + methodName
		}
		globalMethodRegistry.mu.Lock()
		globalMethodRegistry.methods[methodName] = &MethodInfo{
			Name:       methodName,
			MethodType: method,
			IsFunc:     false,
		}
		globalMethodRegistry.mu.Unlock()

		registerToServer(methodName, createStructHandler(v.Type(), method))
		log.Debug("method: registered", "name", methodName)
	}
	if skipped > 0 {
		log.Debug("method: skipped unexported methods", "struct", t.Name(), "count", skipped)
	}
	log.Info("method: struct methods registered", "struct", t.Name(), "count", t.NumMethod()-skipped)
}

func createStructHandler(structType reflect.Type, method reflect.Method) HandlerFunc {
	return func(ctx context.Context, req *Message) (*Message, error) {
		instance := reflect.New(structType)
		recv := instance
		if method.Type.NumIn() > 0 {
			recvType := method.Type.In(0)
			if recvType.Kind() != reflect.Ptr {
				recv = instance.Elem()
			}
		}
		args := []reflect.Value{recv, reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := method.Func.Call(args)

		if len(results) != 2 {
			return nil, fmt.Errorf("method %s must return (*Message, error)", method.Name)
		}

		resp, ok := results[0].Interface().(*Message)
		if !ok {
			return nil, fmt.Errorf("method %s first return value must be *Message", method.Name)
		}

		var err error
		if !results[1].IsNil() {
			err = results[1].Interface().(error)
		}
		return resp, err
	}
}

// RegisterFunc 注册普通函数。
func RegisterFunc(fn any, name string) {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		log.Error("RegisterFunc: expected function")
		return
	}
	if name == "" {
		fullName := runtime.FuncForPC(v.Pointer()).Name()
		parts := strings.Split(fullName, ".")
		name = parts[len(parts)-1]
	}
	globalMethodRegistry.mu.Lock()
	globalMethodRegistry.methods[name] = &MethodInfo{
		Name:      name,
		FuncValue: v,
		IsFunc:    true,
	}
	globalMethodRegistry.mu.Unlock()

	registerToServer(name, createFuncHandler(v))
	log.Info("method: registered function", "name", name)
}

func createFuncHandler(fn reflect.Value) HandlerFunc {
	return func(ctx context.Context, req *Message) (*Message, error) {
		results := fn.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})

		if len(results) != 2 {
			return nil, fmt.Errorf("function must return (*Message, error)")
		}

		resp, ok := results[0].Interface().(*Message)
		if !ok {
			return nil, fmt.Errorf("function first return value must be *Message")
		}

		var err error
		if !results[1].IsNil() {
			err = results[1].Interface().(error)
		}
		return resp, err
	}
}

// registerToServer 注册方法到服务端（若 server 未就绪则暂存）。
func registerToServer(method string, handler HandlerFunc) {
	app := GetApp()
	if app.server != nil {
		app.server.registerHandler(method, handler)
	} else {
		deferredMu.Lock()
		deferredHandlers[method] = handler
		deferredMu.Unlock()
		log.Warn("method: server not initialized, registration deferred", "method", method)
	}
}

// GetMethod 获取已注册的方法信息。
func GetMethod(name string) (*MethodInfo, bool) {
	globalMethodRegistry.mu.RLock()
	defer globalMethodRegistry.mu.RUnlock()
	m, ok := globalMethodRegistry.methods[name]
	return m, ok
}

// ListMethods 列出所有已注册的方法名。
func ListMethods() []string {
	globalMethodRegistry.mu.RLock()
	defer globalMethodRegistry.mu.RUnlock()
	names := make([]string, 0, len(globalMethodRegistry.methods))
	for n := range globalMethodRegistry.methods {
		names = append(names, n)
	}
	return names
}
