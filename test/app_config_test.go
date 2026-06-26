// app/test/app_config_test.go
package test

import (
	"app/api"
	"testing"
)

func TestConfig(t *testing.T) {
	t.Run("业务配置读写与持久化", func(t *testing.T) {
		app, cleanup := StartTestApp(t)
		defer cleanup()

		err := api.SetConfig("db.host", "localhost")
		if err != nil {
			t.Fatalf("SetConfig failed: %v", err)
		}
		err = api.SetConfig("db.port", 3306)
		if err != nil {
			t.Fatalf("SetConfig failed: %v", err)
		}

		val, ok := api.GetConfig("db.host")
		if !ok || val != "localhost" {
			t.Errorf("GetConfig db.host = %v, want localhost", val)
		}
		val, ok = api.GetConfig("db.port")
		if !ok || val != 3306 {
			t.Errorf("GetConfig db.port = %v, want 3306", val)
		}

		if err := app.ReloadConfig(); err != nil {
			t.Errorf("ReloadConfig failed: %v", err)
		}
		val, ok = api.GetConfig("db.host")
		if !ok || val != "localhost" {
			t.Errorf("After reload, db.host = %v, want localhost", val)
		}
	})

	t.Run("配置文件不存在时返回空配置", func(t *testing.T) {
		_, cleanup := StartTestApp(t)
		defer cleanup()

		_, ok := api.GetConfig("any.key")
		if ok {
			t.Error("expected config to be empty")
		}
	})

	t.Run("业务配置批量设置与覆盖", func(t *testing.T) {
		_, cleanup := StartTestApp(t)
		defer cleanup()

		api.SetConfig("a", 1)
		api.SetConfig("b", "hello")
		api.SetConfig("c", true)

		api.SetConfig("a", 100)

		val, ok := api.GetConfig("a")
		if !ok || val != 100 {
			t.Errorf("expected a=100, got %v", val)
		}
		val, ok = api.GetConfig("b")
		if !ok || val != "hello" {
			t.Errorf("expected b='hello', got %v", val)
		}
		val, ok = api.GetConfig("c")
		if !ok || val != true {
			t.Errorf("expected c=true, got %v", val)
		}
	})
}
