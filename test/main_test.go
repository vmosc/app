// app/test/main_test.go
package test

import (
	"app/kernel/log"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// 初始化日志，供所有测试使用
	if err := os.MkdirAll("./test_logs", 0755); err != nil {
		fmt.Printf("failed to create test_logs dir: %v\n", err)
		os.Exit(1)
	}
	err := log.InitWithConfig(log.Config{
		LogPath:          "./test_logs",
		RotationInterval: 24 * time.Hour,
		RetentionPeriod:  7 * 24 * time.Hour,
	})
	if err != nil {
		fmt.Printf("failed to init test log: %v\n", err)
		os.Exit(1)
	}

	log.Info("test: starting all tests", "time", time.Now().Format(time.RFC3339))

	code := m.Run()

	printTestReport()

	log.Info("test: all tests completed", "code", code)

	os.Exit(code)
}

func printTestReport() {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("                    TEST REPORT")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Test Time: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\nTest Summary:")
	fmt.Println("  - Unit Tests: Message, Client, Server, Config, Pool")
	fmt.Println("  - Integration Tests: Full lifecycle, Shutdown")
	fmt.Println("  - Concurrency Tests: Multi-goroutine access")
	fmt.Println("  - Benchmark Tests: Performance metrics")
	fmt.Println("\n" + strings.Repeat("=", 60))
}

