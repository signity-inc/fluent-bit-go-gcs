package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFluentBitPluginConcurrentAccess tests concurrent access to FluentBitPlugin
func TestFluentBitPluginConcurrentAccess(t *testing.T) {
	// Create plugin config
	config := &PluginConfig{
		Bucket:           "test-bucket",
		Region:           "us-east-1",
		Prefix:           "test",
		OutputBufferSize: 1024 * 1024,
		FlushInterval:    time.Minute,
		StorageType:      StorageTypeFile,
		OutputDir:        t.TempDir(),
	}

	plugin, err := NewFluentBitPlugin(context.Background(), config)
	if err != nil {
		t.Fatalf("Failed to create plugin: %v", err)
	}

	const (
		numGoroutines = 100
		numOperations = 1000
	)

	var wg sync.WaitGroup
	var addCount, flushCount int64

	// Concurrent operations on plugin context
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < numOperations; j++ {
				switch rand.Intn(3) {
				case 0, 1: // Add record
					record := map[string]interface{}{
						"worker": id,
						"op":     j,
						"data":   fmt.Sprintf("test-data-%d-%d", id, j),
					}
					
					func() {
						plugin.mutex.Lock()
						defer plugin.mutex.Unlock()
						
						if plugin.context != nil && plugin.context.bufferManager != nil {
							if recordBytes, err := json.Marshal(record); err == nil {
								if err := plugin.context.bufferManager.AddRecord(recordBytes); err == nil {
									atomic.AddInt64(&addCount, 1)
								}
							}
						}
					}()
					
				case 2: // Check and flush if needed
					func() {
						plugin.mutex.Lock()
						defer plugin.mutex.Unlock()
						
						if plugin.context != nil && plugin.context.bufferManager != nil {
							if plugin.context.bufferManager.ShouldFlush() {
								plugin.context.bufferManager.Flush()
								atomic.AddInt64(&flushCount, 1)
							}
						}
					}()
				}
			}
		}(i)
	}

	wg.Wait()
	
	t.Logf("Operations completed - Add: %d, Flush: %d", 
		addCount, flushCount)
}

// TestBufferManagerConcurrentOperations tests thread-safe operations on BufferManager
func TestBufferManagerConcurrentOperations(t *testing.T) {
	tests := []struct {
		name          string
		bufferSize    int
		numWriters    int
		numFlushers   int
		writesPerWorker int
	}{
		{"Small buffer", 1024, 10, 5, 100},
		{"Medium buffer", 1024*100, 50, 10, 500},
		{"Large buffer", 1024*1024, 100, 20, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var flushCount int64
			var writeErrors int64
			
			config := BufferConfig{
				MaxBufferSizeBytes: tt.bufferSize,
				FlushTimeoutSec:    60,
				AddTruncationMeta:  true,
			}
			
			bm := NewBufferManager(config, func() {
				atomic.AddInt64(&flushCount, 1)
			})

			var wg sync.WaitGroup
			
			// Writers
			for i := 0; i < tt.numWriters; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < tt.writesPerWorker; j++ {
						record := map[string]interface{}{
							"writer": id,
							"seq":    j,
							"data":   fmt.Sprintf("data-%d-%d", id, j),
							"time":   time.Now().UnixNano(),
						}
						if recordBytes, err := json.Marshal(record); err == nil {
							if err := bm.AddRecord(recordBytes); err != nil {
								atomic.AddInt64(&writeErrors, 1)
							}
						}
					}
				}(i)
			}

			// Flushers
			for i := 0; i < tt.numFlushers; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < 50; j++ {
						if bm.ShouldFlush() {
							bm.Flush()
						}
						time.Sleep(time.Millisecond * 5)
					}
				}(i)
			}

			wg.Wait()
			
			// Final flush
			bm.Flush()
			
			t.Logf("Test completed - Flushes: %d, Write errors: %d", 
				flushCount, writeErrors)
		})
	}
}

// TestRaceConditions uses race detector to find data races
func TestRaceConditions(t *testing.T) {
	// Run with: go test -race -run TestRaceConditions
	
	config := BufferConfig{
		MaxBufferSizeBytes: 1024*1024,
		FlushTimeoutSec:    60,
	}
	
	bm := NewBufferManager(config, func() {
		// Overflow callback
	})

	const numGoroutines = 50
	var wg sync.WaitGroup

	// Multiple readers and writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Mix of operations that could cause races
			for j := 0; j < 100; j++ {
				switch rand.Intn(6) {
				case 0:
					if recordBytes, err := json.Marshal(map[string]interface{}{"id": id}); err == nil {
						bm.AddRecord(recordBytes)
					}
				case 1:
					// GetBufferData is not exposed, skip
				case 2:
					// GetSize is not exposed, skip
				case 3:
					bm.ShouldFlush()
				case 4:
					bm.Flush()
				case 5:
					bm.Reset()
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestContextLeakage ensures contexts are properly cleaned up
func TestContextLeakage(t *testing.T) {
	// Test multiple plugin creation and cleanup
	const numIterations = 100
	
	for i := 0; i < numIterations; i++ {
		config := &PluginConfig{
			Bucket:           "test-bucket",
			Region:           "us-east-1", 
			Prefix:           fmt.Sprintf("test-%d", i),
			OutputBufferSize: 1024,
			FlushInterval:    time.Second,
			StorageType:      StorageTypeFile,
			OutputDir:        t.TempDir(),
		}
		
		plugin, err := NewFluentBitPlugin(context.Background(), config)
		if err != nil {
			t.Fatalf("Failed to create plugin: %v", err)
		}
		
		// Simulate some work
		func() {
			plugin.mutex.Lock()
			defer plugin.mutex.Unlock()
			
			if plugin.context != nil && plugin.context.bufferManager != nil {
				if recordBytes, err := json.Marshal(map[string]interface{}{"data": "test"}); err == nil {
					plugin.context.bufferManager.AddRecord(recordBytes)
				}
			}
		}()
		
		// Cleanup would normally happen in FLBPluginExit
		if plugin.context != nil && plugin.context.bufferManager != nil {
			plugin.context.bufferManager.Flush()
		}
	}
	
	// In real scenario, memory usage should be stable after iterations
	t.Logf("Context lifecycle test completed for %d iterations", numIterations)
}

// TestStressWithErrors tests behavior under error conditions
func TestStressWithErrors(t *testing.T) {
	const errorRate = 0.1 // 10% error rate
	var errorCount int64
	var overflowCount int64
	
	config := BufferConfig{
		MaxBufferSizeBytes: 1024*10,
		FlushTimeoutSec:    60,
	}
	
	bm := NewBufferManager(config, func() {
		atomic.AddInt64(&overflowCount, 1)
	})

	var wg sync.WaitGroup
	
	// Writers that handle errors
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				record := map[string]interface{}{
					"id": id,
					"data": generateRandomString(100),
				}
				if recordBytes, err := json.Marshal(record); err == nil {
					if err := bm.AddRecord(recordBytes); err != nil {
						atomic.AddInt64(&errorCount, 1)
					}
				}
			}
		}(i)
	}

	// Aggressive flushers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				bm.Flush()
				time.Sleep(time.Millisecond * 10)
			}
		}()
	}

	wg.Wait()
	
	t.Logf("Stress test completed - Errors: %d, Overflows: %d", errorCount, overflowCount)
}

// Helper functions
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}