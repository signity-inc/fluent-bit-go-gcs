package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/fluent/fluent-bit-go/output"
)

// MockStorageClient はテスト用のモックストレージクライアント
type MockStorageClient struct {
	writeFunc func(bucket, objectKey string, data io.Reader) error
	closed    bool
}

func (m *MockStorageClient) Write(bucket, objectKey string, data io.Reader) error {
	if m.writeFunc != nil {
		return m.writeFunc(bucket, objectKey, data)
	}
	return nil
}

func (m *MockStorageClient) Close() error {
	m.closed = true
	return nil
}

// TestFLBPluginFlushCtxReturnValues はFLBPluginFlushCtxの返り値をテスト
func TestFLBPluginFlushCtxReturnValues(t *testing.T) {
	tests := []struct {
		name           string
		setupPlugin    func() *FluentBitPlugin
		storageError   error
		expectedReturn int
		description    string
	}{
		{
			name: "成功時はFLB_OKを返す",
			setupPlugin: func() *FluentBitPlugin {
				config := &PluginConfig{
					Bucket:           "test-bucket",
					Prefix:           "test-prefix",
					Region:           "asia-northeast1",
					OutputBufferSize: 1024,
					FlushInterval:    time.Minute,
					StorageType:      StorageTypeFile,
					OutputDir:        "/tmp/test-output",
				}
				plugin, err := NewFluentBitPlugin(context.Background(), config)
				if err != nil {
					panic(err)
				}
				if plugin == nil {
					panic("plugin is nil")
				}
				if plugin.context == nil {
					panic("plugin.context is nil")
				}
				
				// モックストレージクライアントを設定
				plugin.context.storageClient = &MockStorageClient{
					writeFunc: func(bucket, objectKey string, data io.Reader) error {
						return nil // 成功
					},
				}
				return plugin
			},
			storageError:   nil,
			expectedReturn: output.FLB_OK,
			description:    "データの書き込みに成功した場合、FLB_OKを返す",
		},
		{
			name: "リトライ可能なエラー時はFLB_RETRYを返す",
			setupPlugin: func() *FluentBitPlugin {
				config := &PluginConfig{
					Bucket:           "test-bucket",
					Prefix:           "test-prefix",
					Region:           "asia-northeast1",
					OutputBufferSize: 1024,
					FlushInterval:    time.Minute,
					StorageType:      StorageTypeFile,
					OutputDir:        "/tmp/test-output",
				}
				plugin, _ := NewFluentBitPlugin(context.Background(), config)
				
				// モックストレージクライアントを設定
				plugin.context.storageClient = &MockStorageClient{
					writeFunc: func(bucket, objectKey string, data io.Reader) error {
						return errors.New("connection timeout") // リトライ可能なエラー
					},
				}
				return plugin
			},
			storageError:   errors.New("connection timeout"),
			expectedReturn: output.FLB_RETRY,
			description:    "一時的なエラー（タイムアウト等）の場合、FLB_RETRYを返す",
		},
		{
			name: "永続的なエラー時はFLB_ERRORを返す",
			setupPlugin: func() *FluentBitPlugin {
				config := &PluginConfig{
					Bucket:           "test-bucket",
					Prefix:           "test-prefix",
					Region:           "asia-northeast1",
					OutputBufferSize: 1024,
					FlushInterval:    time.Minute,
					StorageType:      StorageTypeFile,
					OutputDir:        "/tmp/test-output",
				}
				plugin, _ := NewFluentBitPlugin(context.Background(), config)
				
				// モックストレージクライアントを設定
				plugin.context.storageClient = &MockStorageClient{
					writeFunc: func(bucket, objectKey string, data io.Reader) error {
						return errors.New("permission denied") // 永続的なエラー
					},
				}
				return plugin
			},
			storageError:   errors.New("permission denied"),
			expectedReturn: output.FLB_ERROR,
			description:    "永続的なエラー（権限不足等）の場合、FLB_ERRORを返す",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := tt.setupPlugin()
			defer func() {
				if plugin.context.storageClient != nil {
					plugin.context.storageClient.Close()
				}
			}()

			// テストデータを準備
			testRecord := map[interface{}]interface{}{
				"test":      "data",
				"timestamp": "2024-01-01T00:00:00Z",
			}
			
			// レコードを追加
			err := plugin.addRecordToBuffer("test-tag", testRecord)
			if err != nil {
				t.Fatalf("Failed to add record: %v", err)
			}

			// performFlushを呼び出して結果を確認
			result, err := plugin.performFlush("test-tag")
			
			// 返り値の検証
			var actualReturn int
			switch result {
			case FlushResultSuccess:
				actualReturn = output.FLB_OK
			case FlushResultRetry:
				actualReturn = output.FLB_RETRY
			case FlushResultError:
				actualReturn = output.FLB_ERROR
			}

			if actualReturn != tt.expectedReturn {
				t.Errorf("%s: 期待される返り値 %d, 実際の返り値 %d, エラー: %v",
					tt.description, tt.expectedReturn, actualReturn, err)
			}
		})
	}
}

// TestPerformFlushBehavior はperformFlush関数の詳細な動作をテスト
func TestPerformFlushBehavior(t *testing.T) {
	tests := []struct {
		name              string
		bufferData        []byte
		storageError      error
		expectBufferClear bool
		expectFlushResult FlushResult
	}{
		{
			name:              "成功時はバッファがクリアされる",
			bufferData:        []byte(`{"test": "data"}`),
			storageError:      nil,
			expectBufferClear: true,
			expectFlushResult: FlushResultSuccess,
		},
		{
			name:              "リトライ可能エラー時はバッファが保持される",
			bufferData:        []byte(`{"test": "data"}`),
			storageError:      errors.New("connection reset"),
			expectBufferClear: false,
			expectFlushResult: FlushResultRetry,
		},
		{
			name:              "永続的エラー時はバッファが保持される（クリアは呼び出し元の責務）",
			bufferData:        []byte(`{"test": "data"}`),
			storageError:      errors.New("unauthorized"),
			expectBufferClear: false,
			expectFlushResult: FlushResultError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &PluginConfig{
				Bucket:           "test-bucket",
				Prefix:           "test-prefix",
				Region:           "asia-northeast1",
				OutputBufferSize: 1024,
				FlushInterval:    time.Minute,
				StorageType:      StorageTypeFile,
				OutputDir:        "/tmp/test-output",
			}
			plugin, err := NewFluentBitPlugin(context.Background(), config)
			if err != nil {
				t.Fatalf("Failed to create plugin: %v", err)
			}
			
			// モックストレージクライアントを設定
			plugin.context.storageClient = &MockStorageClient{
				writeFunc: func(bucket, objectKey string, data io.Reader) error {
					return tt.storageError
				},
			}

			// テストデータをバッファに追加
			plugin.context.bufferManager.AddRecord(tt.bufferData)
			initialSize := plugin.context.bufferManager.Size()

			// performFlushを実行
			result, _ := plugin.performFlush("test-tag")

			// 結果の検証
			if result != tt.expectFlushResult {
				t.Errorf("期待されるFlushResult: %v, 実際: %v", tt.expectFlushResult, result)
			}

			// バッファ状態の検証
			currentSize := plugin.context.bufferManager.Size()
			if tt.expectBufferClear && currentSize != 0 {
				t.Errorf("バッファがクリアされていません。サイズ: %d", currentSize)
			}
			if !tt.expectBufferClear && currentSize != initialSize {
				t.Errorf("バッファが予期せずに変更されました。初期サイズ: %d, 現在のサイズ: %d", 
					initialSize, currentSize)
			}
		})
	}
}

// TestAddRecordToBuffer はaddRecordToBuffer関数のテスト
func TestAddRecordToBuffer(t *testing.T) {
	config := &PluginConfig{
		Bucket:           "test-bucket",
		Prefix:           "test-prefix",
		Region:           "asia-northeast1",
		OutputBufferSize: 200, // 小さなバッファ
		FlushInterval:    time.Minute,
		StorageType:      StorageTypeFile,
		OutputDir:        "/tmp/test-output",
	}
	plugin, _ := NewFluentBitPlugin(context.Background(), config)

	tests := []struct {
		name          string
		records       []map[interface{}]interface{}
		expectError   bool
		expectFlush   bool
	}{
		{
			name: "通常のレコード追加",
			records: []map[interface{}]interface{}{
				{"test": "data1"},
				{"test": "data2"},
			},
			expectError: false,
			expectFlush: false,
		},
		{
			name: "複数レコードでバッファフル",
			records: []map[interface{}]interface{}{
				{"test": "data1"},
				{"test": "data2"},
				{"test": "data3"},
				{"test": "data4"},
			},
			expectError: false,
			expectFlush: false,
		},
		{
			name: "単一レコードがバッファサイズを超える場合",
			records: []map[interface{}]interface{}{
				{"test": string(make([]byte, 250))},
			},
			expectError: true,
			expectFlush: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// バッファをリセット
			plugin.context.bufferManager.Reset()
			
			var lastErr error
			for _, record := range tt.records {
				err := plugin.addRecordToBuffer("test-tag", record)
				if err != nil {
					lastErr = err
				}
			}

			if tt.expectError && lastErr == nil {
				t.Error("エラーが期待されましたが、発生しませんでした")
			}
			if !tt.expectError && lastErr != nil {
				t.Errorf("予期しないエラー: %v", lastErr)
			}
		})
	}
}