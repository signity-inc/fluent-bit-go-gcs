package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// TestGCSWriteErrorRetryBehavior はGCS書き込みエラー時のリトライ動作をテスト
func TestGCSWriteErrorRetryBehavior(t *testing.T) {
	tests := []struct {
		name                 string
		writeErrors          []error
		expectedFlushResults []FlushResult
		expectedWriteCount   int
		description          string
	}{
		{
			name: "一時的エラー後に成功",
			writeErrors: []error{
				errors.New("connection timeout"),
				nil, // 2回目は成功
			},
			expectedFlushResults: []FlushResult{
				FlushResultRetry,
				FlushResultSuccess,
			},
			expectedWriteCount: 2,
			description:        "一時的なエラー後、Fluent Bitがリトライして成功",
		},
		{
			name: "複数回の一時的エラー後に成功",
			writeErrors: []error{
				errors.New("service unavailable"),
				errors.New("connection reset"),
				errors.New("502 bad gateway"),
				nil, // 4回目で成功
			},
			expectedFlushResults: []FlushResult{
				FlushResultRetry,
				FlushResultRetry,
				FlushResultRetry,
				FlushResultSuccess,
			},
			expectedWriteCount: 4,
			description:        "複数の一時的エラー後、最終的に成功",
		},
		{
			name: "永続的エラーで即座に失敗",
			writeErrors: []error{
				errors.New("permission denied"),
			},
			expectedFlushResults: []FlushResult{
				FlushResultError,
			},
			expectedWriteCount: 1,
			description:        "権限エラーの場合、リトライせずに失敗",
		},
		{
			name: "一時的エラー後に永続的エラー",
			writeErrors: []error{
				errors.New("connection timeout"),
				errors.New("unauthorized access"),
			},
			expectedFlushResults: []FlushResult{
				FlushResultRetry,
				FlushResultError,
			},
			expectedWriteCount: 2,
			description:        "一時的エラー後に永続的エラーが発生",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// テスト用のプラグイン設定
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

			// 書き込み回数カウンター
			var writeCount int32
			errorIndex := 0

			// モックストレージクライアントを設定
			plugin.context.storageClient = &MockStorageClient{
				writeFunc: func(bucket, objectKey string, data io.Reader) error {
					atomic.AddInt32(&writeCount, 1)
					if errorIndex < len(tt.writeErrors) {
						err := tt.writeErrors[errorIndex]
						errorIndex++
						return err
					}
					return nil
				},
			}

			// テストデータをバッファに追加
			testData := []byte(`{"test": "data", "timestamp": "2024-01-01T00:00:00Z"}`)
			plugin.context.bufferManager.AddRecord(testData)

			// 各フラッシュ試行をシミュレート
			for i, expectedResult := range tt.expectedFlushResults {
				result, err := plugin.performFlush("test-tag")
				
				if result != expectedResult {
					t.Errorf("フラッシュ%d: 期待されるResult: %v, 実際: %v, エラー: %v",
						i+1, expectedResult, result, err)
				}

				// リトライの場合、データは保持されるべき
				if result == FlushResultRetry {
					if plugin.context.bufferManager.Size() == 0 {
						t.Errorf("フラッシュ%d: リトライ時にバッファがクリアされています", i+1)
					}
				}

				// 成功の場合、バッファはクリアされるべき
				if result == FlushResultSuccess {
					if plugin.context.bufferManager.Size() != 0 {
						t.Errorf("フラッシュ%d: 成功時にバッファがクリアされていません", i+1)
					}
				}
				
				// エラーの場合、performFlushではバッファは保持される（クリアは呼び出し元の責務）
				if result == FlushResultError {
					if plugin.context.bufferManager.Size() == 0 {
						t.Errorf("フラッシュ%d: エラー時にバッファが予期せずクリアされています", i+1)
					}
				}
				
				// 次のテストのためにデータを再追加（必要な場合）
				if i < len(tt.expectedFlushResults)-1 {
					if result == FlushResultSuccess || result == FlushResultError {
						plugin.context.bufferManager.Reset() // エラー時は手動でクリア
						plugin.context.bufferManager.AddRecord(testData)
					}
				}
			}

			// 書き込み回数の検証
			actualWriteCount := int(atomic.LoadInt32(&writeCount))
			if actualWriteCount != tt.expectedWriteCount {
				t.Errorf("期待される書き込み回数: %d, 実際: %d",
					tt.expectedWriteCount, actualWriteCount)
			}

			t.Logf("✅ %s", tt.description)
		})
	}
}

// TestRetryWithDataPersistence はリトライ時のデータ永続性をテスト
func TestRetryWithDataPersistence(t *testing.T) {
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

	// 書き込みデータを記録
	var writtenData []byte
	writeAttempt := 0

	plugin.context.storageClient = &MockStorageClient{
		writeFunc: func(bucket, objectKey string, data io.Reader) error {
			writeAttempt++
			// データを読み取って保存
			buf := new(bytes.Buffer)
			buf.ReadFrom(data)
			
			if writeAttempt == 1 {
				// 1回目：データを記録してエラーを返す
				writtenData = buf.Bytes()
				return errors.New("temporary failure")
			} else {
				// 2回目：データが同じかチェック
				if !bytes.Equal(writtenData, buf.Bytes()) {
					t.Error("リトライ時のデータが異なります")
				}
				return nil
			}
		},
	}

	// 初回データ追加
	originalData := []byte(`{"important": "data", "must_not": "be_lost"}`)
	plugin.context.bufferManager.AddRecord(originalData)

	// 1回目のフラッシュ（失敗）
	result1, _ := plugin.performFlush("test-tag")
	if result1 != FlushResultRetry {
		t.Errorf("1回目: FlushResultRetryが期待されましたが、%vでした", result1)
	}

	// バッファにデータが残っていることを確認
	if plugin.context.bufferManager.Size() == 0 {
		t.Fatal("エラー後にバッファが空になっています")
	}

	// 2回目のフラッシュ（成功）
	result2, _ := plugin.performFlush("test-tag")
	if result2 != FlushResultSuccess {
		t.Errorf("2回目: FlushResultSuccessが期待されましたが、%vでした", result2)
	}

	// バッファがクリアされたことを確認
	if plugin.context.bufferManager.Size() != 0 {
		t.Error("成功後にバッファがクリアされていません")
	}
}

// TestConcurrentRetryScenario は並行リトライシナリオをテスト
func TestConcurrentRetryScenario(t *testing.T) {
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

	// エラー制御用のチャネル
	errorControl := make(chan error, 10)
	errorControl <- errors.New("connection timeout")
	errorControl <- errors.New("service unavailable")
	errorControl <- nil // 3回目で成功

	plugin.context.storageClient = &MockStorageClient{
		writeFunc: func(bucket, objectKey string, data io.Reader) error {
			select {
			case err := <-errorControl:
				return err
			default:
				return nil
			}
		},
	}

	// 複数のレコードを追加
	records := [][]byte{
		[]byte(`{"record": 1, "data": "first"}`),
		[]byte(`{"record": 2, "data": "second"}`),
		[]byte(`{"record": 3, "data": "third"}`),
	}

	for _, record := range records {
		if err := plugin.context.bufferManager.AddRecord(record); err != nil {
			t.Fatalf("レコード追加失敗: %v", err)
		}
	}

	// リトライループのシミュレーション
	retryCount := 0
	maxRetries := 5

	for retryCount < maxRetries {
		result, err := plugin.performFlush("test-tag")
		
		switch result {
		case FlushResultSuccess:
			t.Logf("✅ %d回目の試行で成功", retryCount+1)
			return
		case FlushResultRetry:
			t.Logf("🔄 %d回目の試行: リトライ必要 (エラー: %v)", retryCount+1, err)
			retryCount++
		case FlushResultError:
			t.Fatalf("❌ 永続的エラーで失敗: %v", err)
		}
	}

	if retryCount >= maxRetries {
		t.Error("最大リトライ回数に到達しました")
	}
}

// TestRetryBackoffSimulation はリトライバックオフのシミュレーション
func TestRetryBackoffSimulation(t *testing.T) {
	// このテストはFluent Bitのリトライバックオフ動作をシミュレート
	// 実際のバックオフはFluent Bit側で制御される
	
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

	attemptTimes := []time.Time{}
	
	plugin.context.storageClient = &MockStorageClient{
		writeFunc: func(bucket, objectKey string, data io.Reader) error {
			attemptTimes = append(attemptTimes, time.Now())
			
			// 最初の3回は失敗、4回目で成功
			if len(attemptTimes) < 4 {
				return errors.New("503 Service Unavailable")
			}
			return nil
		},
	}

	// データ追加
	plugin.context.bufferManager.AddRecord([]byte(`{"test": "data"}`))

	// リトライシミュレーション（実際のFluent Bitでは自動的にバックオフが適用される）
	backoffDelays := []time.Duration{
		0,                     // 初回は即座
		1 * time.Millisecond,  // 1回目のリトライ
		2 * time.Millisecond,  // 2回目のリトライ（指数バックオフ）
		4 * time.Millisecond,  // 3回目のリトライ
	}

	for i, delay := range backoffDelays {
		if delay > 0 {
			time.Sleep(delay)
		}
		
		result, _ := plugin.performFlush("test-tag")
		
		if result == FlushResultSuccess {
			t.Logf("✅ %d回目の試行で成功（合計時間: %v）", 
				i+1, attemptTimes[len(attemptTimes)-1].Sub(attemptTimes[0]))
			break
		} else if result == FlushResultRetry {
			t.Logf("🔄 %d回目の試行: リトライ（遅延: %v）", i+1, delay)
			// バッファにデータを再追加（Fluent Bitが実際に行う動作）
			plugin.context.bufferManager.AddRecord([]byte(`{"test": "data"}`))
		}
	}

	if len(attemptTimes) != 4 {
		t.Errorf("期待される試行回数: 4, 実際: %d", len(attemptTimes))
	}
}

// TestErrorTypeClassification はエラータイプの分類をテスト
func TestErrorTypeClassification(t *testing.T) {
	testCases := []struct {
		name           string
		error          error
		expectedResult FlushResult
		description    string
	}{
		// ネットワーク関連（リトライ可能）
		{
			name:           "ネットワークタイムアウト",
			error:          fmt.Errorf("Post \"https://storage.googleapis.com/\": context deadline exceeded"),
			expectedResult: FlushResultRetry,
			description:    "ネットワークタイムアウトはリトライ可能",
		},
		{
			name:           "DNS解決エラー",
			error:          fmt.Errorf("dial tcp: lookup storage.googleapis.com: no such host"),
			expectedResult: FlushResultRetry,
			description:    "DNS解決エラーは一時的でリトライ可能",
		},
		
		// サービス関連（リトライ可能）
		{
			name:           "サービス利用不可",
			error:          fmt.Errorf("googleapi: Error 503: Service Unavailable"),
			expectedResult: FlushResultRetry,
			description:    "503エラーは一時的でリトライ可能",
		},
		{
			name:           "レート制限",
			error:          fmt.Errorf("googleapi: Error 429: Too Many Requests"),
			expectedResult: FlushResultRetry,
			description:    "レート制限は時間経過で解消される",
		},
		
		// 認証・権限関連（リトライ不可）
		{
			name:           "認証エラー",
			error:          fmt.Errorf("googleapi: Error 401: Invalid Credentials"),
			expectedResult: FlushResultError,
			description:    "認証エラーはリトライしても解決しない",
		},
		{
			name:           "権限不足",
			error:          fmt.Errorf("googleapi: Error 403: Insufficient permissions"),
			expectedResult: FlushResultError,
			description:    "権限エラーはリトライしても解決しない",
		},
		{
			name:           "バケット存在しない",
			error:          fmt.Errorf("googleapi: Error 404: Bucket not found"),
			expectedResult: FlushResultError,
			description:    "存在しないリソースはリトライしても解決しない",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
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

			plugin.context.storageClient = &MockStorageClient{
				writeFunc: func(bucket, objectKey string, data io.Reader) error {
					return tc.error
				},
			}

			// データ追加
			plugin.context.bufferManager.AddRecord([]byte(`{"test": "data"}`))

			// フラッシュ実行
			result, _ := plugin.performFlush("test-tag")

			if result != tc.expectedResult {
				t.Errorf("%s\n期待: %v, 実際: %v",
					tc.description, tc.expectedResult, result)
			}
		})
	}
}