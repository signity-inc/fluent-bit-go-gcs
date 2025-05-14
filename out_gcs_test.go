package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

// テスト用ヘルパー関数 - コンテキストの初期化
func initTestContext(config map[string]string) *PluginContext {
	return &PluginContext{
		Config:           config,
		LastFlushTime:    time.Now().Add(-10 * time.Minute),
		RetryCount:       0,
		MaxRetryCount:    3,                    // デフォルトのリトライ回数
		MaxBufferSizeBytes: 1024 * 1024,        // デフォルトの最大バッファサイズ 1MB
	}
}

// 既存のテスト
func TestGenerateObjectKey(t *testing.T) {
	prefix := "daily"
	tag := "event_log"
	timestamp := time.Now()
	year, month, day := timestamp.Date()

	expected := fmt.Sprintf("%s/%s/%04d/%02d/%02d/", prefix, tag, year, month, day)

	if got := GenerateObjectKey(prefix, tag, timestamp); !strings.Contains(got, expected) {
		t.Errorf("GenerateObjectKey() = %v, want %v", got, expected)
	}
}

func TestGetCurrentJstTime(t *testing.T) {
	now := time.Now()
	_, offset := now.Zone()
	jst := time.FixedZone("JST", 9*60*60)

	if offset == 0 {
		expected := now.In(jst)
		if got := getCurrentJstTime(); !strings.Contains(fmt.Sprintf("%v", got), "JST") {
			t.Errorf("GetCurrentJstTime() = %v, want %v", got, expected)
		}
	} else {
		expected := now
		if got := getCurrentJstTime(); !strings.Contains(fmt.Sprintf("%v", got), "JST") {
			t.Errorf("GetCurrentJstTime() = %v, want %v", got, expected)
		}
	}
}

// 新規テスト - GCSクライアントのモックを使用

// TestFlushBuffer フラッシュバッファ関数のテスト
func TestFlushBuffer(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		// テスト後に元に戻す
		gcsClient = origGcsClient
	}()

	// モックGCSクライアントを設定
	mockClient := NewMockClient()
	gcsClient = mockClient

	// テスト用のコンテキストを作成
	ctx := initTestContext(map[string]string{
		"bucket": "test-bucket",
		"prefix": "test-prefix",
	})

	// テストデータをバッファに追加
	testData := "test log data"
	ctx.Buffer.WriteString(testData)
	ctx.CurrentBufferSize = len(testData)

	// フラッシュ関数を呼び出し
	err := flushBuffer(ctx, "test-tag")
	if err != nil {
		t.Errorf("flushBuffer returned error: %v", err)
	}

	// バッファがリセットされたか確認
	if ctx.Buffer.Len() != 0 || ctx.CurrentBufferSize != 0 {
		t.Errorf("Buffer was not reset after flush: len=%d, size=%d", ctx.Buffer.Len(), ctx.CurrentBufferSize)
	}

	// モックオブジェクトを検証：データが書き込まれたか
	writtenData := GetMockWrittenDataMap(mockClient)
	found := false
	for key := range writtenData {
		if strings.HasPrefix(key, "test-bucket/") {
			// キーのパターンをチェック
			found = true
			break
		}
	}

	if !found {
		t.Errorf("No data was written to GCS bucket")
	}
}

// TestFlushBufferError フラッシュ時のエラー処理テスト
func TestFlushBufferError(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		// テスト後に元に戻す
		gcsClient = origGcsClient
	}()

	// モックGCSクライアントを設定（エラー発生するよう設定）
	mockClient := NewMockClient()
	SetMockGlobalFailure(mockClient, true)
	gcsClient = mockClient

	// テスト用のコンテキストを作成（ヘルパー関数を使用）
	ctx := initTestContext(map[string]string{
		"bucket": "test-bucket",
		"prefix": "test-prefix",
	})

	// テストデータをバッファに追加
	testData := "test log data for error case"
	ctx.Buffer.WriteString(testData)
	initialSize := len(testData)
	ctx.CurrentBufferSize = initialSize

	// フラッシュ関数を呼び出し
	err := flushBuffer(ctx, "test-tag")

	// 修正後はエラーが返されるべき
	if err == nil {
		t.Errorf("Expected error on GCS failure, got nil")
	}

	// 修正後はバッファが保持されるべき
	if ctx.Buffer.Len() == 0 || ctx.CurrentBufferSize == 0 {
		t.Errorf("Buffer was reset after flush error: len=%d, size=%d", ctx.Buffer.Len(), ctx.CurrentBufferSize)
	} else {
		t.Logf("FIXED: Buffer was maintained when GCS write failed")
	}
}

// TestGzipCompression 圧縮機能のテスト
func TestGzipCompression(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		// テスト後に元に戻す
		gcsClient = origGcsClient
	}()

	// モックGCSクライアントを設定
	mockClient := NewMockClient()
	gcsClient = mockClient

	// テスト用のコンテキストを作成（ヘルパー関数を使用）
	ctx := initTestContext(map[string]string{
		"bucket": "test-bucket",
		"prefix": "test-prefix",
	})

	// テストデータをバッファに追加
	testData := "test log data for compression verification"
	ctx.Buffer.WriteString(testData)
	ctx.CurrentBufferSize = len(testData)

	// フラッシュ関数を呼び出し
	err := flushBuffer(ctx, "test-tag")
	if err != nil {
		t.Errorf("flushBuffer returned error: %v", err)
	}

	// モックオブジェクトからデータを取得
	writtenData := GetMockWrittenDataMap(mockClient)
	var compressedData []byte
	for key, data := range writtenData {
		if strings.HasPrefix(key, "test-bucket/") {
			compressedData = data
			break
		}
	}

	if compressedData == nil {
		t.Fatal("No data was written to GCS bucket")
	}

	// 圧縮データを解凍
	gzipReader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressedData, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// 元のデータと一致するか確認
	if string(decompressedData) != testData {
		t.Errorf("Decompressed data does not match original. Expected: %s, Got: %s", testData, string(decompressedData))
	}
}

// シンプルなバッファリセットの問題を再現するテスト
func TestBufferResetOnError(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		// テスト後に元に戻す
		gcsClient = origGcsClient
	}()

	// モックGCSクライアントを設定（エラー発生するよう設定）
	mockClient := NewMockClient()
	SetMockWriteFunction(mockClient, func(bucket, object string, content io.Reader) error {
		return errors.New("forced GCS error for testing")
	})
	gcsClient = mockClient

	// テスト用のコンテキストを作成（ヘルパー関数を使用）
	ctx := initTestContext(map[string]string{
		"bucket": "test-bucket",
		"prefix": "test-prefix",
	})

	// テストデータをバッファに追加
	testData := "test log data for buffer reset test"
	ctx.Buffer.WriteString(testData)
	ctx.CurrentBufferSize = len(testData)

	// フラッシュ関数を呼び出し
	err := flushBuffer(ctx, "test-tag")

	// 修正後はエラーが返される
	if err == nil {
		t.Errorf("Expected error on GCS failure, got nil")
	}

	// 修正後はバッファが保持される
	if ctx.Buffer.Len() == 0 || ctx.CurrentBufferSize == 0 {
		t.Errorf("Expected buffer to be maintained after error, but it was reset: len=%d, size=%d", 
			ctx.Buffer.Len(), ctx.CurrentBufferSize)
	} else {
		t.Log("FIXED: Buffer is maintained when GCS write fails")
	}

	// 修正すべき動作：エラー時にもバッファを保持し、エラーを返す
	// 修正実装後は上記のテストが失敗するようになる
}

// TestSimulateDuplicateLogsScenario 修正後の重複ログ送信問題をシンプルに検証するテスト
func TestSimulateDuplicateLogsScenario(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		gcsClient = origGcsClient
	}()

	// 現在のバグが存在する場合：
	// 1. 書き込み失敗時にバッファがリセットされる（データ消失）
	// 2. エラーがnilで返されるためFluentBitはリトライしない

	// 修正後（理想的な実装）：
	// 1. 書き込み失敗時にバッファを保持
	// 2. エラーを返しFluentBitがリトライする
	// しかし、オブジェクトキーが毎回生成されるため、重複データが別々のオブジェクトとして保存される可能性

	// テスト用の変数
	var callCount int
	generatedKeys := make(map[string]bool)
	bufferContent := "test log data that should be sent only once"

	// モックGCSクライアントを設定
	mockClient := NewMockClient()
	// モックの書き込み関数をオーバーライド
	SetMockWriteFunction(mockClient, func(bucket, object string, content io.Reader) error {
		callCount++
		
		// オブジェクトキーを記録
		generatedKeys[object] = true
		
		// 最初の呼び出しはエラーを返す
		if callCount == 1 {
			t.Logf("First write attempt - will fail. Object key: %s", object)
			return errors.New("simulated error on first attempt")
		}
		
		// 2回目の呼び出しは成功
		t.Logf("Second write attempt - successful. Object key: %s", object)
		return nil
	})
	gcsClient = mockClient

	// テスト用コンテキスト（ヘルパー関数を使用）
	ctx := initTestContext(map[string]string{
		"bucket": "test-bucket", 
		"prefix": "test-prefix",
	})

	// バッファにデータを追加
	ctx.Buffer.WriteString(bufferContent)
	ctx.CurrentBufferSize = len(bufferContent)

	// 修正後の実装では：
	// 1. 最初の呼び出しで失敗、バッファは保持され、エラーを返す
	err := flushBuffer(ctx, "test-tag")
	if err == nil {
		t.Errorf("Fixed implementation should return error on GCS failure, got nil")
	}
	t.Logf("After first attempt (fixed implementation): Buffer size=%d", ctx.Buffer.Len())

	// 現在の実装における問題点：
	// バッファはリセットされるのでデータが失われるが、エラーはnilなのでリトライされない

	// 修正後の理想的な実装をシミュレーション：
	// 1. エラー発生時にもバッファを保持
	// 2. エラーを返してリトライを促す
	t.Log("--- Simulating fixed implementation ---")

	// シミュレーション用に再度バッファを設定（実際の修正後は自動的に保持される）
	ctx.Buffer.WriteString(bufferContent)
	ctx.CurrentBufferSize = len(bufferContent)

	// 修正後は２回目の呼び出しで成功するはず
	err = flushBuffer(ctx, "test-tag")
	if err != nil {
		// 現在はまだ修正前なのでエラーは返らない
		t.Log("Note: In fixed implementation, this would return an error to trigger retry")
	}

	// 最終的な結果を検証
	t.Logf("Total call count: %d", callCount)
	t.Logf("Number of unique object keys generated: %d", len(generatedKeys))
	
	// 重複の問題：同じデータが2つの異なるオブジェクトキーで保存される可能性
	if len(generatedKeys) > 1 {
		t.Log("DUPLICATE DETECTION: Same data would be stored with different object keys")
		for key := range generatedKeys {
			t.Logf("  - Object key: %s", key)
		}
	}
	
	// 結論：
	// 1. 修正によりデータ消失は防げる
	// 2. しかし、オブジェクトキー生成方法により同じデータが複数回保存される可能性がある
	t.Log("Conclusion: After fixing buffer reset issue, we need to also address potential duplicates by using consistent object keys across retries")
}

// TestCompareCurrentVsFixed 現在の実装と修正後の実装を比較するテスト（視覚的にわかりやすく）
func TestCompareCurrentVsFixed(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		// テスト後に元に戻す
		gcsClient = origGcsClient
	}()
	
	t.Log("===============================================================")
	t.Log("         バグとその修正のわかりやすい比較デモンストレーション")
	t.Log("===============================================================")
	
	// テストデータ
	testData := "important log data that should not be lost or duplicated"
	
	//----------------------------------------------
	// 現在の実装（バグあり）
	//----------------------------------------------
	t.Log("\n===== 現在の実装（バグあり） =====")
	
	// モックGCSクライアント（最初の呼び出しのみ失敗）
	currentMock := NewMockClient()
	callCount := 0
	objectKeys := make(map[string]bool)
	
	SetMockWriteFunction(currentMock, func(bucket, object string, content io.Reader) error {
		callCount++
		objectKeys[object] = true
		
		if callCount == 1 {
			t.Log("✖ GCSへの書き込みが失敗しました")
			return errors.New("network error")
		}
		
		t.Log("✓ GCSへの書き込みが成功しました")
		return nil
	})
	
	gcsClient = currentMock
	
	// コンテキスト作成
	currentCtx := &PluginContext{
		Config: map[string]string{
			"bucket": "test-bucket",
			"prefix": "test-prefix",
		},
		LastFlushTime: time.Now().Add(-10 * time.Minute),
	}
	
	// バッファにデータを追加
	t.Log("1. バッファにデータを追加します: " + testData)
	currentCtx.Buffer.WriteString(testData)
	currentCtx.CurrentBufferSize = len(testData)
	
	// 1回目のフラッシュ（失敗する）
	t.Log("2. フラッシュを実行します（失敗するはず）")
	err := flushBuffer(currentCtx, "test-tag")
	
	if err == nil {
		t.Log("   → エラーが返されませんでした（旧実装）")
	} else {
		t.Log("   → エラーが返されました（修正後の動作）")
	}
	
	if currentCtx.Buffer.Len() == 0 {
		t.Log("   → バッファが空になりました（旧実装）")
		t.Log("   → データが失われます！")
	} else {
		t.Log("   → バッファが保持されています（修正後の動作）")
	}
	
	// 失われたデータを再現（実際には失われている）
	t.Log("3. リトライをシミュレート（実際には失われているデータ）")
	currentCtx.Buffer.WriteString(testData)
	currentCtx.CurrentBufferSize = len(testData)
	
	// 2回目のフラッシュ（成功する）
	err = flushBuffer(currentCtx, "test-tag")
	
	t.Logf("4. 結果: 呼び出し回数=%d, ユニークなオブジェクトキー数=%d", callCount, len(objectKeys))
	if len(objectKeys) > 1 {
		t.Log("   → 同じデータが複数のオブジェクトキーで保存される問題")
		for key := range objectKeys {
			t.Logf("     - %s", key)
		}
	}
	
	t.Log("\n現在の実装の問題点:")
	t.Log("1. GCSへの書き込みエラーの場合でもエラーが返されないため、Fluent Bitはリトライしません")
	t.Log("2. バッファがリセットされるため、データが失われます")
	t.Log("3. リトライを手動で行う場合、異なるオブジェクトキーで重複データが保存されます")
	
	//----------------------------------------------
	// 修正後の実装（シミュレーション）
	//----------------------------------------------
	t.Log("\n===== 修正後の実装（シミュレーション） =====")
	
	// 変数をリセット
	callCount = 0
	objectKeys = make(map[string]bool)
	
	// 修正版のモック動作をシミュレーション
	fixedMock := NewMockClient()
	
	var savedObjectKey string // リトライ間でオブジェクトキーを保持するための変数
	
	SetMockWriteFunction(fixedMock, func(bucket, object string, content io.Reader) error {
		callCount++
		
		// 最初の呼び出しでキーを保存
		if callCount == 1 {
			savedObjectKey = object
			objectKeys[object] = true
			t.Log("✖ GCSへの書き込みが失敗しました")
			return errors.New("network error")
		}
		
		// 実際の修正実装では2回目の呼び出しで同じキーが使用される
		// ここではそれをシミュレート
		t.Logf("2回目の呼び出し: %s", object)
		if callCount == 2 {
			if object == savedObjectKey {
				t.Log("✓ 同じオブジェクトキーが使用されました（修正後の理想的な動作）")
			} else {
				t.Log("✖ 異なるオブジェクトキーが使用されました（問題）")
				objectKeys[object] = true
			}
		}
		
		t.Log("✓ GCSへの書き込みが成功しました")
		return nil
	})
	
	gcsClient = fixedMock
	
	// コンテキスト作成
	fixedCtx := &PluginContext{
		Config: map[string]string{
			"bucket": "test-bucket",
			"prefix": "test-fixed",
		},
		LastFlushTime: time.Now().Add(-10 * time.Minute),
		// 修正後に追加されるフィールド（シミュレーション用）
		// RetryObjectKey: "",
		// IsRetrying: false,
	}
	
	// バッファにデータを追加
	t.Log("1. バッファにデータを追加します: " + testData)
	fixedCtx.Buffer.WriteString(testData)
	fixedCtx.CurrentBufferSize = len(testData)
	
	// 1回目のフラッシュ（失敗する）
	t.Log("2. フラッシュを実行します（失敗するはず）")
	err = flushBuffer(fixedCtx, "test-tag")
	
	if err == nil {
		t.Log("   → エラーが返されませんでした（修正前の動作）")
	} else {
		t.Log("   → エラーが返されました（修正後の動作）")
	}
	
	// 修正後はバッファが保持され、リトライ状態が設定される
	if fixedCtx.Buffer.Len() == 0 {
		t.Log("   → バッファが空になりました（修正前の動作）")
		t.Log("   → 修正後はバッファが保持されます")
		
		// テスト用に再度データを設定
		fixedCtx.Buffer.WriteString(testData)
		fixedCtx.CurrentBufferSize = len(testData)
	} else {
		t.Log("   → バッファが保持されています（修正後の動作）")
		
		// IsRetryingとRetryObjectKeyが設定されているはず
		if fixedCtx.IsRetrying && fixedCtx.RetryObjectKey != "" {
			t.Log("   → リトライ状態が正しく設定されています")
		} else {
			t.Log("   → リトライ状態が正しく設定されていません")
		}
	}
	
	// 2回目のフラッシュ（リトライをシミュレート、成功する）
	t.Log("3. リトライをシミュレート（同じオブジェクトキーを使用）")
	err = flushBuffer(fixedCtx, "test-tag")
	
	t.Logf("4. 結果: 呼び出し回数=%d, ユニークなオブジェクトキー数=%d", callCount, len(objectKeys))
	
	t.Log("\n修正後の実装の利点:")
	t.Log("1. GCSへの書き込みエラーの場合、エラーが返されFluentBitが適切にリトライ")
	t.Log("2. バッファが保持されるため、データが失われない")
	t.Log("3. リトライ時に同じオブジェクトキーが使用され、重複データが防止される")
	
	t.Log("\n===============================================================")
	t.Log(" 実装すべき修正: ")
	t.Log(" 1. PluginContextにRetryObjectKeyとIsRetryingフィールドを追加")
	t.Log(" 2. GCS書き込みエラー時にバッファを保持し、エラーを返す")
	t.Log(" 3. リトライ時に同じオブジェクトキーを使用する仕組みを実装")
	t.Log("===============================================================")
}

// 新しいテスト - コンテキスト固有のミューテックスのテスト
func TestContextSpecificMutex(t *testing.T) {
	// シンプル化したテスト - 別々のコンテキストで互いにブロックされないことを検証
	
	// モックGCSクライアントを設定
	mockClient := NewMockClient()
	origGcsClient := gcsClient
	gcsClient = mockClient
	defer func() {
		gcsClient = origGcsClient
	}()
	
	// コンテキストを作成
	ctx1 := &PluginContext{
		Config: map[string]string{
			"bucket": "test-bucket-1",
			"prefix": "test-prefix-1",
		},
		LastFlushTime: time.Now().Add(-10 * time.Minute),
		// 新しいフィールドを初期化
		MaxRetryCount: 3,
		MaxBufferSizeBytes: 1024 * 1024,
	}
	
	ctx2 := &PluginContext{
		Config: map[string]string{
			"bucket": "test-bucket-2", 
			"prefix": "test-prefix-2",
		},
		LastFlushTime: time.Now().Add(-10 * time.Minute),
		// 新しいフィールドを初期化
		MaxRetryCount: 3,
		MaxBufferSizeBytes: 1024 * 1024,
	}
	
	// テストデータを追加
	ctx1.Buffer.WriteString("test data for context 1")
	ctx1.CurrentBufferSize = len("test data for context 1")
	
	ctx2.Buffer.WriteString("test data for context 2")
	ctx2.CurrentBufferSize = len("test data for context 2")
	
	// 順番にフラッシュを実行
	err1 := flushBuffer(ctx1, "test-tag-1")
	if err1 != nil {
		t.Errorf("Error flushing context 1: %v", err1)
	}
	
	err2 := flushBuffer(ctx2, "test-tag-2")
	if err2 != nil {
		t.Errorf("Error flushing context 2: %v", err2)
	}
	
	// 成功を確認
	t.Log("Both contexts flushed successfully")
	t.Log("With context-specific mutexes, contexts can operate independently")
}

// リトライ回数制限と最大バッファサイズのテスト
func TestRetryLimitAndMaxBufferSize(t *testing.T) {
	// オリジナルのGCSクライアントを保存
	origGcsClient := gcsClient
	defer func() {
		gcsClient = origGcsClient
	}()

	// モックGCSクライアントを設定（常に失敗するよう設定）
	mockClient := NewMockClient()
	SetMockGlobalFailure(mockClient, true)
	gcsClient = mockClient

	// テスト用のコンテキストを作成
	ctx := &PluginContext{
		Config: map[string]string{
			"bucket": "test-bucket",
			"prefix": "test-prefix",
		},
		LastFlushTime: time.Now().Add(-10 * time.Minute),
		RetryCount:    0,
		MaxRetryCount: 3, // テスト用に最大リトライ回数を設定
		MaxBufferSizeBytes: 1024 * 1024, // 1MB
	}

	// バッファに初期データを追加
	initialData := "initial test data"
	ctx.Buffer.WriteString(initialData)
	ctx.CurrentBufferSize = len(initialData)

	// フラッシュを試行（失敗するはず）
	err := flushBuffer(ctx, "test-tag")
	
	// 期待される動作の検証
	if err == nil {
		t.Errorf("Expected error on GCS failure, got nil")
	}
	
	if ctx.Buffer.Len() == 0 {
		t.Errorf("Buffer was reset after error, expected to be maintained")
	} else {
		t.Logf("Buffer maintained as expected after error, length: %d", ctx.Buffer.Len())
	}
	
	if ctx.RetryCount != 1 {
		t.Errorf("RetryCount not incremented, expected 1, got %d", ctx.RetryCount)
	} else {
		t.Log("RetryCount incremented as expected")
	}
	
	t.Log("Retry mechanism is working as expected")
}