package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestBufferManager_AddRecord はAddRecordメソッドのテスト
func TestBufferManager_AddRecord(t *testing.T) {
	// オーバーフローカウンター
	overflowCount := 0
	overflowCallback := func() {
		overflowCount++
	}

	// バッファ設定
	config := BufferConfig{
		MaxBufferSizeBytes: 1024, // 1KB
		FlushTimeoutSec:    60,
		AddTruncationMeta:  true,
	}

	bm := NewBufferManager(config, overflowCallback)

	// テストデータ
	record1 := createTestJSONRecord("test1", 1)
	record2 := createTestJSONRecord("test2", 2)

	// 正常に追加できることを確認
	if err := bm.AddRecord(record1); err != nil {
		t.Fatalf("Failed to add record1: %v", err)
	}

	if bm.Size() == 0 {
		t.Error("Buffer size should be greater than 0 after adding a record")
	}

	// 2つ目のレコードも正常に追加できることを確認
	if err := bm.AddRecord(record2); err != nil {
		t.Fatalf("Failed to add record2: %v", err)
	}

	// バッファが満杯になるまでレコードを追加しない
	if bm.IsFull() {
		t.Error("Buffer should not be full yet")
	}

	// オーバーフローが発生していないことを確認
	if overflowCount > 0 {
		t.Errorf("Overflow should not have occurred yet, but got %d", overflowCount)
	}
}

// TestBufferManager_Overflow はバッファ溢れ時の動作をテスト
func TestBufferManager_Overflow(t *testing.T) {
	// オーバーフローカウンター
	overflowCount := 0
	overflowCallback := func() {
		overflowCount++
	}

	// バッファ設定 - 小さなバッファを使用
	config := BufferConfig{
		MaxBufferSizeBytes: 400, // 400B
		FlushTimeoutSec:    60,
		AddTruncationMeta:  true,
	}

	bm := NewBufferManager(config, overflowCallback)

	// 大きいテストデータを生成（オーバーフローを引き起こす）
	var records [][]byte
	for i := 0; i < 10; i++ {
		// 各レコードは70-80バイト程度
		record := createTestJSONRecord(fmt.Sprintf("test%d", i), i)
		records = append(records, record)
	}

	// 最初のレコードを追加
	if err := bm.AddRecord(records[0]); err != nil {
		t.Fatalf("Failed to add first record: %v", err)
	}

	// オーバーフローするまでレコードを追加
	for i := 1; i < len(records); i++ {
		if err := bm.AddRecord(records[i]); err != nil {
			t.Fatalf("Failed to add record %d: %v", i, err)
		}
	}

	// オーバーフローが発生していることを確認
	if overflowCount == 0 {
		t.Error("Overflow should have occurred")
	}

	// バッファをフラッシュ
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}
	if data == nil {
		t.Fatal("Flushed data should not be nil")
	}

	// 行に分割
	lines := strings.Split(string(data), "\n")
	// 空行を削除
	var nonEmptyLines []string
	for _, line := range lines {
		if line != "" {
			nonEmptyLines = append(nonEmptyLines, line)
		}
	}

	// 各行が有効なJSONか確認
	for i, line := range nonEmptyLines {
		var jsonObj interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			t.Errorf("Line %d is not valid JSON: %v", i, err)
		}
	}

	// メタデータが含まれているか確認
	if config.AddTruncationMeta && len(nonEmptyLines) > 0 {
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(nonEmptyLines[0]), &meta); err != nil {
			t.Fatalf("Failed to parse first line as JSON: %v", err)
		}

		// メタデータが期待通りか確認
		if truncEvent, ok := meta["truncation_event"]; ok {
			if !truncEvent.(bool) {
				t.Error("truncation_event should be true")
			}
		} else {
			t.Error("truncation_event field not found in metadata")
		}
	}
}

// TestBufferManager_JSONIntegrity バッファ切り詰め時にJSON整合性が保たれることを確認
func TestBufferManager_JSONIntegrity(t *testing.T) {
	// バッファ設定
	config := BufferConfig{
		MaxBufferSizeBytes: 500, // 小さなバッファを使用
		FlushTimeoutSec:    60,
		AddTruncationMeta:  true,
	}

	bm := NewBufferManager(config, nil)

	// バッファに直接不正なJSONと正常なJSONを追加
	bm.buffer.Write([]byte(`{"name":"valid1","value":1}`))
	bm.buffer.Write([]byte("\n"))
	bm.buffer.Write([]byte(`{"name":"invalid","value":4`)) // 閉じカッコがない不正なJSON
	bm.buffer.Write([]byte("\n"))
	bm.buffer.Write([]byte(`{"name":"valid2","value":2}`))
	bm.buffer.Write([]byte("\n"))
	bm.buffer.Write([]byte(`{"name":"valid3","value":3}`))
	bm.buffer.Write([]byte("\n"))
	bm.currentSize = bm.buffer.Len()

	// バッファを切り詰め
	err := bm.truncateByLine()
	if err != nil {
		t.Fatalf("Failed to truncate buffer: %v", err)
	}

	// バッファをフラッシュ
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}

	// 行に分割
	lines := strings.Split(string(data), "\n")
	// 空行を削除
	var nonEmptyLines []string
	for _, line := range lines {
		if line != "" {
			nonEmptyLines = append(nonEmptyLines, line)
		}
	}

	// 各行が有効なJSONかチェック
	for i, line := range nonEmptyLines {
		var jsonObj interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			t.Errorf("Line %d is not valid JSON after truncation: %v", i, err)
			t.Logf("Invalid JSON content: %s", line)
		}
	}

	// 有効なJSONデータが含まれていることを確認
	foundValid1 := false
	foundValid2 := false
	foundValid3 := false
	foundInvalid := false

	for _, line := range nonEmptyLines {
		var jsonObj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			continue
		}

		if name, ok := jsonObj["name"]; ok {
			switch name {
			case "valid1":
				foundValid1 = true
			case "valid2":
				foundValid2 = true
			case "valid3":
				foundValid3 = true
			case "invalid":
				foundInvalid = true
			}
		}
	}

	// 不正なJSONは含まれていないはず
	if foundInvalid {
		t.Error("Invalid JSON should have been removed by truncation")
	}

	// 全ての有効なJSONが含まれているわけではない（切り詰めが起こった可能性がある）
	// しかし、最低1つの有効なJSONは存在すべき
	if !(foundValid1 || foundValid2 || foundValid3) {
		t.Error("At least one valid JSON should exist in the buffer")
	}
}

// TestBufferManager_AddInvalidJSON 追加時のJSON検証動作を確認
func TestBufferManager_AddInvalidJSON(t *testing.T) {
	// バッファ設定
	config := BufferConfig{
		MaxBufferSizeBytes: 1024,
		FlushTimeoutSec:    60,
	}

	bm := NewBufferManager(config, nil)

	// 有効なJSONを追加
	validJSON := []byte(`{"name":"valid","value":1}`)
	err := bm.AddRecord(validJSON)
	if err != nil {
		t.Fatalf("Failed to add valid JSON: %v", err)
	}

	// 不正なJSONを追加
	invalidJSON := []byte(`{"name":"invalid","value":4`) // 閉じカッコがない
	err = bm.AddRecord(invalidJSON)
	if err != nil {
		t.Fatalf("Failed to add invalid JSON: %v", err)
	}

	// バッファをフラッシュ
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}

	// 行に分割
	lines := strings.Split(string(data), "\n")
	// 空行を削除
	var nonEmptyLines []string
	for _, line := range lines {
		if line != "" {
			nonEmptyLines = append(nonEmptyLines, line)
		}
	}

	// 全行の確認
	t.Logf("Found %d non-empty lines in buffer", len(nonEmptyLines))
	for i, line := range nonEmptyLines {
		t.Logf("Line %d: %s", i, line)
		var jsonObj interface{}
		err := json.Unmarshal([]byte(line), &jsonObj)
		if err != nil {
			t.Logf("Line %d is not valid JSON: %v", i, err)
		}
	}

	// 注意: 現在の実装では入力検証を行っていないため、不正なJSONもバッファに追加される
	// このテストは現状の動作を確認するためのもので、将来的には入力検証機能を追加するべき
	t.Log("Note: Current implementation does not validate input JSON. Future enhancement could include input validation.")
}

// TestBufferManager_TruncateInvalidJSON は不正なJSONの切り詰めテスト
func TestBufferManager_TruncateInvalidJSON(t *testing.T) {
	// バッファ設定
	config := BufferConfig{
		MaxBufferSizeBytes: 1024, // 十分な大きさ
		FlushTimeoutSec:    60,
	}

	bm := NewBufferManager(config, nil)

	// バッファに直接不正なJSONデータを追加（通常の操作ではあり得ない）
	bm.buffer.Write([]byte(`{"valid":true}`))
	bm.buffer.Write([]byte("\n"))
	bm.buffer.Write([]byte(`{"invalid":true`)) // 閉じカッコがない
	bm.buffer.Write([]byte("\n"))
	bm.buffer.Write([]byte(`{"also_valid":true}`))
	bm.buffer.Write([]byte("\n"))
	bm.currentSize = bm.buffer.Len()

	// truncateByLineを実行
	if err := bm.truncateByLine(); err != nil {
		t.Fatalf("Failed to truncate buffer: %v", err)
	}

	// バッファをフラッシュして内容を確認
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}

	// 行に分割
	lines := strings.Split(string(data), "\n")
	// 空行を削除
	var nonEmptyLines []string
	for _, line := range lines {
		if line != "" {
			nonEmptyLines = append(nonEmptyLines, line)
		}
	}

	// 各行が有効なJSONかチェック
	for i, line := range nonEmptyLines {
		var jsonObj interface{}
		if err := json.Unmarshal([]byte(line), &jsonObj); err != nil {
			t.Errorf("Line %d after truncation is not valid JSON: %v", i, err)
			t.Logf("Invalid JSON content: %s", line)
		}
	}

	// 不正なJSONが含まれていないことを確認
	foundInvalid := false
	for _, line := range nonEmptyLines {
		if strings.Contains(line, `{"invalid":true`) {
			foundInvalid = true
			break
		}
	}

	if foundInvalid {
		t.Error("Invalid JSON should have been removed by truncation")
	}
}

// TestBufferManager_MetadataAddition はメタデータ追加機能をテスト
func TestBufferManager_MetadataAddition(t *testing.T) {
	// メタデータありの設定
	configWithMeta := BufferConfig{
		MaxBufferSizeBytes: 300, // 小さめのバッファ
		FlushTimeoutSec:    60,
		AddTruncationMeta:  true,
	}

	// メタデータなしの設定
	configWithoutMeta := BufferConfig{
		MaxBufferSizeBytes: 300,
		FlushTimeoutSec:    60,
		AddTruncationMeta:  false,
	}

	testWithMetadata := func(config BufferConfig, expectMeta bool) {
		bm := NewBufferManager(config, nil)

		// バッファをオーバーフローさせるデータを追加
		for i := 0; i < 10; i++ {
			record := createTestJSONRecord(fmt.Sprintf("test%d", i), i)
			if err := bm.AddRecord(record); err != nil {
				t.Fatalf("Failed to add record: %v", err)
			}
		}

		// フラッシュ
		data, err := bm.Flush()
		if err != nil {
			t.Fatalf("Failed to flush buffer: %v", err)
		}

		// 行に分割
		lines := strings.Split(string(data), "\n")
		var nonEmptyLines []string
		for _, line := range lines {
			if line != "" {
				nonEmptyLines = append(nonEmptyLines, line)
			}
		}

		if len(nonEmptyLines) == 0 {
			t.Fatal("No data in buffer after flush")
		}

		// 最初の行がメタデータか確認
		var firstLine map[string]interface{}
		if err := json.Unmarshal([]byte(nonEmptyLines[0]), &firstLine); err != nil {
			t.Fatalf("Failed to parse first line as JSON: %v", err)
		}

		hasMeta := false
		if _, ok := firstLine["truncation_event"]; ok {
			hasMeta = true
		}

		if expectMeta && !hasMeta {
			t.Error("Expected metadata but none found")
		}
		if !expectMeta && hasMeta {
			t.Error("Did not expect metadata but found it")
		}
	}

	t.Run("WithMetadata", func(t *testing.T) {
		testWithMetadata(configWithMeta, true)
	})

	t.Run("WithoutMetadata", func(t *testing.T) {
		testWithMetadata(configWithoutMeta, false)
	})
}

// TestBufferManager_Reset はResetメソッドのテスト
func TestBufferManager_Reset(t *testing.T) {
	config := BufferConfig{
		MaxBufferSizeBytes: 1024,
		FlushTimeoutSec:    60,
	}

	bm := NewBufferManager(config, nil)

	// データを追加
	record := createTestJSONRecord("test", 1)
	if err := bm.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	initialSize := bm.Size()
	if initialSize == 0 {
		t.Fatal("Buffer size should be greater than 0 after adding data")
	}

	// Flushしてもサイズは変わらないはず
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}
	if data == nil {
		t.Fatal("Flushed data should not be nil")
	}
	if bm.Size() != initialSize {
		t.Errorf("Buffer size changed after flush: expected %d, got %d", initialSize, bm.Size())
	}

	// リセット後はサイズが0になるはず
	bm.Reset()
	if bm.Size() != 0 {
		t.Errorf("Buffer size should be 0 after reset, got %d", bm.Size())
	}
}

// TestBufferManager_ShouldFlush はタイムアウトベースのフラッシュをテスト
func TestBufferManager_ShouldFlush(t *testing.T) {
	// 短いタイムアウトを設定
	config := BufferConfig{
		MaxBufferSizeBytes: 1024,
		FlushTimeoutSec:    1, // 1秒
	}

	bm := NewBufferManager(config, nil)

	// データを追加
	record := createTestJSONRecord("test", 1)
	if err := bm.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// 初期状態ではフラッシュすべきでない
	if bm.ShouldFlush() {
		t.Error("Buffer should not require flush immediately after adding data")
	}

	// 一定時間後にフラッシュすべき状態になる
	time.Sleep(1100 * time.Millisecond) // タイムアウト+余裕
	if !bm.ShouldFlush() {
		t.Error("Buffer should require flush after timeout")
	}

	// フラッシュしても状態は変わらない
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush buffer: %v", err)
	}
	if data == nil {
		t.Fatal("Flushed data should not be nil")
	}

	// リセット後はフラッシュすべきでない
	bm.Reset()
	if bm.ShouldFlush() {
		t.Error("Buffer should not require flush after reset")
	}
}

// エッジケース: バッファが空の状態
func TestBufferManager_EmptyBuffer(t *testing.T) {
	config := BufferConfig{
		MaxBufferSizeBytes: 1024,
		FlushTimeoutSec:    60,
	}

	bm := NewBufferManager(config, nil)

	// 何も追加せずにフラッシュ
	data, err := bm.Flush()
	if err != nil {
		t.Fatalf("Failed to flush empty buffer: %v", err)
	}
	if data != nil {
		t.Errorf("Flushed data from empty buffer should be nil, got %v", data)
	}

	// 何も追加せずにTruncateByLine
	err = bm.truncateByLine()
	if err != nil {
		t.Fatalf("Failed to truncate empty buffer: %v", err)
	}
}

// ヘルパー関数: テスト用のJSONレコードを作成
func createTestJSONRecord(name string, value int) []byte {
	record := map[string]interface{}{
		"name":      name,
		"value":     value,
		"timestamp": time.Now().Format(time.RFC3339),
		"data":      strings.Repeat("x", 20), // 少し大きめのデータ
	}
	
	data, _ := json.Marshal(record)
	return data
}