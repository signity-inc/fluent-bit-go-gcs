package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// MetricsOutput JSONファイル出力用の構造体（metrics_proposal.mdから）
type MetricsOutput struct {
	Timestamp           string                     `json:"timestamp"`
	SuccessRate         float64                    `json:"success_rate_percent"`
	TotalLogs           int64                      `json:"total_logs"`
	TotalBytes          int64                      `json:"total_bytes"`
	BufferUsage         float64                    `json:"buffer_usage_percent"`
	AvgWriteLatency     string                     `json:"avg_write_latency_ms"`
	AvgCompressionRatio float64                    `json:"avg_compression_ratio"`
	Retries             int64                      `json:"retry_attempts"`
	MaxRetriesReached   int64                      `json:"max_retries_reached"`
	BufferOverflows     int64                      `json:"buffer_overflows"`
	ErrorsByType        map[string]int64           `json:"errors_by_type"`
	TagStats            map[string]TestOutputTagStat `json:"tag_stats"`
}

// TestOutputTagStat タグごとの統計情報
type TestOutputTagStat struct {
	LogCount       int64   `json:"log_count"`
	BytesProcessed int64   `json:"bytes_processed"`
	SuccessRate    float64 `json:"success_rate_percent"`
}

// テスト用のGcsMetrics構造体（実際のアプリケーションと同等のもの）
type GcsMetrics struct {
	// カウンターメトリクス
	TotalLogs         int64 // 受信した総ログ数
	TotalBytes        int64 // 受信した総バイト数
	SuccessWrites     int64 // 成功書き込み数
	FailedWrites      int64 // 失敗書き込み数
	RetryAttempts     int64 // リトライ試行数
	MaxRetriesReached int64 // 最大リトライ到達回数
	BufferOverflows   int64 // バッファオーバーフロー回数

	// ゲージメトリクス
	CurrentBufferSize int64   // 現在のバッファサイズ
	BufferUtilization float64 // バッファ使用率

	// ヒストグラムメトリクス
	WriteLatencies    []time.Duration // 書き込み遅延時間の履歴
	CompressionRatios []float64       // 圧縮率の履歴

	// エラー詳細
	ErrorCounts map[string]int64 // エラータイプごとの発生数

	// タグ別統計
	TagStats map[string]*TestTagStatInternal
}

// TestTagStatInternal は内部的に使用するタグ統計構造体
type TestTagStatInternal struct {
	LogCount       int64
	BytesProcessed int64
	SuccessWrites  int64
	FailedWrites   int64
}

// TestMetricsOutput はメトリクスのJSONファイル出力をテストする
func TestMetricsOutput(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "gcs_metrics_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト完了後にディレクトリを削除

	// テスト用のメトリクスデータを準備
	metrics := createTestMetrics()

	// メトリクス出力処理を実行
	outputMetricsToFile(metrics, tempDir)

	// 出力されたファイルを検証
	files, err := ioutil.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read temp directory: %v", err)
	}

	// メトリクスファイルを検索
	var metricsFiles []os.FileInfo
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "gcs_metrics_") &&
			strings.HasSuffix(file.Name(), ".json") {
			metricsFiles = append(metricsFiles, file)
		}
	}

	// ファイルが生成されたことを確認
	if len(metricsFiles) == 0 {
		t.Fatal("No metrics files were generated")
	}

	// 最初のファイルを検証
	fileName := filepath.Join(tempDir, metricsFiles[0].Name())
	jsonData, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Fatalf("Failed to read metrics file: %v", err)
	}

	// JSONデータをパース
	var output MetricsOutput
	if err := json.Unmarshal(jsonData, &output); err != nil {
		t.Fatalf("Failed to parse JSON metrics: %v", err)
	}

	// メトリクスの内容を検証
	validateMetricsOutput(t, metrics, output)
}

// outputMetricsToFile はテスト用のメトリクスファイル出力関数
func outputMetricsToFile(metrics *GcsMetrics, metricsPath string) {
	// 成功率計算
	successRate := 0.0
	totalOps := metrics.SuccessWrites + metrics.FailedWrites
	if totalOps > 0 {
		successRate = float64(metrics.SuccessWrites) / float64(totalOps) * 100
	}

	// 平均書き込み遅延計算
	avgLatency := time.Duration(0)
	if len(metrics.WriteLatencies) > 0 {
		sum := time.Duration(0)
		for _, lat := range metrics.WriteLatencies {
			sum += lat
		}
		avgLatency = sum / time.Duration(len(metrics.WriteLatencies))
	}

	// 平均圧縮率計算
	avgCompression := 0.0
	if len(metrics.CompressionRatios) > 0 {
		sum := 0.0
		for _, ratio := range metrics.CompressionRatios {
			sum += ratio
		}
		avgCompression = sum / float64(len(metrics.CompressionRatios))
	}

	// メトリクス出力用構造体の作成
	output := MetricsOutput{
		Timestamp:           time.Now().Format(time.RFC3339),
		SuccessRate:         successRate,
		TotalLogs:           metrics.TotalLogs,
		TotalBytes:          metrics.TotalBytes,
		BufferUsage:         metrics.BufferUtilization * 100,
		AvgWriteLatency:     avgLatency.String(),
		AvgCompressionRatio: avgCompression,
		Retries:             metrics.RetryAttempts,
		MaxRetriesReached:   metrics.MaxRetriesReached,
		BufferOverflows:     metrics.BufferOverflows,
		ErrorsByType:        make(map[string]int64),
		TagStats:            make(map[string]TestOutputTagStat),
	}

	// エラータイプのコピー
	for errType, count := range metrics.ErrorCounts {
		output.ErrorsByType[errType] = count
	}

	// タグ別統計情報のコピー
	for tag, stats := range metrics.TagStats {
		tagSuccessRate := 0.0
		tagTotalOps := stats.SuccessWrites + stats.FailedWrites
		if tagTotalOps > 0 {
			tagSuccessRate = float64(stats.SuccessWrites) / float64(tagTotalOps) * 100
		}

		output.TagStats[tag] = TestOutputTagStat{
			LogCount:       stats.LogCount,
			BytesProcessed: stats.BytesProcessed,
			SuccessRate:    tagSuccessRate,
		}
	}

	// JSONに変換
	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return
	}

	// 時刻ベースのファイル名生成
	timeStr := time.Now().Format("20060102-150405")
	fileName := filepath.Join(metricsPath, "gcs_metrics_"+timeStr+".json")

	// ファイル出力
	ioutil.WriteFile(fileName, jsonData, 0644)
}

// createTestMetrics はテスト用のメトリクスデータを作成する
func createTestMetrics() *GcsMetrics {
	metrics := &GcsMetrics{
		TotalLogs:         1250000,
		TotalBytes:        2348576982,
		SuccessWrites:     9980,
		FailedWrites:      20,
		RetryAttempts:     15,
		MaxRetriesReached: 0,
		BufferOverflows:   0,
		CurrentBufferSize: 524288,
		BufferUtilization: 0.452,
		WriteLatencies: []time.Duration{
			200 * time.Millisecond,
			250 * time.Millisecond,
			220 * time.Millisecond,
			270 * time.Millisecond,
		},
		CompressionRatios: []float64{3.2, 3.5, 3.7, 3.6},
		ErrorCounts: map[string]int64{
			"connection": 10,
			"timeout":    5,
			"auth":       0,
		},
		TagStats: map[string]*TestTagStatInternal{
			"app.access": {
				LogCount:       850000,
				BytesProcessed: 1598576982,
				SuccessWrites:  8500,
				FailedWrites:   5,
			},
			"app.error": {
				LogCount:       400000,
				BytesProcessed: 750000000,
				SuccessWrites:  1480,
				FailedWrites:   15,
			},
		},
	}
	return metrics
}

// validateMetricsOutput はメトリクス出力の検証を行う
func validateMetricsOutput(t *testing.T, metrics *GcsMetrics, output MetricsOutput) {
	// 基本的なメトリクスの検証
	if output.TotalLogs != metrics.TotalLogs {
		t.Errorf("Total logs mismatch: expected %d, got %d", metrics.TotalLogs, output.TotalLogs)
	}

	if output.TotalBytes != metrics.TotalBytes {
		t.Errorf("Total bytes mismatch: expected %d, got %d", metrics.TotalBytes, output.TotalBytes)
	}

	// 計算値の検証（許容範囲内であることを確認）
	expectedSuccessRate := float64(metrics.SuccessWrites) / float64(metrics.SuccessWrites+metrics.FailedWrites) * 100
	if !almostEqual(output.SuccessRate, expectedSuccessRate, 0.01) {
		t.Errorf("Success rate mismatch: expected %.2f%%, got %.2f%%", expectedSuccessRate, output.SuccessRate)
	}

	// エラータイプの検証
	for errType, count := range metrics.ErrorCounts {
		if output.ErrorsByType[errType] != count {
			t.Errorf("Error count mismatch for type %s: expected %d, got %d",
				errType, count, output.ErrorsByType[errType])
		}
	}

	// タグ統計の検証
	for tag, stats := range metrics.TagStats {
		outputStat, exists := output.TagStats[tag]
		if !exists {
			t.Errorf("Tag stat not found for tag %s", tag)
			continue
		}

		if outputStat.LogCount != stats.LogCount {
			t.Errorf("Log count mismatch for tag %s: expected %d, got %d",
				tag, stats.LogCount, outputStat.LogCount)
		}

		if outputStat.BytesProcessed != stats.BytesProcessed {
			t.Errorf("Bytes processed mismatch for tag %s: expected %d, got %d",
				tag, stats.BytesProcessed, outputStat.BytesProcessed)
		}

		expectedTagSuccessRate := float64(stats.SuccessWrites) / float64(stats.SuccessWrites+stats.FailedWrites) * 100
		if !almostEqual(outputStat.SuccessRate, expectedTagSuccessRate, 0.01) {
			t.Errorf("Success rate mismatch for tag %s: expected %.2f%%, got %.2f%%",
				tag, expectedTagSuccessRate, outputStat.SuccessRate)
		}
	}
}

// almostEqual は浮動小数点値が許容範囲内で等しいかをチェックする
func almostEqual(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// TestMetricsRotation はメトリクスファイルのローテーション機能をテストする
func TestMetricsRotation(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "gcs_metrics_rotation_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト完了後にディレクトリを削除

	// 複数のテストファイルを作成（古い日付から順に）
	testFiles := []string{
		"gcs_metrics_20230501-120000.json",
		"gcs_metrics_20230502-120000.json",
		"gcs_metrics_20230503-120000.json",
		"gcs_metrics_20230504-120000.json",
		"gcs_metrics_20230505-120000.json",
		"gcs_metrics_20230506-120000.json",
	}

	for _, fileName := range testFiles {
		filePath := filepath.Join(tempDir, fileName)
		dummyContent := []byte("{\"timestamp\": \"" + fileName + "\"}")
		if err := ioutil.WriteFile(filePath, dummyContent, 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", fileName, err)
		}

		// ファイル作成時間に差をつける
		time.Sleep(1 * time.Millisecond)
	}

	// テストファイルが正しく作成されたことを確認
	initialFiles, _ := ioutil.ReadDir(tempDir)
	if len(initialFiles) != len(testFiles) {
		t.Fatalf("Expected %d test files, but got %d", len(testFiles), len(initialFiles))
	}

	// ローテーション関数を実行（最新の3ファイルを保持）
	keepCount := 3
	cleanupOldMetricsFiles(tempDir, keepCount)

	// 結果を検証
	remainingFiles, _ := ioutil.ReadDir(tempDir)
	if len(remainingFiles) != keepCount {
		t.Errorf("Expected %d files after rotation, but got %d", keepCount, len(remainingFiles))
	}

	// 残ったファイルが最新のものであることを確認
	expectedToKeep := testFiles[len(testFiles)-keepCount:]
	for i, file := range remainingFiles {
		expected := expectedToKeep[i]
		if file.Name() != expected {
			t.Errorf("Expected file %s at position %d, but got %s", expected, i, file.Name())
		}
	}
}

// cleanupOldMetricsFiles は古いメトリクスファイルを削除する
func cleanupOldMetricsFiles(metricsPath string, keepCount int) {
	files, err := ioutil.ReadDir(metricsPath)
	if err != nil {
		return
	}

	// メトリクスファイルのみをフィルタリング
	var metricFiles []os.FileInfo
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "gcs_metrics_") &&
			strings.HasSuffix(file.Name(), ".json") {
			metricFiles = append(metricFiles, file)
		}
	}

	// 修正時間で降順ソート
	sort.SliceStable(metricFiles, func(i, j int) bool {
		return metricFiles[i].ModTime().After(metricFiles[j].ModTime())
	})

	// 古いファイルを削除
	if len(metricFiles) > keepCount {
		for i := keepCount; i < len(metricFiles); i++ {
			filePath := filepath.Join(metricsPath, metricFiles[i].Name())
			os.Remove(filePath)
		}
	}
}

// TestLoadMetricsFromFile はJSONファイルからメトリクスを読み込むテスト
func TestLoadMetricsFromFile(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "gcs_metrics_load_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト完了後にディレクトリを削除

	// テスト用のメトリクスを作成
	metrics := createTestMetrics()

	// メトリクスをファイルに出力
	outputMetricsToFile(metrics, tempDir)

	// 出力されたファイルを検索
	files, _ := ioutil.ReadDir(tempDir)
	var latestFile string
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "gcs_metrics_") {
			latestFile = file.Name()
			break
		}
	}

	if latestFile == "" {
		t.Fatal("No metrics file found")
	}

	// ファイルからメトリクスを読み込む
	filePath := filepath.Join(tempDir, latestFile)
	loadedMetrics, err := loadMetricsFromFile(filePath)
	if err != nil {
		t.Fatalf("Failed to load metrics from file: %v", err)
	}

	// 読み込んだメトリクスを検証
	if loadedMetrics.TotalLogs != metrics.TotalLogs {
		t.Errorf("Total logs mismatch after loading: expected %d, got %d",
			metrics.TotalLogs, loadedMetrics.TotalLogs)
	}

	if loadedMetrics.TotalBytes != metrics.TotalBytes {
		t.Errorf("Total bytes mismatch after loading: expected %d, got %d",
			metrics.TotalBytes, loadedMetrics.TotalBytes)
	}

	// タグごとの統計も検証
	for tag, expectedStats := range metrics.TagStats {
		loadedStats, exists := loadedMetrics.TagStats[tag]
		if !exists {
			t.Errorf("Tag %s not found in loaded metrics", tag)
			continue
		}

		if loadedStats.LogCount != expectedStats.LogCount {
			t.Errorf("Log count mismatch for tag %s: expected %d, got %d",
				tag, expectedStats.LogCount, loadedStats.LogCount)
		}
	}
}

// loadMetricsFromFile はJSONファイルからメトリクスを読み込む
func loadMetricsFromFile(filePath string) (*GcsMetrics, error) {
	// ファイルを読み込む
	jsonData, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// JSONをパース
	var output MetricsOutput
	if err := json.Unmarshal(jsonData, &output); err != nil {
		return nil, err
	}

	// GcsMetrics構造体に変換
	metrics := &GcsMetrics{
		TotalLogs:         output.TotalLogs,
		TotalBytes:        output.TotalBytes,
		RetryAttempts:     output.Retries,
		MaxRetriesReached: output.MaxRetriesReached,
		BufferOverflows:   output.BufferOverflows,
		BufferUtilization: output.BufferUsage / 100,
		ErrorCounts:       make(map[string]int64),
		TagStats:          make(map[string]*TestTagStatInternal),
	}

	// 成功率から成功/失敗書き込み数を計算（概算）
	totalWrites := int64(100)
	metrics.SuccessWrites = int64(float64(totalWrites) * output.SuccessRate / 100)
	metrics.FailedWrites = totalWrites - metrics.SuccessWrites

	// エラータイプをコピー
	for errType, count := range output.ErrorsByType {
		metrics.ErrorCounts[errType] = count
	}

	// タグ別統計情報をコピー
	for tag, stats := range output.TagStats {
		tagTotalWrites := int64(100)
		tagSuccessWrites := int64(float64(tagTotalWrites) * stats.SuccessRate / 100)

		metrics.TagStats[tag] = &TestTagStatInternal{
			LogCount:       stats.LogCount,
			BytesProcessed: stats.BytesProcessed,
			SuccessWrites:  tagSuccessWrites,
			FailedWrites:   tagTotalWrites - tagSuccessWrites,
		}
	}

	return metrics, nil
}