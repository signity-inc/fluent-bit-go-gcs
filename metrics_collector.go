package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Metrics はプラグインのメトリクスを表す構造体
type Metrics struct {
	Timestamp           string             `json:"timestamp"`
	SuccessRate         float64            `json:"success_rate_percent"`
	TotalLogs           int64              `json:"total_logs"`
	TotalBytes          int64              `json:"total_bytes"`
	BufferUsage         float64            `json:"buffer_usage_percent"`
	AvgWriteLatency     string             `json:"avg_write_latency_ms"`
	AvgCompressionRatio float64            `json:"avg_compression_ratio"`
	Retries             int64              `json:"retry_attempts"`
	MaxRetriesReached   int64              `json:"max_retries_reached"`
	BufferOverflows     int64              `json:"buffer_overflows"`
	ErrorsByType        map[string]int64   `json:"errors_by_type"`
	TagStats            map[string]TagStat `json:"tag_stats"`
}

// TagStat はタグごとの統計情報を表す構造体
type TagStat struct {
	LogCount       int64   `json:"log_count"`
	BytesProcessed int64   `json:"bytes_processed"`
	SuccessRate    float64 `json:"success_rate_percent"`
}

// MetricsCollector はメトリクスの収集と出力を担当する
type MetricsCollector struct {
	// カウンターメトリクス
	totalLogs         int64
	totalBytes        int64
	successWrites     int64
	failedWrites      int64
	retryAttempts     int64
	maxRetriesReached int64
	bufferOverflows   int64

	// ゲージメトリクス
	currentBufferSize int64
	maxBufferSize     int64

	// ヒストグラムメトリクス
	writeLatencies    []time.Duration
	compressionRatios []float64

	// エラー詳細
	errorCounts map[string]int64

	// タグ別統計
	tagStats map[string]*TagStatInternal

	// 設定
	metricsOutputPath string
	metricsOutputEnabled bool
	metricsRetention  int

	// 同期
	mutex sync.Mutex
}

// TagStatInternal は内部的なタグ統計構造体
type TagStatInternal struct {
	LogCount       int64
	BytesProcessed int64
	SuccessWrites  int64
	FailedWrites   int64
}

// NewMetricsCollector は新しいメトリクスコレクターを作成する
func NewMetricsCollector(metricsPath string, retention int, enabled bool) *MetricsCollector {
	// デフォルト値の設定
	if retention <= 0 {
		retention = 5 // デフォルトで5つのメトリクスファイルを保持
	}

	return &MetricsCollector{
		errorCounts:         make(map[string]int64),
		tagStats:            make(map[string]*TagStatInternal),
		metricsOutputPath:   metricsPath,
		metricsOutputEnabled: enabled,
		metricsRetention:    retention,
	}
}

// RecordWrite は書き込み操作を記録する
func (m *MetricsCollector) RecordWrite(success bool, tag string, byteCount int, latency time.Duration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.totalLogs++
	m.totalBytes += int64(byteCount)
	
	// 書き込み成功/失敗のカウント
	if success {
		m.successWrites++
	} else {
		m.failedWrites++
	}

	// レイテンシの記録
	m.writeLatencies = append(m.writeLatencies, latency)

	// タグごとの統計を更新
	tagStat, exists := m.tagStats[tag]
	if !exists {
		tagStat = &TagStatInternal{}
		m.tagStats[tag] = tagStat
	}

	tagStat.LogCount++
	tagStat.BytesProcessed += int64(byteCount)
	if success {
		tagStat.SuccessWrites++
	} else {
		tagStat.FailedWrites++
	}
}

// RecordCompressionRatio は圧縮率を記録する
func (m *MetricsCollector) RecordCompressionRatio(originalSize, compressedSize int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if compressedSize > 0 {
		ratio := float64(originalSize) / float64(compressedSize)
		m.compressionRatios = append(m.compressionRatios, ratio)
	}
}

// RecordRetry はリトライを記録する
func (m *MetricsCollector) RecordRetry() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.retryAttempts++
}

// RecordMaxRetriesReached は最大リトライ回数到達を記録する
func (m *MetricsCollector) RecordMaxRetriesReached() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.maxRetriesReached++
}

// RecordBufferOverflow はバッファオーバーフローを記録する
func (m *MetricsCollector) RecordBufferOverflow() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.bufferOverflows++
}

// RecordError はエラーの種類ごとにカウントを増やす
func (m *MetricsCollector) RecordError(errorType string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.errorCounts[errorType]++
}

// UpdateBufferSizeMetrics はバッファサイズメトリクスを更新する
func (m *MetricsCollector) UpdateBufferSizeMetrics(current, max int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.currentBufferSize = int64(current)
	m.maxBufferSize = int64(max)
}

// GetMetrics は現在のメトリクスを取得する
func (m *MetricsCollector) GetMetrics() Metrics {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 成功率の計算
	successRate := 0.0
	totalOps := m.successWrites + m.failedWrites
	if totalOps > 0 {
		successRate = float64(m.successWrites) / float64(totalOps) * 100
	}

	// 平均書き込み遅延の計算
	avgLatency := time.Duration(0)
	if len(m.writeLatencies) > 0 {
		sum := time.Duration(0)
		for _, lat := range m.writeLatencies {
			sum += lat
		}
		avgLatency = sum / time.Duration(len(m.writeLatencies))
	}

	// 平均圧縮率の計算
	avgCompression := 0.0
	if len(m.compressionRatios) > 0 {
		sum := 0.0
		for _, ratio := range m.compressionRatios {
			sum += ratio
		}
		avgCompression = sum / float64(len(m.compressionRatios))
	}

	// バッファ使用率の計算
	bufferUsage := 0.0
	if m.maxBufferSize > 0 {
		bufferUsage = float64(m.currentBufferSize) / float64(m.maxBufferSize) * 100
	}

	// メトリクス構造体の作成
	metrics := Metrics{
		Timestamp:           time.Now().Format(time.RFC3339),
		SuccessRate:         successRate,
		TotalLogs:           m.totalLogs,
		TotalBytes:          m.totalBytes,
		BufferUsage:         bufferUsage,
		AvgWriteLatency:     avgLatency.String(),
		AvgCompressionRatio: avgCompression,
		Retries:             m.retryAttempts,
		MaxRetriesReached:   m.maxRetriesReached,
		BufferOverflows:     m.bufferOverflows,
		ErrorsByType:        make(map[string]int64),
		TagStats:            make(map[string]TagStat),
	}

	// エラータイプのコピー
	for errType, count := range m.errorCounts {
		metrics.ErrorsByType[errType] = count
	}

	// タグ別統計情報のコピー
	for tag, stats := range m.tagStats {
		tagSuccessRate := 0.0
		tagTotalOps := stats.SuccessWrites + stats.FailedWrites
		if tagTotalOps > 0 {
			tagSuccessRate = float64(stats.SuccessWrites) / float64(tagTotalOps) * 100
		}

		metrics.TagStats[tag] = TagStat{
			LogCount:       stats.LogCount,
			BytesProcessed: stats.BytesProcessed,
			SuccessRate:    tagSuccessRate,
		}
	}

	return metrics
}

// OutputMetrics はメトリクスをJSONファイルに出力する
func (m *MetricsCollector) OutputMetrics() error {
	if !m.metricsOutputEnabled || m.metricsOutputPath == "" {
		return nil
	}

	// ディレクトリの存在確認
	if err := os.MkdirAll(m.metricsOutputPath, 0755); err != nil {
		return fmt.Errorf("failed to create metrics directory: %w", err)
	}

	// メトリクスの取得
	metrics := m.GetMetrics()

	// JSONに変換
	jsonData, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics to JSON: %w", err)
	}

	// ファイル名の生成
	timeStr := time.Now().Format("20060102-150405")
	fileName := filepath.Join(m.metricsOutputPath, "gcs_metrics_"+timeStr+".json")

	// ファイル出力
	if err := os.WriteFile(fileName, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write metrics file: %w", err)
	}

	// 古いメトリクスファイルのクリーンアップ
	m.cleanupOldMetricsFiles()

	return nil
}

// cleanupOldMetricsFiles は古いメトリクスファイルを削除する
func (m *MetricsCollector) cleanupOldMetricsFiles() {
	files, err := os.ReadDir(m.metricsOutputPath)
	if err != nil {
		return
	}

	// メトリクスファイルをフィルタリング
	var metricFiles []os.DirEntry
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "gcs_metrics_") &&
			strings.HasSuffix(file.Name(), ".json") {
			metricFiles = append(metricFiles, file)
		}
	}

	// ファイル数が制限を超えていれば、最も古いファイルを削除
	if len(metricFiles) > m.metricsRetention {
		// ファイル名でソート（時間ベースのファイル名なので時系列順になる）
		sort.Slice(metricFiles, func(i, j int) bool {
			return metricFiles[i].Name() < metricFiles[j].Name()
		})

		// 古いファイルを削除
		for i := 0; i < len(metricFiles)-m.metricsRetention; i++ {
			filePath := filepath.Join(m.metricsOutputPath, metricFiles[i].Name())
			os.Remove(filePath)
		}
	}
}