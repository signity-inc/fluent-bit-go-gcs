package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PluginContext はFluentBitプラグインのコンテキストを管理する構造体
type PluginContext struct {
	// 設定
	config map[string]string
	
	// コンポーネント
	bufferManager    *BufferManager
	metricsCollector *MetricsCollector
	storageClient    StorageClient
	
	// タイムゾーン
	timezone *time.Location
}

// NewPluginContext は新しいPluginContextを作成する
func NewPluginContext(
	config map[string]string, 
	bufferManager *BufferManager,
	metricsCollector *MetricsCollector,
	storageClient StorageClient,
) *PluginContext {
	// JST（日本標準時）タイムゾーンの設定
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		// ロケーションの読み込みに失敗した場合はUTC+9の固定タイムゾーンを使用
		jst = time.FixedZone("JST", 9*60*60)
	}

	return &PluginContext{
		config:           config,
		bufferManager:    bufferManager,
		metricsCollector: metricsCollector,
		storageClient:    storageClient,
		timezone:         jst,
	}
}

// ProcessRecord はレコードを処理してバッファに追加する
func (p *PluginContext) ProcessRecord(record []byte, tag string) error {
	// リトライ中でもデータを受け入れる（データロスを防ぐため）
	// 注: 以前はリトライ中はデータを破棄していたが、これはデータロスの原因となっていた
	return p.bufferManager.AddRecord(record)
}

// FlushIfNeeded はバッファが条件を満たす場合にフラッシュを実行する
func (p *PluginContext) FlushIfNeeded(tag string) (int, error) {
	shouldFlush := p.bufferManager.IsFull() || 
	               p.bufferManager.ShouldFlush()
	
	if shouldFlush {
		return p.Flush(tag)
	}
	
	return 0, nil // フラッシュ不要
}

// Flush はバッファをフラッシュして圧縮しストレージに書き込む
func (p *PluginContext) Flush(tag string) (int, error) {
	// 注: リトライ回数のチェックは削除
	// Fluent Bitのリトライメカニズムを活用するため、
	// プラグイン側での複雑なリトライ管理は不要

	// バッファの取得
	bufferData, err := p.bufferManager.Flush()
	if err != nil {
		log.Printf("[error] Failed to flush buffer: %v", err)
		return -1, err
	}

	// バッファが空なら何もしない
	if len(bufferData) == 0 {
		return 0, nil
	}

	startTime := time.Now()

	// データ圧縮
	compressedData, err := p.compressData(bufferData)
	if err != nil {
		log.Printf("[error] Compression error: %v", err)
		p.metricsCollector.RecordError("compression")
		// 圧縮エラーは一時的なエラーとして扱う
		return -1, err
	}

	// 圧縮率の記録
	p.metricsCollector.RecordCompressionRatio(len(bufferData), compressedData.Len())

	// オブジェクトキーの生成
	objectKey := p.generateObjectKey(tag)

	// 圧縮データをストレージに書き込み
	err = p.storageClient.Write(p.config["bucket"], objectKey, compressedData)
	
	// 処理時間の計測
	elapsed := time.Since(startTime)
	
	// 結果の処理
	if err != nil {
		// エラータイプの判定
		errType := "storage"
		errStr := err.Error()
		if strings.Contains(errStr, "connection") {
			errType = "connection"
		} else if strings.Contains(errStr, "timeout") {
			errType = "timeout"
		} else if strings.Contains(errStr, "permission") || strings.Contains(errStr, "forbidden") {
			errType = "permission"
		}
		
		log.Printf("[error] Failed to write to storage: %v", err)
		p.metricsCollector.RecordError(errType)
		p.metricsCollector.RecordWrite(false, tag, len(bufferData), elapsed)
		
		// エラーを返す（Fluent Bitが適切にリトライを判断）
		return -1, err
	}

	// 成功時の処理
	log.Printf("[info] Successfully wrote data to storage: %s (%d bytes)", objectKey, compressedData.Len())
	
	// 成功時のみバッファをクリア
	p.bufferManager.Reset()
	p.metricsCollector.RecordWrite(true, tag, len(bufferData), elapsed)
	
	// メトリクス出力
	if err := p.metricsCollector.OutputMetrics(); err != nil {
		log.Printf("[warn] Failed to output metrics: %v", err)
	}
	
	return 0, nil
}

// compressData はデータをGZIP圧縮する
func (p *PluginContext) compressData(data []byte) (*bytes.Buffer, error) {
	var gzipBuffer bytes.Buffer
	zw := gzip.NewWriter(&gzipBuffer)
	
	// データを書き込む
	if _, err := zw.Write(data); err != nil {
		zw.Close() // エラー時もWriterをクローズ
		return nil, fmt.Errorf("gzip compression error: %w", err)
	}
	
	// 明示的にClose呼び出し（deferではなく）
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("error closing gzip writer: %w", err)
	}
	
	return &gzipBuffer, nil
}

// generateObjectKey はログデータ用のオブジェクトキーを生成する
func (p *PluginContext) generateObjectKey(tag string) string {
	// JSTタイムゾーンでの現在時刻を取得
	now := time.Now().In(p.timezone)
	year, month, day := now.Date()
	
	// PREFIX/TAG/YEAR/MONTH/DAY/timestamp_uuid.log.gz 形式のキーを生成
	dateStr := fmt.Sprintf("%04d/%02d/%02d", year, month, day)
	fileName := fmt.Sprintf("%s/%d_%s.log.gz", dateStr, now.Unix(), uuid.Must(uuid.NewRandom()).String())
	
	return filepath.Join(p.config["prefix"], tag, fileName)
}