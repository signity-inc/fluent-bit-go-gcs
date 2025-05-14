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
	retryManager     *RetryManager
	metricsCollector *MetricsCollector
	storageClient    StorageClient
	
	// タイムゾーン
	timezone *time.Location
}

// NewPluginContext は新しいPluginContextを作成する
func NewPluginContext(
	config map[string]string, 
	bufferManager *BufferManager,
	retryManager *RetryManager,
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
		retryManager:     retryManager,
		metricsCollector: metricsCollector,
		storageClient:    storageClient,
		timezone:         jst,
	}
}

// ProcessRecord はレコードを処理してバッファに追加する
func (p *PluginContext) ProcessRecord(record []byte, tag string) error {
	// リトライ中でない場合のみバッファに追加
	if !p.retryManager.IsRetrying() {
		return p.bufferManager.AddRecord(record)
	}
	return nil
}

// FlushIfNeeded はバッファが条件を満たす場合にフラッシュを実行する
func (p *PluginContext) FlushIfNeeded(tag string) (int, error) {
	shouldFlush := p.bufferManager.IsFull() || 
	               p.bufferManager.ShouldFlush() || 
				   p.retryManager.IsRetrying()
	
	if shouldFlush {
		return p.Flush(tag)
	}
	
	return 0, nil // フラッシュ不要
}

// Flush はバッファをフラッシュして圧縮しストレージに書き込む
func (p *PluginContext) Flush(tag string) (int, error) {
	// 最大リトライ回数チェック
	if p.retryManager.GetRetryCount() > p.retryManager.maxRetryCount {
		log.Printf("[warn] Maximum retry count (%d) reached, discarding buffer", p.retryManager.maxRetryCount)
		p.bufferManager.Reset()
		p.retryManager.ResetRetry()
		p.metricsCollector.RecordMaxRetriesReached()
		return 0, nil // エラーを返さない、バッファを破棄して続行
	}

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
		// 圧縮エラーはリトライ対象
		p.retryManager.IncrementRetryCount()
		p.metricsCollector.RecordRetry()
		p.metricsCollector.RecordError("compression")
		return -1, err
	}

	// 圧縮率の記録
	p.metricsCollector.RecordCompressionRatio(len(bufferData), compressedData.Len())

	// オブジェクトキーの生成または再利用
	var objectKey string
	if p.retryManager.IsRetrying() && p.retryManager.GetRetryObjectKey() != "" {
		objectKey = p.retryManager.GetRetryObjectKey()
		log.Printf("[info] Retrying with the same object key: %s", objectKey)
	} else {
		objectKey = p.generateObjectKey(tag)
		p.retryManager.SetRetryObjectKey(objectKey)
	}

	// 圧縮データをストレージに書き込み
	err = p.storageClient.Write(p.config["bucket"], objectKey, compressedData)
	
	// 処理時間の計測
	elapsed := time.Since(startTime)
	
	// 結果の処理
	if err != nil {
		// エラーログ
		errType := "storage"
		if strings.Contains(err.Error(), "connection") {
			errType = "connection"
		} else if strings.Contains(err.Error(), "timeout") {
			errType = "timeout"
		} else if strings.Contains(err.Error(), "permission") {
			errType = "permission"
		}
		
		log.Printf("[error] Failed to write to storage: %v", err)
		p.metricsCollector.RecordError(errType)
		
		// リトライ可能かチェック
		if p.retryManager.ShouldRetry(err) {
			p.retryManager.IncrementRetryCount()
			p.metricsCollector.RecordRetry()
			p.metricsCollector.RecordWrite(false, tag, len(bufferData), elapsed)
			return -1, err // リトライを指示
		} else {
			// リトライ不可のエラーの場合はバッファを破棄
			log.Printf("[warn] Non-retryable error, discarding buffer: %v", err)
			p.bufferManager.Reset()
			p.retryManager.ResetRetry()
			p.metricsCollector.RecordWrite(false, tag, len(bufferData), elapsed)
			return -1, err
		}
	}

	// 成功時の処理
	log.Printf("[info] Successfully wrote data to storage: %s (%d bytes)", objectKey, compressedData.Len())
	p.bufferManager.Reset()
	p.retryManager.ResetRetry()
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
	
	// 必ずCloseを呼び出すようにする
	defer func() {
		if zw != nil {
			zw.Close()
		}
	}()
	
	if _, err := zw.Write(data); err != nil {
		return nil, fmt.Errorf("gzip compression error: %w", err)
	}
	
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("error closing gzip writer: %w", err)
	}
	
	// 明示的にnilを設定してdeferで二重クローズを防止
	zw = nil
	
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