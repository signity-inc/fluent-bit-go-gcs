package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

// BufferConfig はバッファの設定を表す構造体
type BufferConfig struct {
	MaxBufferSizeBytes int
	FlushTimeoutSec    int
}

// BufferManager はログデータのバッファリングを管理する構造体
type BufferManager struct {
	buffer           bytes.Buffer
	currentSize      int
	lastFlushTime    time.Time
	config           BufferConfig
	mutex            sync.Mutex
	overflowCallback func() // バッファオーバーフロー時のコールバック
}

// NewBufferManager は新しいBufferManagerを作成する
func NewBufferManager(config BufferConfig, overflowCallback func()) *BufferManager {
	// 最小値のデフォルト設定
	if config.MaxBufferSizeBytes <= 0 {
		config.MaxBufferSizeBytes = 4 * 1024 // 4KB
	}
	if config.FlushTimeoutSec <= 0 {
		config.FlushTimeoutSec = 60 // 1分
	}

	return &BufferManager{
		lastFlushTime:    time.Now(),
		config:           config,
		overflowCallback: overflowCallback,
	}
}

// AddRecord はバッファにレコードを追加する
// JSONの完全性を保持するため、末尾に改行を追加
func (b *BufferManager) AddRecord(record []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// バッファサイズの確認
	requiredSize := b.buffer.Len() + len(record) + 1
	if requiredSize > b.config.MaxBufferSizeBytes {
		// バッファオーバーフローが発生
		// 重要: データ損失を防ぐため、切り詰めは行わずエラーを返す
		// Fluent Bitにリトライを促すことで、データの整合性を保つ
		
		// オーバーフローコールバックの呼び出し（メトリクス記録用）
		if b.overflowCallback != nil {
			b.overflowCallback()
		}
		
		// バッファ満杯エラーを返す
		return fmt.Errorf("buffer overflow: current size %d, record size %d, max size %d", 
			b.buffer.Len(), len(record)+1, b.config.MaxBufferSizeBytes)
	}

	// レコード追加
	_, err := b.buffer.Write(record)
	if err != nil {
		return err
	}
	
	// NDJSON形式のために改行を追加
	_, err = b.buffer.Write([]byte("\n"))
	if err != nil {
		return err
	}

	b.currentSize = b.buffer.Len()
	return nil
}

// truncateByLine は廃止されました
// バッファオーバーフロー時はエラーを返すように変更されたため、
// この関数は不要になりました

// Flush はバッファの内容を取得して返す
// clearBufferがtrueの場合、バッファをクリアする
func (b *BufferManager) Flush() ([]byte, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.buffer.Len() == 0 {
		return nil, nil
	}

	data := b.buffer.Bytes()
	result := make([]byte, len(data))
	copy(result, data)
	
	// 注: バッファのクリアはResetメソッドで明示的に行う
	// これにより、リトライ時のデータ保持と成功時のクリアを分離

	return result, nil
}

// FlushAndReset はバッファの内容を取得してからバッファをリセットする
// 成功時に使用するためのコンビニエンスメソッド
func (b *BufferManager) FlushAndReset() ([]byte, error) {
	data, err := b.Flush()
	if err != nil {
		return nil, err
	}
	
	if len(data) > 0 {
		b.Reset()
	}
	
	return data, nil
}

// Reset はバッファをリセットする（成功時やリトライ上限到達時に呼び出す）
func (b *BufferManager) Reset() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.buffer.Reset()
	b.currentSize = 0
	b.lastFlushTime = time.Now()
}

// Size はバッファの現在のサイズを返す
func (b *BufferManager) Size() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.currentSize
}

// IsFull はバッファが設定された最大サイズに達しているかを返す
func (b *BufferManager) IsFull() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.currentSize >= b.config.MaxBufferSizeBytes
}

// ShouldFlush はタイムアウトに基づいてフラッシュすべきかを返す
func (b *BufferManager) ShouldFlush() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.currentSize == 0 {
		return false
	}

	timeout := time.Duration(b.config.FlushTimeoutSec) * time.Second
	return time.Since(b.lastFlushTime) >= timeout
}

// UpdateFlushTime は最終フラッシュ時間を更新する
func (b *BufferManager) UpdateFlushTime() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.lastFlushTime = time.Now()
}