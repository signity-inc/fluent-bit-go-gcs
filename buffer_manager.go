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
	TruncateByLine     bool // 行単位での切り詰めを有効にするフラグ
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

	// バッファサイズの確認と切り詰め処理
	if b.buffer.Len()+len(record)+1 > b.config.MaxBufferSizeBytes {
		if b.config.TruncateByLine {
			// 行単位での切り詰め（JSON整合性を保持）
			err := b.truncateByLine()
			if err != nil {
				return fmt.Errorf("buffer truncation error: %w", err)
			}
		} else {
			// バイト単位での単純切り詰め
			newBuffer := b.buffer.Bytes()[b.buffer.Len()-(b.config.MaxBufferSizeBytes/2):]
			b.buffer.Reset()
			b.buffer.Write(newBuffer)
			b.currentSize = b.buffer.Len()
		}

		// オーバーフローコールバックの呼び出し
		if b.overflowCallback != nil {
			b.overflowCallback()
		}
	}

	// レコード追加
	_, err := b.buffer.Write(record)
	if err != nil {
		return err
	}
	// JSON行の区切りとして改行を追加
	_, err = b.buffer.Write([]byte("\n"))
	if err != nil {
		return err
	}

	b.currentSize = b.buffer.Len()
	return nil
}

// truncateByLine はバッファを行単位で切り詰める
// JSON整合性を保持するため、行の途中で切らない
func (b *BufferManager) truncateByLine() error {
	data := b.buffer.Bytes()
	if len(data) == 0 {
		return nil
	}

	// バッファの半分のサイズから開始して、最初の改行位置を見つける
	startPos := len(data) - (b.config.MaxBufferSizeBytes / 2)
	if startPos < 0 {
		startPos = 0
	}

	// 最初の改行（完全なJSON行の開始）を探す
	for i := startPos; i < len(data); i++ {
		if data[i] == '\n' {
			// 改行の次の位置から新しいバッファを作成
			newBuffer := data[i+1:]
			b.buffer.Reset()
			_, err := b.buffer.Write(newBuffer)
			return err
		}
	}

	// 改行が見つからない場合はバッファを空にする
	b.buffer.Reset()
	return nil
}

// Flush はバッファの内容を取得して、バッファをリセットする
func (b *BufferManager) Flush() ([]byte, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.buffer.Len() == 0 {
		return nil, nil
	}

	data := b.buffer.Bytes()
	result := make([]byte, len(data))
	copy(result, data)
	
	// バッファはリセットしない - リトライ時のためにデータを保持
	// リセットはResetメソッドで明示的に行う

	return result, nil
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