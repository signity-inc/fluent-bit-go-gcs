package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// BufferConfig はバッファの設定を表す構造体
type BufferConfig struct {
	MaxBufferSizeBytes int
	FlushTimeoutSec    int
	TruncateByLine     bool // 廃止予定: 常にJSON整合性を保持します
	AddTruncationMeta  bool // 切り詰め時にメタデータを追加するフラグ
}

// TruncationMetadata は切り詰め情報のメタデータを表す構造体
type TruncationMetadata struct {
	TruncationEvent bool      `json:"truncation_event"`
	Timestamp       string    `json:"timestamp"`
	DroppedLines    int       `json:"dropped_lines"`
	RetainedLines   int       `json:"retained_lines"`
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
		// 常にJSON整合性を保持する行単位での切り詰めを使用
		err := b.truncateByLine()
		if err != nil {
			return fmt.Errorf("buffer truncation error: %w", err)
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
// JSON整合性を保持するため、完全なJSON行のみを保持する
func (b *BufferManager) truncateByLine() error {
	data := b.buffer.Bytes()
	if len(data) == 0 {
		return nil
	}

	// 行ごとに分割
	lines := bytes.Split(data, []byte("\n"))
	if len(lines) <= 1 {
		// 改行が見つからない場合はバッファを空にする
		b.buffer.Reset()
		return nil
	}

	// 各行が有効なJSONかを確認
	validLines := make([][]byte, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue // 空行はスキップ
		}

		// 各行が有効なJSONかチェック
		var jsonObj interface{}
		if err := json.Unmarshal(line, &jsonObj); err == nil {
			validLines = append(validLines, line)
		}
	}

	if len(validLines) == 0 {
		// 有効なJSON行がない場合はバッファを空にする
		b.buffer.Reset()
		return nil
	}

	// 保持するラインの計算
	// バッファの半分のサイズになるまで、古いラインから削除
	targetSize := b.config.MaxBufferSizeBytes / 2
	totalSize := 0
	var retainedLines [][]byte
	droppedLines := 0

	// 新しいラインから逆順に追加
	for i := len(validLines) - 1; i >= 0; i-- {
		lineSize := len(validLines[i]) + 1 // 改行分を追加
		if totalSize + lineSize <= targetSize || len(retainedLines) == 0 {
			// 少なくとも1行は保持する
			retainedLines = append(retainedLines, validLines[i])
			totalSize += lineSize
		} else {
			droppedLines++
		}
	}

	// retainedLinesを逆順にして、古いものから順に並べる
	for i, j := 0, len(retainedLines)-1; i < j; i, j = i+1, j-1 {
		retainedLines[i], retainedLines[j] = retainedLines[j], retainedLines[i]
	}

	// 切り詰め情報のメタデータを追加
	if b.config.AddTruncationMeta && droppedLines > 0 {
		metadata := TruncationMetadata{
			TruncationEvent: true,
			Timestamp:       time.Now().Format(time.RFC3339),
			DroppedLines:    droppedLines,
			RetainedLines:   len(retainedLines),
		}
		metaJSON, err := json.Marshal(metadata)
		if err == nil {
			// メタデータを先頭に追加
			retainedLines = append([][]byte{metaJSON}, retainedLines...)
		}
	}

	// バッファを再構築
	b.buffer.Reset()
	for _, line := range retainedLines {
		b.buffer.Write(line)
		b.buffer.Write([]byte("\n"))
	}

	b.currentSize = b.buffer.Len()
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