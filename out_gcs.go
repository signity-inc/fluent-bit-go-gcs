package main

import (
	"C"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
)
import (
	"compress/gzip"
	"strconv"
	"sync"
)

type PluginContext struct {
	Buffer            bytes.Buffer
	CurrentBufferSize int
	LastFlushTime     time.Time
	Config            map[string]string
	// リトライ状態を管理するフィールド
	RetryObjectKey    string       // リトライ時に同じオブジェクトキーを使用するための保存フィールド
	IsRetrying        bool         // 現在リトライ中であるかどうかを示すフラグ
	RetryCount        int          // リトライの回数を追跡
	MaxRetryCount     int          // 最大リトライ回数（この回数を超えるとバッファを破棄）
	MaxBufferSizeBytes int         // バッファの最大サイズ制限（バイト）
	contextMutex      sync.Mutex   // コンテキスト固有のロック
}

var (
	gcsClient  Client
	err        error
	bufferSize int
	// 注: グローバルミューテックスはコンテキスト固有のミューテックスに移行するため削除
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "gcs", "GCS Output plugin written in GO!")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", output.FLBPluginConfigKey(plugin, "Credential"))
	gcsClient, err = NewClient()
	if err != nil {
		output.FLBPluginUnregister(plugin)
		log.Fatal(err)
		return output.FLB_ERROR
	}

	bufferSizeStr := output.FLBPluginConfigKey(plugin, "Output_Buffer_Size")
	bufferSize, err = strconv.Atoi(bufferSizeStr)
	if err != nil {
		log.Printf("[error] Invalid buffer size value: %s, error: %v\n", bufferSizeStr, err)
		return output.FLB_ERROR
	}
	
	// バッファサイズの検証
	const minBufferSize = 4 * 1024        // 4KB
	const maxBufferSizeLimit = 1024 * 1024 * 1024 // 1GB
	
	if bufferSize < minBufferSize {
		log.Printf("[warn] Buffer size too small (%d bytes), using minimum size: %d bytes\n", 
			bufferSize, minBufferSize)
		bufferSize = minBufferSize
	} else if bufferSize > maxBufferSizeLimit {
		log.Printf("[warn] Buffer size too large (%d bytes), using maximum size: %d bytes\n", 
			bufferSize, maxBufferSizeLimit)
		bufferSize = maxBufferSizeLimit
	}

	cfg := map[string]string{
		"region":  output.FLBPluginConfigKey(plugin, "Region"),
		"bucket":  output.FLBPluginConfigKey(plugin, "Bucket"),
		"prefix":  output.FLBPluginConfigKey(plugin, "Prefix"),
		"jsonKey": output.FLBPluginConfigKey(plugin, "JSON_Key"),
	}

	// デフォルト設定値
	maxRetryStr := output.FLBPluginConfigKey(plugin, "Max_Retry_Count")
	maxRetry := 5 // デフォルト値
	if maxRetryStr != "" {
		if val, err := strconv.Atoi(maxRetryStr); err == nil && val > 0 {
			maxRetry = val
		}
	}
	
	maxBufferSizeStr := output.FLBPluginConfigKey(plugin, "Max_Buffer_Size_MB")
	maxBufferSize := 100 * 1024 * 1024 // デフォルト100MB
	if maxBufferSizeStr != "" {
		if val, err := strconv.Atoi(maxBufferSizeStr); err == nil && val > 0 {
			maxBufferSize = val * 1024 * 1024
		}
	}

	pluginContext := &PluginContext{
		LastFlushTime:     time.Now(),
		Config:            cfg,
		RetryCount:        0,
		MaxRetryCount:     maxRetry,
		MaxBufferSizeBytes: maxBufferSize,
	}
	output.FLBPluginSetContext(plugin, pluginContext)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	values := output.FLBPluginGetContext(ctx).(*PluginContext)

	// リトライ中であればログに記録
	if values.IsRetrying {
		log.Printf("[info] Retrying flush for %s with the same buffer\n", values.Config["bucket"])
	} else {
		log.Printf("[event] Flush called %s, %v\n", values.Config["bucket"], C.GoString(tag))
	}
	
	dec := output.NewDecoder(data, int(length))

	// コンテキスト固有のロックを使用
	values.contextMutex.Lock()
	defer values.contextMutex.Unlock()
	
	// バッファサイズチェック - 最大サイズを超えている場合は切り詰める
	if values.Buffer.Len() > values.MaxBufferSizeBytes {
		log.Printf("[warn] Buffer exceeds maximum size limit (%d bytes). Oldest data will be truncated.", 
			values.MaxBufferSizeBytes)
		// バッファを切り詰める処理
		newBuffer := values.Buffer.Bytes()[values.Buffer.Len()-values.MaxBufferSizeBytes:]
		values.Buffer.Reset()
		values.Buffer.Write(newBuffer)
		values.CurrentBufferSize = len(newBuffer)
	}
	
	// リトライカウントが上限を超えていた場合はリセット
	if values.RetryCount > values.MaxRetryCount {
		log.Printf("[warn] Maximum retry count (%d) reached, discarding buffer", values.MaxRetryCount)
		values.Buffer.Reset()
		values.CurrentBufferSize = 0
		values.IsRetrying = false
		values.RetryObjectKey = ""
		values.RetryCount = 0
	}

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		line, err := createJSON(values.Config["jsonKey"], record)
		if err != nil {
			log.Printf("[warn] error creating message for GCS: %v\n", err)
			continue
		}

		// リトライ中でなければ通常通りバッファに追加
		if !values.IsRetrying {
			values.Buffer.Write(line)
			values.Buffer.Write([]byte("\n"))
			values.CurrentBufferSize += len(line) + 1
		}

		if values.CurrentBufferSize >= bufferSize || values.IsRetrying {
			if err := flushBuffer(values, C.GoString(tag)); err != nil {
				log.Printf("[info] Scheduling retry for buffer flush: %v\n", err)
				// ロックはdefer文で解放されるのでここで明示的に解放する必要はない
				return output.FLB_RETRY
			}
		}
	}

	currentTime := time.Now()
	if values.IsRetrying || (currentTime.Sub(values.LastFlushTime) >= time.Minute && values.Buffer.Len() > 0) {
		if err := flushBuffer(values, C.GoString(tag)); err != nil {
			log.Printf("[info] Scheduling retry for time-based flush: %v\n", err)
			// ロックはdefer文で解放されるのでここで明示的に解放する必要はない
			return output.FLB_RETRY
		}
	}
	
	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later
	return output.FLB_OK
}

// gzipリソース管理を改善するヘルパー関数
func compressBuffer(data []byte) (*bytes.Buffer, error) {
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

// エラーの種類に基づいてリトライ可能かを判断する関数
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// 特定のエラータイプに基づく判定ロジック
	// 一時的なエラーはリトライ可能だが、永続的なエラーはリトライ不可
	
	// ネットワーク関連のエラーはリトライ可能
	if strings.Contains(err.Error(), "connection") || 
	   strings.Contains(err.Error(), "timeout") ||
	   strings.Contains(err.Error(), "temporary") {
		return true
	}
	
	// 認証エラーなどの永続的なエラーはリトライ不可
	if strings.Contains(err.Error(), "permission") || 
	   strings.Contains(err.Error(), "auth") ||
	   strings.Contains(err.Error(), "credential") {
		return false
	}
	
	// デフォルトはリトライ可能として扱う
	return true
}

func flushBuffer(values *PluginContext, tag string) error {
	log.Printf("[event] Flushing buffer %s, %v\n", values.Config["bucket"], tag)
	if values.Buffer.Len() > 0 {
		// 改善されたgzip処理を使用
		gzipBuffer, err := compressBuffer(values.Buffer.Bytes())
		if err != nil {
			log.Printf("[warn] %v\n", err)
			values.IsRetrying = true
			values.RetryCount++
			return err
		}

		// リトライ時には前回保存したオブジェクトキーを再利用し、
		// そうでない場合は新しいキーを生成して保存する
		var objectKey string
		if values.IsRetrying && values.RetryObjectKey != "" {
			objectKey = values.RetryObjectKey
			log.Printf("[info] Retrying with the same object key: %s\n", objectKey)
		} else {
			objectKey = GenerateObjectKey(values.Config["prefix"], tag, getCurrentJstTime())
			values.RetryObjectKey = objectKey // 後続のリトライのためにキーを保存
		}

		if err = gcsClient.Write(values.Config["bucket"], objectKey, gzipBuffer); err != nil {
			// エラーの種類を判断してリトライ戦略を決定
			if isRetryableError(err) {
				log.Printf("[warn] Retryable error sending message to GCS: %v\n", err)
				
				// リトライカウントを増やす
				values.RetryCount++
				
				// 最大リトライ回数を超えた場合は諦める
				if values.MaxRetryCount > 0 && values.RetryCount >= values.MaxRetryCount {
					log.Printf("[error] Maximum retry count reached (%d), discarding buffer data\n", 
						values.MaxRetryCount)
					// バッファをリセット
					values.Buffer.Reset()
					values.CurrentBufferSize = 0
					values.LastFlushTime = time.Now()
					values.IsRetrying = false
					values.RetryObjectKey = ""
					values.RetryCount = 0
					return nil
				}
				
				// リトライフラグを設定して続行
				values.IsRetrying = true
				log.Printf("[info] Scheduling retry %d/%d\n", values.RetryCount, values.MaxRetryCount)
				return err
			} else {
				// リトライ不可能なエラーの場合はバッファを破棄
				log.Printf("[error] Non-retryable error, discarding buffer: %v\n", err)
				values.Buffer.Reset()
				values.CurrentBufferSize = 0
				values.LastFlushTime = time.Now()
				values.IsRetrying = false
				values.RetryObjectKey = ""
				values.RetryCount = 0
				return err
			}
		}

		// 成功時のみバッファをリセットし、リトライ状態をクリアする
		values.Buffer.Reset()
		values.CurrentBufferSize = 0
		values.LastFlushTime = time.Now()
		values.IsRetrying = false
		values.RetryObjectKey = ""
		values.RetryCount = 0
		log.Printf("[info] Successfully wrote data to GCS: %s\n", objectKey)
	}
	return nil
}

func getCurrentJstTime() time.Time {
	now := time.Now()
	_, offset := now.Zone()
	if offset == 0 {
		jst := time.FixedZone("JST", 9*60*60)
		return now.In(jst)
	}
	return now
}

// GenerateObjectKey : gen format object name PREFIX/YEAR/MONTH/DAY/tag/timestamp_uuid.log
func GenerateObjectKey(prefix, tag string, t time.Time) string {
	year, month, day := t.Date()
	date_str := fmt.Sprintf("%04d/%02d/%02d", year, month, day)
	fileName := fmt.Sprintf("%s/%d_%s.log.gz", date_str, t.Unix(), uuid.Must(uuid.NewRandom()).String())
	return filepath.Join(prefix, tag, fileName)
}

func parseMap(mapInterface map[interface{}]interface{}) map[string]interface{} {
	m := make(map[string]interface{})

	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			m[k.(string)] = string(t)
		case map[interface{}]interface{}:
			m[k.(string)] = parseMap(t)
		default:
			m[k.(string)] = v
		}
	}

	return m
}

func createJSON(key string, record map[interface{}]interface{}) ([]byte, error) {
	m := parseMap(record)

	var data map[string]interface{}
	if val, ok := m[key]; ok {
		data = val.(map[string]interface{})
	} else {
		data = m
	}

	js, err := jsoniter.Marshal(data)
	if err != nil {
		return []byte("{}"), err
	}

	return js, nil
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {}
