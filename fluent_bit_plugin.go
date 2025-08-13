package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

// FlushResult はフラッシュ操作の結果を表す型
type FlushResult int

const (
	// FlushResultSuccess は成功を表す
	FlushResultSuccess FlushResult = iota
	// FlushResultRetry はリトライ可能なエラーを表す
	FlushResultRetry
	// FlushResultError は永続的なエラーを表す
	FlushResultError
	// FlushResultNoOp は操作が不要だったことを表す
	FlushResultNoOp
)

// FluentBitPlugin はFluent BitのGoプラグインインターフェースを実装するコンポーネントです
type FluentBitPlugin struct {
	context       *PluginContext
	mutex         sync.Mutex
	flushInterval time.Duration
	lastFlushTime time.Time
	config        *PluginConfig
}

// PluginConfig はプラグインの設定パラメータを保持します
type PluginConfig struct {
	Credential       string
	Bucket           string
	Prefix           string
	Region           string
	JSONKey          string
	OutputBufferSize int
	StorageType      StorageType
	OutputDir        string // File_Output_Dirパラメータから読み取った値
	MetricsDir       string
	MaxRetryCount    int
	FlushInterval    time.Duration
}

// NewFluentBitPlugin は新しいFluentBitPluginインスタンスを作成します
func NewFluentBitPlugin(ctx context.Context, config *PluginConfig) (*FluentBitPlugin, error) {
	if config.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if config.Region == "" {
		return nil, fmt.Errorf("region is required")
	}
	if config.OutputBufferSize <= 0 {
		return nil, fmt.Errorf("invalid output buffer size: %d", config.OutputBufferSize)
	}

	// StorageTypeのデフォルト値設定
	if config.StorageType == "" {
		config.StorageType = StorageTypeGCS
	}

	// GCSの場合はCredentialが必須
	if config.StorageType == StorageTypeGCS && config.Credential == "" {
		return nil, fmt.Errorf("credential is required for GCS storage")
	}

	// ファイルストレージの場合はOutputDirが必須
	if config.StorageType == StorageTypeFile && config.OutputDir == "" {
		return nil, fmt.Errorf("output_dir is required for file storage")
	}

	// RetryCountのデフォルト値設定
	if config.MaxRetryCount <= 0 {
		config.MaxRetryCount = 3
	}

	// FlushIntervalのデフォルト値設定（デフォルトは60秒）
	if config.FlushInterval <= 0 {
		config.FlushInterval = 60 * time.Second
	}

	// StorageClientの設定をマップに変換
	storageConfig := map[string]string{
		"Credential":      config.Credential,
		"Region":          config.Region,
		"File_Output_Dir": config.OutputDir,
	}

	// バッファ設定の作成
	bufferConfig := BufferConfig{
		MaxBufferSizeBytes: config.OutputBufferSize,
		FlushTimeoutSec:    int(config.FlushInterval.Seconds()),
	}

	// StorageClientFactoryの作成
	storageFactory := &StorageClientFactory{}

	// コンポーネントを初期化
	storage, err := storageFactory.NewStorageClient(ctx, config.StorageType, storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	// メトリクスの有効化判定
	metricsEnabled := config.MetricsDir != ""

	// メトリクスコレクターを初期化
	metricsCollector := NewMetricsCollector(config.MetricsDir, 5, metricsEnabled)

	// コンポーネントの初期化
	bufferManager := NewBufferManager(bufferConfig, func() {
		metricsCollector.RecordBufferOverflow()
	})

	// プラグインコンテキスト用の設定を作成
	contextConfig := map[string]string{
		"bucket":  config.Bucket,
		"prefix":  config.Prefix,
		"jsonKey": config.JSONKey,
	}

	// PluginContextの作成
	pluginContext := NewPluginContext(
		contextConfig,
		bufferManager,
		metricsCollector,
		storage,
	)

	return &FluentBitPlugin{
		context:       pluginContext,
		flushInterval: config.FlushInterval,
		lastFlushTime: time.Now(),
		config:        config,
	}, nil
}

// addRecordToBuffer はレコードをバッファに追加します（フラッシュは行わない）
func (p *FluentBitPlugin) addRecordToBuffer(tag string, record map[interface{}]interface{}) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// JSONKeyが指定されている場合、そのキーのデータのみを使用
	var data []byte
	var err error

	if p.config.JSONKey != "" {
		if value, ok := record[p.config.JSONKey]; ok {
			if strValue, ok := value.(string); ok {
				data = []byte(strValue)
			} else {
				// すべての型に対して汎用的な処理を行う
				normalized := parseRecordValue(value)
				jsonData, err := json.Marshal(normalized)
				if err != nil {
					return fmt.Errorf("failed to convert JSONKey value to JSON: %w", err)
				}
				data = jsonData
			}
		} else {
			return fmt.Errorf("specified JSONKey '%s' not found in record", p.config.JSONKey)
		}
	} else {
		// JSONKeyが指定されていない場合、レコード全体をJSONに変換
		data, err = p.convertToJSON(record)
		if err != nil {
			return fmt.Errorf("failed to convert record to JSON: %w", err)
		}
	}

	// NDJSON形式のために末尾の余分な改行を削除
	// 改行はバッファマネージャーで追加される
	data = trimTrailingNewlines(data)

	// データをバッファに追加（リトライ中でも追加する）
	err = p.context.bufferManager.AddRecord(data)
	if err != nil {
		// バッファ満杯の場合は特別なエラーメッセージを返す
		if strings.Contains(err.Error(), "buffer truncation") || p.context.bufferManager.IsFull() {
			return fmt.Errorf("buffer full: %w", err)
		}
		return fmt.Errorf("failed to add record to buffer: %w", err)
	}

	return nil
}

// performFlush はバッファのフラッシュを実行し、結果を返します
func (p *FluentBitPlugin) performFlush(tag string) (FlushResult, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// バッファが空の場合は何もしない
	if p.context.bufferManager.Size() == 0 {
		return FlushResultNoOp, nil
	}

	// フラッシュ条件をチェック
	shouldFlush := time.Since(p.lastFlushTime) >= p.flushInterval || 
	               p.context.bufferManager.IsFull() ||
	               p.context.bufferManager.ShouldFlush()

	if !shouldFlush {
		// フラッシュ条件を満たさない場合
		return FlushResultNoOp, nil
	}

	// フラッシュを実行
	flushResult, err := p.context.Flush(tag)
	
	if err != nil {
		// エラーの種類を判定
		if isRetryableError(err) {
			// リトライ可能なエラー
			return FlushResultRetry, err
		}
		// 永続的なエラー
		return FlushResultError, err
	}

	// 成功時
	p.lastFlushTime = time.Now()
	if flushResult == 0 {
		return FlushResultSuccess, nil
	}

	// その他のケース
	return FlushResultNoOp, nil
}


// parseRecordValue はインターフェース値を再帰的に処理します
func parseRecordValue(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		// prevent encoding to base64
		return string(val)
	case map[interface{}]interface{}:
		return parseRecordMap(val)
	case map[string]interface{}:
		// 文字列キーのマップも再帰的に処理
		m := make(map[string]interface{})
		for k, v := range val {
			m[k] = parseRecordValue(v)
		}
		return m
	case []interface{}:
		var array []interface{}
		for _, item := range val {
			array = append(array, parseRecordValue(item))
		}
		return array
	default:
		return v
	}
}

// parseRecordMap はparseMapを拡張し、非文字列キーも処理します
func parseRecordMap(record map[interface{}]interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for k, v := range record {
		switch key := k.(type) {
		case string:
			m[key] = parseRecordValue(v)
		default:
			// 文字列でないキーは文字列に変換
			m[fmt.Sprintf("%v", k)] = parseRecordValue(v)
		}
	}
	return m
}

// convertToJSON はレコードをJSON形式に変換します
func (p *FluentBitPlugin) convertToJSON(record interface{}) ([]byte, error) {
	switch t := record.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	case map[interface{}]interface{}:
		// 既存のparseMap関数を使用するが、より堅牢なparseRecordMapで拡張する
		jsonMap := parseRecordMap(t)
		jsonData, err := json.Marshal(jsonMap)
		if err != nil {
			return nil, err
		}
		return jsonData, nil
	default:
		jsonData, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		return jsonData, nil
	}
}

// hasTrailingNewline はデータの末尾に改行があるかをチェックします
func hasTrailingNewline(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return data[len(data)-1] == '\n'
}

// trimTrailingNewlines はデータの末尾のすべての改行を削除します
func trimTrailingNewlines(data []byte) []byte {
	for len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	return data
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "gcs", "Google Cloud Storage Output")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	// 設定パラメータの取得
	credential := output.FLBPluginConfigKey(plugin, "Credential")
	bucket := output.FLBPluginConfigKey(plugin, "Bucket")
	prefix := output.FLBPluginConfigKey(plugin, "Prefix")
	region := output.FLBPluginConfigKey(plugin, "Region")
	jsonKey := output.FLBPluginConfigKey(plugin, "JSON_Key")
	outputBufferSizeStr := output.FLBPluginConfigKey(plugin, "Output_Buffer_Size")
	storageTypeStr := output.FLBPluginConfigKey(plugin, "Storage_Type")
	fileOutputDir := output.FLBPluginConfigKey(plugin, "File_Output_Dir")
	metricsDir := output.FLBPluginConfigKey(plugin, "Metrics_Dir")
	maxRetryCountStr := output.FLBPluginConfigKey(plugin, "Max_Retry_Count")
	flushIntervalStr := output.FLBPluginConfigKey(plugin, "Flush_Interval")

	// バッファサイズの変換
	outputBufferSize, err := strconv.Atoi(outputBufferSizeStr)
	if err != nil {
		fmt.Printf("[error] Invalid Output_Buffer_Size: %s\n", outputBufferSizeStr)
		return output.FLB_ERROR
	}

	// オプショナルパラメータの変換
	var maxRetryCount int
	if maxRetryCountStr != "" {
		maxRetryCount, err = strconv.Atoi(maxRetryCountStr)
		if err != nil {
			fmt.Printf("[error] Invalid Max_Retry_Count: %s\n", maxRetryCountStr)
			return output.FLB_ERROR
		}
	}

	var flushInterval time.Duration
	if flushIntervalStr != "" {
		flushIntervalSec, err := strconv.Atoi(flushIntervalStr)
		if err != nil {
			fmt.Printf("[error] Invalid Flush_Interval: %s\n", flushIntervalStr)
			return output.FLB_ERROR
		}
		flushInterval = time.Duration(flushIntervalSec) * time.Second
	}

	// ストレージタイプの変換
	var storageType StorageType
	if storageTypeStr != "" {
		storageType = StorageType(strings.ToLower(storageTypeStr))
		if storageType != StorageTypeGCS && storageType != StorageTypeFile {
			fmt.Printf("[error] Invalid Storage_Type: %s, must be 'gcs' or 'file'\n", storageTypeStr)
			return output.FLB_ERROR
		}
	} else {
		storageType = StorageTypeGCS
	}

	// プラグイン設定の作成
	config := &PluginConfig{
		Credential:       credential,
		Bucket:           bucket,
		Prefix:           prefix,
		Region:           region,
		JSONKey:          jsonKey,
		OutputBufferSize: outputBufferSize,
		StorageType:      storageType,
		OutputDir:        fileOutputDir,
		MetricsDir:       metricsDir,
		MaxRetryCount:    maxRetryCount,
		FlushInterval:    flushInterval,
	}

	// プラグインの初期化
	flbPlugin, err := NewFluentBitPlugin(context.Background(), config)
	if err != nil {
		fmt.Printf("[error] Failed to initialize plugin: %v\n", err)
		return output.FLB_ERROR
	}

	// コンテキストにプラグインを保存
	output.FLBPluginSetContext(plugin, flbPlugin)
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Fluent BitプラグインのContextからプラグインインスタンスを取得
	plugin := output.FLBPluginGetContext(ctx)
	if plugin == nil {
		return output.FLB_ERROR
	}

	flbPlugin, ok := plugin.(*FluentBitPlugin)
	if !ok {
		fmt.Printf("[error] Invalid plugin instance\n")
		return output.FLB_ERROR
	}

	// タグの取得
	tagStr := C.GoString(tag)

	// レコード処理中のエラーカウント
	var processErrors int

	// デコード
	decoder := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(decoder)
		if ret != 0 {
			break
		}

		// レコードを処理（フラッシュロジックなしバージョンを使用）
		err := flbPlugin.addRecordToBuffer(tagStr, record)
		if err != nil {
			log.Printf("[error] Failed to add record to buffer: %v", err)
			processErrors++
			// バッファ満杯エラーの場合は、これ以上の処理を中断
			if strings.Contains(err.Error(), "buffer full") {
				break
			}
		}
	}

	// すべてのレコードをバッファに追加した後、フラッシュを実行
	flushResult, err := flbPlugin.performFlush(tagStr)
	
	// フラッシュ結果に基づいて適切な戻り値を返す
	switch flushResult {
	case FlushResultSuccess:
		// 成功時はバッファをクリアしてOKを返す
		return output.FLB_OK
	case FlushResultRetry:
		// リトライ可能なエラーの場合
		log.Printf("[warn] Flush failed with retryable error: %v", err)
		return output.FLB_RETRY
	case FlushResultError:
		// 永続的なエラーの場合
		log.Printf("[error] Flush failed with permanent error: %v", err)
		// バッファをクリアして次のデータを受け入れる準備をする
		flbPlugin.context.bufferManager.Reset()
		return output.FLB_ERROR
	default:
		// バッファが空または条件を満たさない場合はOKを返す
		return output.FLB_OK
	}
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

// FlushPlugin はプラグインを終了してリソースを解放します
func (p *FluentBitPlugin) FlushPlugin() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 残りのバッファをフラッシュ
	if p.context.bufferManager.Size() > 0 {
		_, err := p.context.Flush("")
		if err != nil {
			return fmt.Errorf("final flush error: %w", err)
		}
	}

	// メトリクスを出力
	err := p.context.metricsCollector.OutputMetrics()
	if err != nil {
		return fmt.Errorf("metrics flush error: %w", err)
	}

	// リソースをクローズ
	return p.context.storageClient.Close()
}

// SafeAddRecord は並行安全なレコード追加メソッド（テスト用）
func (p *FluentBitPlugin) SafeAddRecord(record interface{}) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.context == nil || p.context.bufferManager == nil {
		return fmt.Errorf("plugin context not initialized")
	}
	
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	
	return p.context.bufferManager.AddRecord(recordBytes)
}

// SafeFlushIfNeeded は並行安全なフラッシュメソッド（テスト用）
func (p *FluentBitPlugin) SafeFlushIfNeeded() (bool, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.context == nil || p.context.bufferManager == nil {
		return false, fmt.Errorf("plugin context not initialized")
	}
	
	if p.context.bufferManager.ShouldFlush() {
		p.context.bufferManager.Flush()
		return true, nil
	}
	
	return false, nil
}

// main関数はmain.goで定義されています
