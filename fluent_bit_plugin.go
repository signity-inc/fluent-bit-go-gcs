package main

import (
	"C"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
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
	OutputDir        string
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
		"credential": config.Credential,
		"region":     config.Region,
		"output_dir": config.OutputDir,
	}

	// バッファ設定の作成
	bufferConfig := BufferConfig{
		MaxBufferSizeBytes: config.OutputBufferSize,
		FlushTimeoutSec:    int(config.FlushInterval.Seconds()),
		TruncateByLine:     true,
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
	retryManager := NewRetryManager(config.MaxRetryCount, NewExponentialBackoff(
		1*time.Second, 30*time.Second, 2.0))

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
		retryManager,
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

// processRecord はFluentBitから受け取ったレコードを処理します
func (p *FluentBitPlugin) processRecord(tag string, timestamp output.FLBTime, record map[interface{}]interface{}) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// JSONKeyが指定されている場合、そのキーのデータのみを使用
	var data []byte
	var err error

	if p.config.JSONKey != "" {
		if value, ok := record[p.config.JSONKey]; ok {
			if strValue, ok := value.(string); ok {
				data = []byte(strValue + "\n")
			} else {
				data, err = convertToJSON(value)
				if err != nil {
					return fmt.Errorf("failed to convert JSONKey value to JSON: %w", err)
				}
			}
		} else {
			return fmt.Errorf("specified JSONKey '%s' not found in record", p.config.JSONKey)
		}
	} else {
		// JSONKeyが指定されていない場合、レコード全体をJSONに変換
		data, err = convertToJSON(record)
		if err != nil {
			return fmt.Errorf("failed to convert record to JSON: %w", err)
		}
	}

	// データをバッファに追加
	err = p.context.ProcessRecord(data, tag)
	if err != nil {
		return fmt.Errorf("failed to add record to buffer: %w", err)
	}

	// フラッシュ間隔またはバッファサイズの条件を満たす場合、フラッシュを実行
	shouldFlush := time.Since(p.lastFlushTime) >= p.flushInterval || p.context.bufferManager.IsFull()
	if shouldFlush {
		_, err := p.context.Flush(tag)
		if err != nil {
			return fmt.Errorf("flush error: %w", err)
		}
		p.lastFlushTime = time.Now()
	}

	return nil
}

// convertToJSON はレコードをJSON形式に変換します
func convertToJSON(record interface{}) ([]byte, error) {
	// 実装はmock_client.goから移行する
	switch t := record.(type) {
	case []byte:
		return append(t, '\n'), nil
	case string:
		return []byte(t + "\n"), nil
	case map[interface{}]interface{}:
		jsonMap := make(map[string]interface{})
		for k, v := range t {
			strKey, ok := k.(string)
			if !ok {
				strKey = fmt.Sprintf("%v", k)
			}
			jsonMap[strKey] = v
		}
		jsonData, err := json.Marshal(jsonMap)
		if err != nil {
			return nil, err
		}
		return append(jsonData, '\n'), nil
	default:
		jsonData, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		return append(jsonData, '\n'), nil
	}
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
	outputDir := output.FLBPluginConfigKey(plugin, "Output_Dir")
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
		OutputDir:        outputDir,
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

	// デコード
	decoder := output.NewDecoder(data, int(length))
	for {
		ret, _, record := output.GetRecord(decoder)
		if ret != 0 {
			break
		}

		// レコードを処理
		err := flbPlugin.processRecord(tagStr, output.FLBTime{}, record)
		if err != nil {
			fmt.Printf("[error] Failed to process record: %v\n", err)
			// エラーがあっても処理を継続
		}
	}

	// 明示的なフラッシュは行わず、バッファ管理に任せる
	// タイミングにより、processRecord内のフラッシュロジックが実行されることもある

	return output.FLB_OK
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

// main関数はmain.goで定義されています