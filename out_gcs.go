package main

import (
	"C"
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
}

var (
	gcsClient  Client
	err        error
	bufferSize int
	mutex      sync.Mutex
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

	cfg := map[string]string{
		"region":  output.FLBPluginConfigKey(plugin, "Region"),
		"bucket":  output.FLBPluginConfigKey(plugin, "Bucket"),
		"prefix":  output.FLBPluginConfigKey(plugin, "Prefix"),
		"jsonKey": output.FLBPluginConfigKey(plugin, "JSON_Key"),
	}

	pluginContext := &PluginContext{
		LastFlushTime: time.Now(),
		Config:        cfg,
	}
	output.FLBPluginSetContext(plugin, pluginContext)

	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	// Type assert context back into the original type for the Go variable
	values := output.FLBPluginGetContext(ctx).(*PluginContext)

	log.Printf("[event] Flush called %s, %v\n", values.Config["bucket"], C.GoString(tag))
	dec := output.NewDecoder(data, int(length))

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

		mutex.Lock()
		values.Buffer.Write(line)
		values.Buffer.Write([]byte("\n"))
		values.CurrentBufferSize += len(line) + 1

		if values.CurrentBufferSize >= bufferSize {
			if err := flushBuffer(values, C.GoString(tag)); err != nil {
				mutex.Unlock()
				return output.FLB_RETRY
			}
		}
		mutex.Unlock()
	}

	mutex.Lock()
	currentTime := time.Now()
	if currentTime.Sub(values.LastFlushTime) >= time.Minute {
		if err := flushBuffer(values, C.GoString(tag)); err != nil {
			mutex.Unlock()
			return output.FLB_RETRY
		}
	}
	mutex.Unlock()
	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later
	return output.FLB_OK
}

func flushBuffer(values *PluginContext, tag string) error {
	log.Printf("[event] Flushing buffer %s, %v\n", values.Config["bucket"], tag)
	if values.Buffer.Len() > 0 {
		var gzipBuffer bytes.Buffer
		zw := gzip.NewWriter(&gzipBuffer)
		defer zw.Close()

		if _, err := zw.Write(values.Buffer.Bytes()); err != nil {
			log.Printf("[warn] error compressing data: %v\n", err)
			return err
		}
		if err := zw.Close(); err != nil {
			log.Printf("[warn] error closing gzip writer: %v\n", err)
			return err
		}

		objectKey := GenerateObjectKey(values.Config["prefix"], tag, getCurrentJstTime())
		if err = gcsClient.Write(values.Config["bucket"], objectKey, &gzipBuffer); err != nil {
			log.Printf("[warn] error sending message in GCS: %v\n", err)
		}

		values.Buffer.Reset()
		values.CurrentBufferSize = 0
		values.LastFlushTime = time.Now()
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
