package main

import (
	"C"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// GenerateObjectKey : gen format object name PREFIX/YEAR/MONTH/DAY/tag/timestamp_uuid.log
func GenerateObjectKey(prefix, tag string, t time.Time) string {
	year, month, day := t.Date()
	date_str := fmt.Sprintf("%04d/%02d/%02d", year, month, day)
	fileName := fmt.Sprintf("%s/%d_%s.log.gz", date_str, t.Unix(), uuid.Must(uuid.NewRandom()).String())
	return filepath.Join(prefix, tag, fileName)
}

// getCurrentJstTime JST（日本標準時）でのタイムスタンプを取得
func getCurrentJstTime() time.Time {
	now := time.Now()
	_, offset := now.Zone()
	if offset == 0 {
		jst := time.FixedZone("JST", 9*60*60)
		return now.In(jst)
	}
	return now
}

// parseMap はmap[interface{}]interface{}をmap[string]interface{}に変換します
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

// 下記の関数はfluent_bit_plugin.goで再実装されているため、
// このファイルはレガシーなコードの一部を保持するだけです。
// これらの機能は新しいコンポーネントベースの設計に移行されました。

// 現在のout_gcs.goに実装されている関数のシグネチャは保持されていますが、
// 動作はfluent_bit_plugin.goに実装されています。このファイルは
// 後方互換性のために残されています。

// This function is commented out to avoid conflict with main in fluent_bit_plugin.go
//func main() {
//	log.Println("Using the new component-based implementation of Fluent Bit GCS Output Plugin")
//	log.Println("Please refer to the documentation for the new configuration options")
//}