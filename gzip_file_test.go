package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// TestFileStorageWithGzip はファイル出力モードでのgzip圧縮データの書き込みをテストする
func TestFileStorageWithGzip(t *testing.T) {
	// テスト用のディレクトリを作成
	testDir := "/tmp/test-gcs-file-mode-gzip"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir) // テスト終了後にクリーンアップ

	// ファイル出力クライアントを作成
	client, err := NewClient(string(StorageTypeFile), testDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// テスト用のgzip圧縮データを準備
	testData := []byte(`{"test":"data"}`)
	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	_, err = gzipWriter.Write(testData)
	if err != nil {
		t.Fatalf("Failed to compress test data: %v", err)
	}
	gzipWriter.Close()

	// 圧縮データをWriteメソッドで書き込む
	bucket := "test-bucket"
	objectKey := "test/2025/05/14/123456_test.log.gz"
	err = client.Write(bucket, objectKey, bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to write gzipped data: %v", err)
	}

	// 書き込まれたファイルを確認
	filePath := filepath.Join(testDir, bucket, objectKey)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("Output file does not exist: %s", filePath)
	}

	// ファイルの内容を読み取る
	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// gzip圧縮されたデータと一致するか確認
	if !bytes.Equal(fileContent, buf.Bytes()) {
		t.Errorf("File content does not match original gzipped data")
	}

	// gzip解凍して元のデータと一致するか確認
	gzipReader, err := gzip.NewReader(bytes.NewReader(fileContent))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	decompressedContent, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("Failed to decompress file content: %v", err)
	}
	gzipReader.Close()

	if !bytes.Equal(decompressedContent, testData) {
		t.Errorf("Decompressed content does not match original data")
	}
}
