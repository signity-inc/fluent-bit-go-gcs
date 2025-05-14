package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFileOutputMode はファイル出力モードの基本的な機能をテストする
func TestFileOutputMode(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-file-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト終了後に削除

	// ファイル出力モードのクライアントを作成
	client, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// テスト用のバケット名とオブジェクトキー
	bucket := "test-bucket"
	objectKey := "test-prefix/test-tag/2023/01/01/1672531200_test.log.gz"

	// テスト用のコンテンツを準備（GZIPで圧縮）
	var contentBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&contentBuf)
	testData := "test log line 1\ntest log line 2\ntest log line 3"
	_, err = gzipWriter.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Failed to write gzip data: %v", err)
	}
	err = gzipWriter.Close()
	if err != nil {
		t.Fatalf("Failed to close gzip writer: %v", err)
	}

	// Write関数でファイルを書き込み
	err = client.Write(bucket, objectKey, bytes.NewReader(contentBuf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// ファイルが正しく作成されたか確認
	expectedFilePath := filepath.Join(tempDir, bucket, objectKey)
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("Expected file not created: %s", expectedFilePath)
	}

	// ファイルの内容を検証
	fileContent, err := ioutil.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// GZIPを解凍して中身を確認
	gzipReader, err := gzip.NewReader(bytes.NewReader(fileContent))
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	decompressedContent, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		t.Fatalf("Failed to read gzip content: %v", err)
	}

	// 元のデータと一致するか確認
	if string(decompressedContent) != testData {
		t.Errorf("File content mismatch. Expected: %s, Got: %s", testData, string(decompressedContent))
	}
}

// TestFileOutputModeDirectoryStructure はファイル出力モードで作成されるディレクトリ構造をテストする
func TestFileOutputModeDirectoryStructure(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-directory-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト終了後に削除

	// ファイル出力モードのクライアントを作成
	client, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// テスト用の階層化されたオブジェクトキー
	bucket := "nested-bucket"
	objectKey := "level1/level2/level3/test.log.gz"

	// テスト用のコンテンツ
	testContent := "test content for nested directories"
	var contentBuf bytes.Buffer
	gzipWriter := gzip.NewWriter(&contentBuf)
	_, err = gzipWriter.Write([]byte(testContent))
	if err != nil {
		t.Fatalf("Failed to write gzip data: %v", err)
	}
	err = gzipWriter.Close()
	if err != nil {
		t.Fatalf("Failed to close gzip writer: %v", err)
	}

	// Write関数でファイルを書き込み
	err = client.Write(bucket, objectKey, bytes.NewReader(contentBuf.Bytes()))
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// 全ての親ディレクトリが作成されたか確認
	expectedFilePath := filepath.Join(tempDir, bucket, objectKey)
	if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
		t.Errorf("Expected file not created: %s", expectedFilePath)
	}

	// 各階層のディレクトリが作成されたか確認
	dirs := []string{
		filepath.Join(tempDir, bucket),
		filepath.Join(tempDir, bucket, "level1"),
		filepath.Join(tempDir, bucket, "level1/level2"),
		filepath.Join(tempDir, bucket, "level1/level2/level3"),
	}

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Expected directory not created: %s", dir)
		}
	}
}

// TestFileOutputModeMultipleWrites は複数のファイル書き込みをテストする
func TestFileOutputModeMultipleWrites(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-multiple-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // テスト終了後に削除

	// ファイル出力モードのクライアントを作成
	client, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// テスト用のファイル情報
	bucket := "multi-bucket"
	testFiles := []struct {
		objectKey string
		content   string
	}{
		{
			objectKey: "prefix1/tag1/file1.log.gz",
			content:   "content for file 1",
		},
		{
			objectKey: "prefix1/tag2/file2.log.gz",
			content:   "content for file 2",
		},
		{
			objectKey: "prefix2/tag1/file3.log.gz",
			content:   "content for file 3",
		},
	}

	// 複数のファイルを書き込み
	for _, tf := range testFiles {
		var contentBuf bytes.Buffer
		gzipWriter := gzip.NewWriter(&contentBuf)
		_, err = gzipWriter.Write([]byte(tf.content))
		if err != nil {
			t.Fatalf("Failed to write gzip data: %v", err)
		}
		err = gzipWriter.Close()
		if err != nil {
			t.Fatalf("Failed to close gzip writer: %v", err)
		}

		err = client.Write(bucket, tf.objectKey, bytes.NewReader(contentBuf.Bytes()))
		if err != nil {
			t.Fatalf("Failed to write file %s: %v", tf.objectKey, err)
		}
	}

	// すべてのファイルが作成されたか確認
	for _, tf := range testFiles {
		expectedFilePath := filepath.Join(tempDir, bucket, tf.objectKey)
		if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
			t.Errorf("Expected file not created: %s", expectedFilePath)
			continue
		}

		// ファイルの内容を検証
		fileContent, err := ioutil.ReadFile(expectedFilePath)
		if err != nil {
			t.Errorf("Failed to read output file %s: %v", tf.objectKey, err)
			continue
		}

		// GZIPを解凍して中身を確認
		gzipReader, err := gzip.NewReader(bytes.NewReader(fileContent))
		if err != nil {
			t.Errorf("Failed to create gzip reader for %s: %v", tf.objectKey, err)
			continue
		}

		decompressedContent, err := ioutil.ReadAll(gzipReader)
		gzipReader.Close()
		if err != nil {
			t.Errorf("Failed to read gzip content for %s: %v", tf.objectKey, err)
			continue
		}

		// 元のデータと一致するか確認
		if string(decompressedContent) != tf.content {
			t.Errorf("File content mismatch for %s. Expected: %s, Got: %s",
				tf.objectKey, tf.content, string(decompressedContent))
		}
	}

	// ディレクトリ内のファイル数をカウント
	var fileCount int
	err = filepath.Walk(filepath.Join(tempDir, bucket), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk directory: %v", err)
	}

	// ファイル数が期待値と一致するか確認
	if fileCount != len(testFiles) {
		t.Errorf("Expected %d files, but found %d", len(testFiles), fileCount)
	}
}

// TestFileOutputModeError はエラーケースをテストする
func TestFileOutputModeError(t *testing.T) {
	// 無効なディレクトリでの初期化テスト
	_, err := NewClient(string(StorageTypeFile), "")
	if err == nil {
		t.Error("Expected error when initializing with empty directory, but got nil")
	}

	// 読み取り専用ディレクトリでのテスト
	// 注: このテストはOSの権限によっては一部環境で失敗する可能性があります
	tempDir, err := ioutil.TempDir("", "fluent-bit-readonly-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Linuxの場合のみ実行（権限の操作がOS依存）
	if os.Getenv("SKIP_PERMISSION_TEST") != "true" {
		// ディレクトリを読み取り専用に変更
		err = os.Chmod(tempDir, 0500) // r-x------
		if err != nil {
			t.Fatalf("Failed to change directory permissions: %v", err)
		}

		client, err := NewClient(string(StorageTypeFile), tempDir)
		if err != nil {
			t.Fatalf("Failed to create file output client: %v", err)
		}

		// 書き込みを試みる（失敗するはず）
		testContent := "test content for permission error"
		var contentBuf bytes.Buffer
		gzipWriter := gzip.NewWriter(&contentBuf)
		_, err = gzipWriter.Write([]byte(testContent))
		if err != nil {
			t.Fatalf("Failed to write gzip data: %v", err)
		}
		err = gzipWriter.Close()
		if err != nil {
			t.Fatalf("Failed to close gzip writer: %v", err)
		}

		err = client.Write("test-bucket", "test-file.log.gz", bytes.NewReader(contentBuf.Bytes()))
		if err == nil {
			t.Error("Expected error when writing to read-only directory, but got nil")
		} else if !strings.Contains(err.Error(), "permission") &&
			!strings.Contains(err.Error(), "denied") {
			t.Errorf("Expected permission error, but got: %v", err)
		}

		// 権限を戻す
		err = os.Chmod(tempDir, 0700)
		if err != nil {
			t.Fatalf("Failed to restore directory permissions: %v", err)
		}
	}
}

// TestClientStorageTypeDetection はストレージタイプの検出をテストする
func TestClientStorageTypeDetection(t *testing.T) {
	// ファイル出力モードのクライアントを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-type-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileClient, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// ストレージタイプが正しく設定されているか確認
	if fileClient.StorageType != string(StorageTypeFile) {
		t.Errorf("Expected storage type %s, but got %s", string(StorageTypeFile), fileClient.StorageType)
	}

	// GCSクライアントはテスト環境に依存するため、モックを使用
	mockClient := NewMockClient()
	if mockClient.StorageType != string(StorageTypeGCS) {
		t.Errorf("Expected mock storage type %s, but got %s", string(StorageTypeGCS), mockClient.StorageType)
	}
}

// TestFilePathsWithSpecialCharacters は特殊文字を含むファイルパスのテスト
func TestFilePathsWithSpecialCharacters(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-special-chars")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// ファイル出力モードのクライアントを作成
	client, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// 特殊文字を含むオブジェクトキー
	testCases := []struct {
		name      string
		objectKey string
		content   string
	}{
		{
			name:      "spaces in path",
			objectKey: "prefix with space/tag with space/file with space.log.gz",
			content:   "content for file with spaces",
		},
		{
			name:      "dashes and underscores",
			objectKey: "prefix-with-dash/tag_with_underscore/file-with-dash_underscore.log.gz",
			content:   "content for file with dashes and underscores",
		},
		{
			name:      "dots in path",
			objectKey: "prefix.with.dots/tag.with.dots/file.with.dots.log.gz",
			content:   "content for file with dots",
		},
	}

	// 各テストケースを実行
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// GZIPで圧縮
			var contentBuf bytes.Buffer
			gzipWriter := gzip.NewWriter(&contentBuf)
			_, err = gzipWriter.Write([]byte(tc.content))
			if err != nil {
				t.Fatalf("Failed to write gzip data: %v", err)
			}
			err = gzipWriter.Close()
			if err != nil {
				t.Fatalf("Failed to close gzip writer: %v", err)
			}

			// ファイルを書き込み
			bucket := "special-chars-bucket"
			err = client.Write(bucket, tc.objectKey, bytes.NewReader(contentBuf.Bytes()))
			if err != nil {
				t.Fatalf("Failed to write file with special chars: %v", err)
			}

			// ファイルが正しく作成されたか確認
			expectedFilePath := filepath.Join(tempDir, bucket, tc.objectKey)
			if _, err := os.Stat(expectedFilePath); os.IsNotExist(err) {
				t.Errorf("Expected file not created: %s", expectedFilePath)
				return
			}

			// ファイルの内容を検証
			fileContent, err := ioutil.ReadFile(expectedFilePath)
			if err != nil {
				t.Fatalf("Failed to read output file: %v", err)
			}

			// GZIPを解凍して中身を確認
			gzipReader, err := gzip.NewReader(bytes.NewReader(fileContent))
			if err != nil {
				t.Fatalf("Failed to create gzip reader: %v", err)
			}
			defer gzipReader.Close()

			decompressedContent, err := ioutil.ReadAll(gzipReader)
			if err != nil {
				t.Fatalf("Failed to read gzip content: %v", err)
			}

			// 元のデータと一致するか確認
			if string(decompressedContent) != tc.content {
				t.Errorf("File content mismatch. Expected: %s, Got: %s", tc.content, string(decompressedContent))
			}
		})
	}
}

// TestConcurrentFileWrites は並行書き込みのテスト
func TestConcurrentFileWrites(t *testing.T) {
	// テスト用の一時ディレクトリを作成
	tempDir, err := ioutil.TempDir("", "fluent-bit-concurrent")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// ファイル出力モードのクライアントを作成
	client, err := NewClient(string(StorageTypeFile), tempDir)
	if err != nil {
		t.Fatalf("Failed to create file output client: %v", err)
	}

	// テスト用のデータを準備
	bucket := "concurrent-bucket"
	const numGoroutines = 10
	const filesPerGoroutine = 5

	// チャネルを使って完了を待機
	done := make(chan bool, numGoroutines)

	// 複数のゴルーチンでファイル書き込みを実行
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			for j := 0; j < filesPerGoroutine; j++ {
				// ユニークなオブジェクトキーを生成
				objectKey := fmt.Sprintf("prefix%d/tag%d/file%d.log.gz", routineID, j, routineID*filesPerGoroutine+j)

				// コンテンツを生成
				content := fmt.Sprintf("content for routine %d, file %d", routineID, j)

				// GZIPで圧縮
				var contentBuf bytes.Buffer
				gzipWriter := gzip.NewWriter(&contentBuf)
				_, err := gzipWriter.Write([]byte(content))
				if err != nil {
					t.Errorf("Failed to write gzip data: %v", err)
					done <- false
					return
				}
				err = gzipWriter.Close()
				if err != nil {
					t.Errorf("Failed to close gzip writer: %v", err)
					done <- false
					return
				}

				// ファイルを書き込み
				err = client.Write(bucket, objectKey, bytes.NewReader(contentBuf.Bytes()))
				if err != nil {
					t.Errorf("Goroutine %d failed to write file %s: %v", routineID, objectKey, err)
					done <- false
					return
				}
			}
			done <- true
		}(i)
	}

	// すべてのゴルーチンの完了を待機
	for i := 0; i < numGoroutines; i++ {
		success := <-done
		if !success {
			t.Errorf("One or more goroutines failed")
		}
	}

	// ファイル数をカウント
	var fileCount int
	err = filepath.Walk(filepath.Join(tempDir, bucket), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to walk directory: %v", err)
	}

	// 期待されるファイル数と一致するか確認
	expectedFileCount := numGoroutines * filesPerGoroutine
	if fileCount != expectedFileCount {
		t.Errorf("Expected %d files, but found %d", expectedFileCount, fileCount)
	}
}
