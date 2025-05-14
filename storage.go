package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"cloud.google.com/go/storage"
)

// mockData モックデータを格納する構造体
type mockData struct {
	writeFunc     func(bucket, object string, content io.Reader) error
	writtenData   map[string][]byte
	callCount     map[string]int
	failureConfig map[string]bool
	isMock        bool
	mutex         sync.Mutex
}

// Client & Context Google Cloud
type Client struct {
	CTX           context.Context
	GCS           *storage.Client
	StorageType   string
	FileOutputDir string    // ファイル出力用ディレクトリ
	mockData      *mockData // テスト用モックデータ
}

// NewClient は新しいクライアントを作成します（レガシーAPI互換）
func NewClient(storageType string, fileOutputDir string) (Client, error) {
	ctx := context.Background()

	switch storageType {
	case string(StorageTypeGCS):
		client, err := storage.NewClient(ctx)
		if err != nil {
			return Client{}, err
		}

		return Client{
			CTX:         ctx,
			GCS:         client,
			StorageType: string(StorageTypeGCS),
		}, nil

	case string(StorageTypeFile):
		// ファイル出力モード
		if fileOutputDir == "" {
			return Client{}, errors.New("file output directory not specified")
		}

		// 出力ディレクトリが存在することを確認
		if err := os.MkdirAll(fileOutputDir, 0755); err != nil {
			return Client{}, fmt.Errorf("failed to create output directory: %w", err)
		}

		log.Printf("[info] File output mode initialized with directory: %s", fileOutputDir)
		return Client{
			CTX:           ctx,
			StorageType:   string(StorageTypeFile),
			FileOutputDir: fileOutputDir,
		}, nil

	default:
		return Client{}, fmt.Errorf("unknown storage type: %s", storageType)
	}
}

// Write はレガシーAPIを使用してデータを書き込みます（レガシーAPI互換）
func (c Client) Write(bucket, object string, content io.Reader) error {
	// モックモードの場合
	if c.mockData != nil && c.mockData.isMock {
		c.mockData.mutex.Lock()
		defer c.mockData.mutex.Unlock()

		key := bucket + "/" + object
		c.mockData.callCount[key]++

		// 特定のキーに対して失敗を設定している場合はエラーを返す
		if c.mockData.failureConfig[key] {
			return errors.New("mock GCS client write error")
		}

		// カスタムWrite関数が設定されている場合はそれを使用
		if c.mockData.writeFunc != nil {
			return c.mockData.writeFunc(bucket, object, content)
		}

		// デフォルトの実装：データを読み込みメモリに保存
		data, err := ioutil.ReadAll(content)
		if err != nil {
			return err
		}
		c.mockData.writtenData[key] = data
		return nil
	}

	switch c.StorageType {
	case string(StorageTypeGCS):
		// 実際のGCSクライアントを使用
		wc := c.GCS.Bucket(bucket).Object(object).NewWriter(c.CTX)
		if _, err := io.Copy(wc, content); err != nil {
			return err
		}

		if err := wc.Close(); err != nil {
			return err
		}

		return nil

	case string(StorageTypeFile):
		// ファイル出力モード

		// バケット用ディレクトリの作成
		bucketDir := filepath.Join(c.FileOutputDir, bucket)
		if err := os.MkdirAll(bucketDir, 0755); err != nil {
			return fmt.Errorf("failed to create bucket directory: %w", err)
		}

		// オブジェクトキーからファイルパスを生成
		// スラッシュを含むキーもサポート（サブディレクトリ作成）
		filePath := filepath.Join(bucketDir, object)

		// サブディレクトリが必要な場合は作成
		fileDir := filepath.Dir(filePath)
		if err := os.MkdirAll(fileDir, 0755); err != nil {
			return fmt.Errorf("failed to create directories for object: %w", err)
		}

		// ファイル作成
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()

		// contentをファイルに書き込み（gzipファイルはそのまま書き込まれる）
		n, err := io.Copy(file, content)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		log.Printf("[info] File written successfully: %s (%d bytes)", filePath, n)
		return nil

	default:
		return fmt.Errorf("unknown storage type: %s", c.StorageType)
	}
}