package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
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
	CTX      context.Context
	GCS      *storage.Client
	mockData *mockData // テスト用モックデータ
}

// NewClient Google Cloud
func NewClient() (Client, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return Client{}, err
	}

	return Client{
		CTX: ctx,
		GCS: client,
	}, nil
}

// Write content in object GCS
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

	// 実際のGCSクライアントを使用
	wc := c.GCS.Bucket(bucket).Object(object).NewWriter(c.CTX)
	if _, err := io.Copy(wc, content); err != nil {
		return err
	}

	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}