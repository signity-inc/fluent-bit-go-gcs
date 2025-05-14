package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"sync"
)

// mockDataはstorage.goで定義されています

// MockStorageClientForTest はテストのためのモッククライアントを提供するインターフェース
type MockStorageClientForTest interface {
	StorageClient
	SetWriteFunc(fn func(bucket, object string, content io.Reader) error)
	SetFailureConfig(bucket, object string, shouldFail bool)
	GetCallCount(bucket, object string) int
	GetWrittenData(bucket, object string) []byte
	ResetData()
}

// NewMockStorageClient は新しいモックストレージクライアントを作成します
func NewMockStorageClient() MockStorageClientForTest {
	return &MockStorageClientImpl{
		writtenData:   make(map[string][]byte),
		callCount:     make(map[string]int),
		failureConfig: make(map[string]bool),
	}
}

// MockStorageClientImpl はモッククライアントの実装です
type MockStorageClientImpl struct {
	mutex         sync.Mutex
	writeFunc     func(bucket, object string, content io.Reader) error
	writtenData   map[string][]byte
	callCount     map[string]int
	failureConfig map[string]bool
}

// Write はバケットとオブジェクトにデータを書き込みます（モック）
func (m *MockStorageClientImpl) Write(bucket, objectKey string, data io.Reader) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := bucket + "/" + objectKey
	m.callCount[key]++
	
	// 特定のキーに対して失敗を設定している場合はエラーを返す
	if m.failureConfig[key] {
		return errors.New("mock storage client write error")
	}
	
	// カスタムWrite関数が設定されている場合はそれを使用
	if m.writeFunc != nil {
		return m.writeFunc(bucket, objectKey, data)
	}
	
	// デフォルトの実装：データを読み込みメモリに保存
	content, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	m.writtenData[key] = content
	return nil
}

// Close はモッククライアントのリソースを解放します
func (m *MockStorageClientImpl) Close() error {
	return nil
}

// SetWriteFunc はカスタムの書き込み関数を設定します
func (m *MockStorageClientImpl) SetWriteFunc(fn func(bucket, object string, content io.Reader) error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.writeFunc = fn
}

// SetFailureConfig は特定のキーに対して失敗を設定します
func (m *MockStorageClientImpl) SetFailureConfig(bucket, object string, shouldFail bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := bucket + "/" + object
	m.failureConfig[key] = shouldFail
}

// GetCallCount は特定のキーに対する呼び出し回数を取得します
func (m *MockStorageClientImpl) GetCallCount(bucket, object string) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := bucket + "/" + object
	return m.callCount[key]
}

// GetWrittenData は特定のキーに書き込まれたデータを取得します
func (m *MockStorageClientImpl) GetWrittenData(bucket, object string) []byte {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	key := bucket + "/" + object
	return m.writtenData[key]
}

// ResetData はモックの状態をリセットします
func (m *MockStorageClientImpl) ResetData() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.writtenData = make(map[string][]byte)
	m.callCount = make(map[string]int)
	m.failureConfig = make(map[string]bool)
	m.writeFunc = nil
}

// 以下はレガシーコードと互換性を持たせるための関数です

// NewMockClient モッククライアントを作成します（レガシーAPI互換）
func NewMockClient() Client {
	return Client{
		CTX: context.Background(),
		GCS: nil,
		StorageType: string(StorageTypeGCS),
		mockData: &mockData{
			writtenData:   make(map[string][]byte),
			callCount:     make(map[string]int),
			failureConfig: make(map[string]bool),
			isMock:        true,
		},
	}
}

// SetMockGlobalFailure すべての書き込みで失敗するよう設定（レガシーAPI互換）
func SetMockGlobalFailure(c Client, shouldFail bool) {
	if c.mockData == nil || !c.mockData.isMock {
		return
	}

	c.mockData.writeFunc = func(bucket, object string, content io.Reader) error {
		if shouldFail {
			return errors.New("mock GCS global write error")
		}
		data, err := ioutil.ReadAll(content)
		if err != nil {
			return err
		}
		c.mockData.mutex.Lock()
		defer c.mockData.mutex.Unlock()
		key := bucket + "/" + object
		c.mockData.writtenData[key] = data
		return nil
	}
}

// GetMockWrittenData モックに書き込まれたデータを取得（レガシーAPI互換）
func GetMockWrittenData(c Client, bucket, object string) ([]byte, bool) {
	if c.mockData == nil || !c.mockData.isMock {
		return nil, false
	}

	c.mockData.mutex.Lock()
	defer c.mockData.mutex.Unlock()
	key := bucket + "/" + object
	data, exists := c.mockData.writtenData[key]
	return data, exists
}

// GetMockCallCount モックの呼び出し回数を取得（レガシーAPI互換）
func GetMockCallCount(c Client, bucket, object string) int {
	if c.mockData == nil || !c.mockData.isMock {
		return 0
	}

	c.mockData.mutex.Lock()
	defer c.mockData.mutex.Unlock()
	key := bucket + "/" + object
	return c.mockData.callCount[key]
}

// SetMockWriteFunction カスタム書き込み関数を設定（レガシーAPI互換）
func SetMockWriteFunction(c Client, fn func(bucket, object string, content io.Reader) error) {
	if c.mockData == nil || !c.mockData.isMock {
		return
	}
	c.mockData.writeFunc = fn
}

// GetMockWrittenDataMap モックに書き込まれたすべてのデータマップを取得（レガシーAPI互換）
func GetMockWrittenDataMap(c Client) map[string][]byte {
	if c.mockData == nil || !c.mockData.isMock {
		return nil
	}
	c.mockData.mutex.Lock()
	defer c.mockData.mutex.Unlock()
	
	// 防御的コピーを作成
	result := make(map[string][]byte)
	for k, v := range c.mockData.writtenData {
		dataCopy := make([]byte, len(v))
		copy(dataCopy, v)
		result[k] = dataCopy
	}
	return result
}

// ResetMock モックの状態をリセット（レガシーAPI互換）
func ResetMock(c Client) {
	if c.mockData == nil || !c.mockData.isMock {
		return
	}
	c.mockData.mutex.Lock()
	defer c.mockData.mutex.Unlock()
	c.mockData.writtenData = make(map[string][]byte)
	c.mockData.callCount = make(map[string]int)
	c.mockData.failureConfig = make(map[string]bool)
	c.mockData.writeFunc = nil
}