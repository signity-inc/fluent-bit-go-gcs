package main

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"sync"
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

// NewMockClient モッククライアントを作成します
func NewMockClient() Client {
	return Client{
		CTX: context.Background(),
		GCS: nil,
		mockData: &mockData{
			writtenData:   make(map[string][]byte),
			callCount:     make(map[string]int),
			failureConfig: make(map[string]bool),
			isMock:        true,
		},
	}
}

// SetMockGlobalFailure すべての書き込みで失敗するよう設定
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

// GetMockWrittenData モックに書き込まれたデータを取得
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

// GetMockCallCount モックの呼び出し回数を取得
func GetMockCallCount(c Client, bucket, object string) int {
	if c.mockData == nil || !c.mockData.isMock {
		return 0
	}

	c.mockData.mutex.Lock()
	defer c.mockData.mutex.Unlock()
	key := bucket + "/" + object
	return c.mockData.callCount[key]
}

// SetMockWriteFunction カスタム書き込み関数を設定
func SetMockWriteFunction(c Client, fn func(bucket, object string, content io.Reader) error) {
	if c.mockData == nil || !c.mockData.isMock {
		return
	}
	c.mockData.writeFunc = fn
}

// GetMockWrittenDataMap モックに書き込まれたすべてのデータマップを取得
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

// ResetMock モックの状態をリセット
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