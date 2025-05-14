package main

import (
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/googleapi"
)

// BackoffStrategy はリトライ間隔の計算戦略を表すインターフェース
type BackoffStrategy interface {
	// NextBackoff は次のリトライまでの待機時間を計算する
	NextBackoff(retryCount int) time.Duration
}

// ExponentialBackoff は指数関数的バックオフ戦略
type ExponentialBackoff struct {
	initialBackoff time.Duration
	maxBackoff     time.Duration
	factor         float64
}

// NewExponentialBackoff は新しい指数関数的バックオフ戦略を作成する
func NewExponentialBackoff(initial, max time.Duration, factor float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		initialBackoff: initial,
		maxBackoff:     max,
		factor:         factor,
	}
}

// NextBackoff は次のバックオフ時間を計算する
func (e *ExponentialBackoff) NextBackoff(retryCount int) time.Duration {
	// 初回リトライの場合は初期バックオフを返す
	if retryCount <= 0 {
		return e.initialBackoff
	}

	// 指数関数的にバックオフを計算
	backoff := float64(e.initialBackoff)
	for i := 0; i < retryCount; i++ {
		backoff *= e.factor
		if backoff >= float64(e.maxBackoff) {
			return e.maxBackoff
		}
	}

	return time.Duration(backoff)
}

// RetryManager はリトライ状態とロジックを管理する
type RetryManager struct {
	retryCount      int
	maxRetryCount   int
	objectKey       string
	isRetrying      bool
	backoffStrategy BackoffStrategy
	mutex           sync.Mutex
}

// NewRetryManager は新しいRetryManagerを作成する
func NewRetryManager(maxRetryCount int, backoffStrategy BackoffStrategy) *RetryManager {
	// デフォルト値の設定
	if maxRetryCount <= 0 {
		maxRetryCount = 5
	}
	
	if backoffStrategy == nil {
		backoffStrategy = NewExponentialBackoff(
			1*time.Second,    // 初期バックオフ
			1*time.Minute,    // 最大バックオフ
			2.0,              // 倍率
		)
	}

	return &RetryManager{
		maxRetryCount:   maxRetryCount,
		backoffStrategy: backoffStrategy,
	}
}

// ShouldRetry はエラーに基づいてリトライすべきかを判断する
func (r *RetryManager) ShouldRetry(err error) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if err == nil {
		return false
	}

	// 最大リトライ回数を超えている場合はリトライしない
	if r.retryCount >= r.maxRetryCount {
		return false
	}

	// エラーの種類に基づいてリトライ可能かを判断
	return isRetryableError(err)
}

// isRetryableError はエラーの種類に基づいてリトライ可能かを判断する
func isRetryableError(err error) bool {
	// 一時的なエラーや接続エラーはリトライ可能
	var tempErr interface {
		Temporary() bool
	}
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}

	// 特定のGCSエラーはリトライ可能
	var gcsErr *googleapi.Error
	if errors.As(err, &gcsErr) {
		// 500番台のサーバーエラーはリトライ可能
		if gcsErr.Code >= 500 && gcsErr.Code < 600 {
			return true
		}
		// 429 Too Many Requestsはリトライ可能
		if gcsErr.Code == 429 {
			return true
		}
	}

	// 認証エラーなどはリトライ不可
	if strings.Contains(err.Error(), "permission") ||
	   strings.Contains(err.Error(), "auth") ||
	   strings.Contains(err.Error(), "credential") {
		return false
	}

	// デフォルトはリトライ可能とする
	return true
}

// GetRetryObjectKey はリトライ用のオブジェクトキーを返す
func (r *RetryManager) GetRetryObjectKey() string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.objectKey
}

// SetRetryObjectKey はリトライ用のオブジェクトキーを設定する
func (r *RetryManager) SetRetryObjectKey(key string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.objectKey = key
}

// IncrementRetryCount はリトライカウントを増加させる
func (r *RetryManager) IncrementRetryCount() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.retryCount++
	r.isRetrying = true
}

// GetRetryCount は現在のリトライカウントを返す
func (r *RetryManager) GetRetryCount() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.retryCount
}

// IsRetrying は現在リトライ中かどうかを返す
func (r *RetryManager) IsRetrying() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.isRetrying
}

// ResetRetry はリトライ状態をリセットする
func (r *RetryManager) ResetRetry() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.retryCount = 0
	r.objectKey = ""
	r.isRetrying = false
}

// GetBackoffDuration は現在のリトライカウントに基づくバックオフ時間を返す
func (r *RetryManager) GetBackoffDuration() time.Duration {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.backoffStrategy.NextBackoff(r.retryCount)
}