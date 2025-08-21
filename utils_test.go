package main

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsRetryableError はisRetryableError関数の判定ロジックをテスト
func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// リトライ可能なエラーパターン
		{
			name:     "connection エラー",
			err:      errors.New("connection refused"),
			expected: true,
		},
		{
			name:     "timeout エラー",
			err:      errors.New("request timeout"),
			expected: true,
		},
		{
			name:     "temporary エラー",
			err:      errors.New("temporary failure"),
			expected: true,
		},
		{
			name:     "unavailable エラー",
			err:      errors.New("service unavailable"),
			expected: true,
		},
		{
			name:     "too many requests エラー",
			err:      errors.New("too many requests"),
			expected: true,
		},
		{
			name:     "rate limit エラー",
			err:      errors.New("rate limit exceeded"),
			expected: true,
		},
		{
			name:     "retry を含むエラー",
			err:      errors.New("please retry later"),
			expected: true,
		},
		{
			name:     "503 Service Unavailable",
			err:      errors.New("HTTP 503 Service Unavailable"),
			expected: true,
		},
		{
			name:     "502 Bad Gateway",
			err:      errors.New("HTTP 502 Bad Gateway"),
			expected: true,
		},
		{
			name:     "504 Gateway Timeout",
			err:      errors.New("HTTP 504 Gateway Timeout"),
			expected: true,
		},
		
		// 永続的なエラーパターン（リトライ不可）
		{
			name:     "permission エラー",
			err:      errors.New("permission denied"),
			expected: false,
		},
		{
			name:     "forbidden エラー",
			err:      errors.New("access forbidden"),
			expected: false,
		},
		{
			name:     "unauthorized エラー",
			err:      errors.New("unauthorized access"),
			expected: false,
		},
		{
			name:     "invalid エラー",
			err:      errors.New("invalid request"),
			expected: false,
		},
		{
			name:     "not found エラー",
			err:      errors.New("resource not found"),
			expected: false,
		},
		{
			name:     "401 Unauthorized",
			err:      errors.New("HTTP 401 Unauthorized"),
			expected: false,
		},
		{
			name:     "403 Forbidden",
			err:      errors.New("HTTP 403 Forbidden"),
			expected: false,
		},
		{
			name:     "404 Not Found",
			err:      errors.New("HTTP 404 Not Found"),
			expected: false,
		},
		
		// エッジケース
		{
			name:     "nil エラー",
			err:      nil,
			expected: false,
		},
		{
			name:     "空のエラーメッセージ",
			err:      errors.New(""),
			expected: true, // デフォルトはリトライ可能
		},
		{
			name:     "未知のエラー",
			err:      errors.New("unknown error occurred"),
			expected: true, // デフォルトはリトライ可能（安全側に倒す）
		},
		
		// 優先順位のテスト（永続的パターンが優先される）
		{
			name:     "connectionとpermissionの両方を含む",
			err:      errors.New("connection failed: permission denied"),
			expected: false, // permissionが優先される
		},
		{
			name:     "retryと401の両方を含む",
			err:      errors.New("401 Unauthorized, please retry"),
			expected: false, // 401が優先される
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("isRetryableError(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsRetryableErrorCaseInsensitive は大文字小文字を区別しないことをテスト
func TestIsRetryableErrorCaseInsensitive(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{
			err:      errors.New("CONNECTION REFUSED"),
			expected: true,
		},
		{
			err:      errors.New("Connection Refused"),
			expected: true,
		},
		{
			err:      errors.New("PERMISSION DENIED"),
			expected: false,
		},
		{
			err:      errors.New("Permission Denied"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			result := isRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("isRetryableError(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

// TestIsRetryableErrorRealWorldExamples は実際のエラーメッセージをテスト
func TestIsRetryableErrorRealWorldExamples(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
		scenario string
	}{
		// GCS関連のエラー
		{
			name:     "GCS一時的なエラー",
			err:      errors.New("Post \"https://storage.googleapis.com/upload/storage/v1/b/my-bucket/o\": dial tcp: i/o timeout"),
			expected: true,
			scenario: "ネットワークタイムアウト",
		},
		{
			name:     "GCS認証エラー",
			err:      errors.New("googleapi: Error 401: Invalid Credentials, authError"),
			expected: false,
			scenario: "認証情報が無効",
		},
		{
			name:     "GCSバケット権限エラー",
			err:      errors.New("googleapi: Error 403: user@example.com does not have storage.objects.create access to the Google Cloud Storage object"),
			expected: false,
			scenario: "バケットへの書き込み権限なし",
		},
		{
			name:     "GCSバケット存在しない",
			err:      errors.New("googleapi: Error 404: The specified bucket does not exist"),
			expected: false,
			scenario: "バケットが存在しない",
		},
		{
			name:     "GCSレート制限",
			err:      errors.New("googleapi: Error 429: Too many requests. Please retry later"),
			expected: true,
			scenario: "APIレート制限に到達",
		},
		{
			name:     "GCSサービス停止",
			err:      errors.New("googleapi: Error 503: The service is currently unavailable"),
			expected: true,
			scenario: "GCSサービスの一時的な停止",
		},
		
		// ネットワーク関連
		{
			name:     "DNS解決失敗",
			err:      errors.New("dial tcp: lookup storage.googleapis.com: no such host"),
			expected: true,
			scenario: "DNS解決の一時的な失敗",
		},
		{
			name:     "接続リセット",
			err:      errors.New("read tcp 192.168.1.1:12345->172.217.0.0:443: read: connection reset by peer"),
			expected: true,
			scenario: "接続がリセットされた",
		},
		{
			name:     "接続拒否",
			err:      errors.New("dial tcp 172.217.0.0:443: connect: connection refused"),
			expected: true,
			scenario: "接続が拒否された",
		},
		
		// 圧縮関連
		{
			name:     "圧縮エラー",
			err:      fmt.Errorf("gzip compression error: %w", errors.New("write failed: disk full")),
			expected: true,
			scenario: "圧縮中のディスクフル（一時的）",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			if result != tt.expected {
				t.Errorf("シナリオ: %s\nエラー: %v\n期待値: %v, 実際: %v",
					tt.scenario, tt.err, tt.expected, result)
			}
		})
	}
}

// BenchmarkIsRetryableError はパフォーマンスを測定
func BenchmarkIsRetryableError(b *testing.B) {
	errors := []error{
		errors.New("connection timeout"),
		errors.New("permission denied"),
		errors.New("service unavailable"),
		errors.New("unauthorized"),
		errors.New("too many requests"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, err := range errors {
			isRetryableError(err)
		}
	}
}