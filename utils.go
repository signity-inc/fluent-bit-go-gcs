package main

import (
	"strings"
)

// isRetryableError はエラーがリトライ可能かどうかを判定します
// 一時的なエラー（ネットワーク、タイムアウトなど）はリトライ可能とし、
// 永続的なエラー（権限、認証など）はリトライ不可とします
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := strings.ToLower(err.Error())
	
	// 永続的なエラーパターン（先にチェック）
	permanentPatterns := []string{
		"permission",
		"forbidden",
		"unauthorized",
		"invalid",
		"not found",
		"401",
		"403",
		"404",
	}
	
	for _, pattern := range permanentPatterns {
		if strings.Contains(errStr, pattern) {
			return false
		}
	}
	
	// リトライ可能なエラーパターン
	retryablePatterns := []string{
		"connection",
		"timeout",
		"temporary",
		"unavailable",
		"too many requests",
		"rate limit",
		"retry",
		"503",
		"502",
		"504",
	}
	
	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	
	// デフォルトはリトライ可能とする（安全側に倒す）
	return true
}