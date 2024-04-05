package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestGenerateObjectKey(t *testing.T) {

	prefix := "daily"
	tag := "event_log"
	timestamp := time.Now()
	year, month, day := timestamp.Date()

	expected := fmt.Sprintf("%s/%s/%04d/%02d/%02d/", prefix, tag, year, month, day)

	if got := GenerateObjectKey(prefix, tag, timestamp); !strings.Contains(got, expected) {
		t.Errorf("GenerateObjectKey() = %v, want %v", got, expected)
	}
}
