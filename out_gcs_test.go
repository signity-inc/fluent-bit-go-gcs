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

func TestGetCurrentJstTime(t *testing.T) {
	now := time.Now()
	_, offset := now.Zone()
	jst := time.FixedZone("JST", 9*60*60)

	if offset == 0 {
		expected := now.In(jst)
		if got := getCurrentJstTime(); !strings.Contains(fmt.Sprintf("%v", got), "JST") {
			t.Errorf("GetCurrentJstTime() = %v, want %v", got, expected)
		}
	} else {
		expected := now
		if got := getCurrentJstTime(); !strings.Contains(fmt.Sprintf("%v", got), "JST") {
			t.Errorf("GetCurrentJstTime() = %v, want %v", got, expected)
		}
	}
}
