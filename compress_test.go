package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
)

func TestCompressData(t *testing.T) {
	// テスト用のコンテキスト作成
	ctx := &PluginContext{}
	
	testCases := []struct {
		name     string
		input    []byte
		wantErr  bool
		validate func(t *testing.T, compressed *bytes.Buffer)
	}{
		{
			name:    "通常のテキストデータ",
			input:   []byte("これはテストデータです。This is test data."),
			wantErr: false,
			validate: func(t *testing.T, compressed *bytes.Buffer) {
				// GZIPデータの解凍をテスト
				validateGzipCompression(t, compressed, "これはテストデータです。This is test data.")
			},
		},
		{
			name:    "大きなデータ",
			input:   []byte(strings.Repeat("大きなテストデータ", 1000)), // 約15KBのデータ
			wantErr: false,
			validate: func(t *testing.T, compressed *bytes.Buffer) {
				validateGzipCompression(t, compressed, strings.Repeat("大きなテストデータ", 1000))
			},
		},
		{
			name:    "空のデータ",
			input:   []byte{},
			wantErr: false,
			validate: func(t *testing.T, compressed *bytes.Buffer) {
				validateGzipCompression(t, compressed, "")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := ctx.compressData(tc.input)
			if (err != nil) != tc.wantErr {
				t.Errorf("compressData() error = %v, wantErr %v", err, tc.wantErr)
				return
			}

			if !tc.wantErr && compressed == nil {
				t.Errorf("compressData() returned nil buffer without error")
				return
			}

			if !tc.wantErr {
				// 圧縮データの検証
				tc.validate(t, compressed)
			}
		})
	}
}

// validateGzipCompression はGZIP圧縮データが正しく解凍できることを検証
func validateGzipCompression(t *testing.T, compressed *bytes.Buffer, expected string) {
	// GZIPリーダーの作成
	gr, err := gzip.NewReader(compressed)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gr.Close()

	// 解凍データの読み込み
	decompressed, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("failed to read decompressed data: %v", err)
	}

	// 解凍データの検証
	if string(decompressed) != expected {
		t.Errorf("decompressed data does not match input. got: %s, want: %s", string(decompressed), expected)
	}
}

// メモリリークテスト
func TestCompressDataNoLeak(t *testing.T) {
	ctx := &PluginContext{}
	data := []byte(strings.Repeat("メモリリークテスト用データ", 1000))

	// 複数回の圧縮テスト（リークがあると失敗する可能性が高い）
	for i := 0; i < 100; i++ {
		compressed, err := ctx.compressData(data)
		if err != nil {
			t.Fatalf("compression failed on iteration %d: %v", i, err)
		}
		
		// 圧縮データの検証
		gr, err := gzip.NewReader(compressed)
		if err != nil {
			t.Fatalf("failed to create gzip reader on iteration %d: %v", i, err)
		}
		
		// リーダーを必ず閉じる
		gr.Close()
	}
}