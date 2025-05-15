build-docker:
	docker build -t go-gcs-builder:latest -f ./builder/Dockerfile .

build-library:
	docker run --rm -v $(PWD):/app go-gcs-builder:latest /bin/sh -c "go build -buildmode=c-shared -o build/out_gcs.so *.go"

# ローカル環境用ビルド
build:
	mkdir -p build
	go build -buildmode=c-shared -o build/out_gcs.so *.go

# amd64向けビルド
build-amd64:
	mkdir -p build/amd64
	GOOS=linux GOARCH=amd64 go build -buildmode=c-shared -o build/amd64/out_gcs.so *.go

# arm64向けビルド
build-arm64:
	mkdir -p build/arm64
	GOOS=linux GOARCH=arm64 go build -buildmode=c-shared -o build/arm64/out_gcs.so *.go

# 両アーキテクチャ向けビルド
build-all: build-amd64 build-arm64

# 異なるアーキテクチャ向けのビルド用コンテナを使用
build-amd64-docker:
	docker run --rm -v $(PWD):/app --platform=linux/amd64 golang:1.22.1 /bin/sh -c "cd /app && mkdir -p build/amd64 && CGO_ENABLED=1 go build -buildmode=c-shared -o build/amd64/out_gcs.so *.go"

build-arm64-docker:
	docker run --rm -v $(PWD):/app --platform=linux/arm64 golang:1.22.1 /bin/sh -c "cd /app && mkdir -p build/arm64 && CGO_ENABLED=1 go build -buildmode=c-shared -o build/arm64/out_gcs.so *.go"

# 両アーキテクチャ向けビルド
build-all-docker: build-amd64-docker build-arm64-docker

clean:
	go clean
	rm -rf ./build

test:
	PATH="${PWD}/bin:${PWD}/test/bin:${PATH}" go test ${TEST_FLAGS} $(shell go list ./... | sort -u)
