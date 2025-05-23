build-docker:
	docker build -t go-gcs-builder:latest -f ./builder/Dockerfile .

build-library:
	docker run --rm -v $(PWD):/app go-gcs-builder:latest /bin/sh -c "go build -buildmode=c-shared -o build/out_gcs.so *.go"

# ローカル環境用ビルド
build:
	mkdir -p build
	go build -buildmode=c-shared -o build/out_gcs.so *.go

# クロスプラットフォームビルド
build-all: build-linux-amd64 build-linux-arm64

# Linux AMD64ビルド
build-linux-amd64:
	mkdir -p build/linux/amd64
	docker run --rm -v $(PWD):/app go-gcs-builder:latest bash -c 'GOOS=linux GOARCH=amd64 CGO_ENABLED=1 CC=x86_64-linux-gnu-gcc go build -buildmode=c-shared -o build/linux/amd64/out_gcs.so *.go'

# Linux ARM64ビルド
build-linux-arm64:
	mkdir -p build/linux/arm64
	docker run --rm -v $(PWD):/app go-gcs-builder:latest bash -c 'GOOS=linux GOARCH=arm64 CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc go build -buildmode=c-shared -o build/linux/arm64/out_gcs.so *.go'

clean:
	go clean
	rm -rf ./build

test:
	PATH="${PWD}/bin:${PWD}/test/bin:${PATH}" go test ${TEST_FLAGS} $(shell go list ./... | sort -u)
