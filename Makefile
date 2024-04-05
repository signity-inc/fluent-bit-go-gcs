build-docker:
	docker build -t go-gcs-builder:latest -f ./builder/Dockerfile .

build-library:
	docker run --rm -v $(PWD):/app go-gcs-builder:latest /bin/sh -c "go build -buildmode=c-shared -o build/out_gcs.so out_gcs.go storage.go"

clean:
	go clean
	rm -rf ./build

test:
	PATH="${PWD}/bin:${PWD}/test/bin:${PATH}" go test ${TEST_FLAGS} $(shell go list ./... | sort -u)
