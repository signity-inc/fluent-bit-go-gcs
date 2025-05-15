FROM golang:1.22.1-bullseye

# クロスコンパイルのサポートを追加
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    gcc-x86-64-linux-gnu \
    g++-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY go.mod ./
COPY go.sum ./

RUN go mod download

# クロスコンパイル環境変数を設定
ENV CGO_ENABLED=1