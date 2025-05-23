FROM golang:1.22.1-bullseye

WORKDIR /app
COPY go.mod ./
COPY go.sum ./

RUN go mod download

# クロスコンパイルに必要なパッケージをインストール
RUN apt-get update && apt-get install -y \
    gcc-aarch64-linux-gnu \
    g++-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    gcc-x86-64-linux-gnu \
    g++-x86-64-linux-gnu \
    libc6-dev-amd64-cross \
    && rm -rf /var/lib/apt/lists/*

# 環境変数を設定
ENV CC_ARM64=aarch64-linux-gnu-gcc
ENV CXX_ARM64=aarch64-linux-gnu-g++
ENV CC_AMD64=x86_64-linux-gnu-gcc
ENV CXX_AMD64=x86_64-linux-gnu-g++