FROM golang:1.22.1-bullseye

WORKDIR /app
COPY go.mod ./
COPY go.sum ./

RUN go mod download