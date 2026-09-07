# syntax=docker/dockerfile:1

# ---------- Stage 1: builder ----------
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY *.go ./
# CGO を無効化してスタティックリンクした Linux バイナリを生成する。
# 実行イメージ (alpine) には glibc が入らないため、動的リンクだと動かない。
RUN CGO_ENABLED=0 GOOS=linux go build -o health-checker .

# ---------- Stage 2: runtime ----------
FROM alpine:3.20
RUN apk --no-cache add ca-certificates

# コンテナ内プロセスを非 root で実行するための共通ユーザ。
# CIS Docker Benchmark 4.1 "Ensure that a user for the container has been created"
# に従い、root による特権実行を回避する。
# -D: パスワードなし, -H: ホームディレクトリを作らない, -s /sbin/nologin: シェル無効
RUN adduser -D -H -s /sbin/nologin app

WORKDIR /app
COPY --from=builder --chown=app:app /app/health-checker .

# 以降のプロセスは非 root ユーザで実行する。
USER app

EXPOSE 8002

CMD ["./health-checker"]
