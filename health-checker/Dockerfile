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

# ランタイム前処理を 1 レイヤに統合する:
#   - `ca-certificates` を追加 (TLS 検証で必要)
#   - 非 root 実行用ユーザ (CIS Docker Benchmark 4.1 準拠) を作成
# `hadolint DL3059` (連続 RUN の統合) を満たすため、両処理を `&&` で連結。
# パッケージバージョンの pin (`DL3018`) はリポジトリの `.hadolint.yaml` で ignore。
# `-D`: パスワードなし、`-H`: ホームディレクトリを作らない、`-s /sbin/nologin`: シェル無効
RUN apk --no-cache add ca-certificates \
    && adduser -D -H -s /sbin/nologin app

WORKDIR /app
COPY --from=builder --chown=app:app /app/health-checker .

# 以降のプロセスは非 root ユーザで実行する。
USER app

EXPOSE 8002

CMD ["./health-checker"]
