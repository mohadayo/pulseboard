# syntax=docker/dockerfile:1

# ---------- Stage 1: builder ----------
# devDependencies (typescript / ts-node / eslint / jest 等) を含めて全依存を install し、
# TypeScript を dist/ にコンパイルする。この layer は最終イメージには含めない。
FROM node:22-alpine AS builder

WORKDIR /app

# package.json / package-lock.json をコピーして npm ci でロックファイル通り install
COPY package*.json ./
RUN npm ci

# ビルドに必要なソースと tsconfig をコピー
COPY tsconfig.json ./
COPY src/ ./src/

# TypeScript を dist/ にビルド
RUN npm run build

# ---------- Stage 2: runtime ----------
# 本番実行に必要な production 依存のみを install し、builder からコンパイル済みの
# dist/ をコピーする。TypeScript ソースや devDependencies は最終イメージに含めない。
FROM node:22-alpine AS runtime

# express / winston などが production 経路 (キャッシュ / 最適化) を使うようにする
ENV NODE_ENV=production

WORKDIR /app

# production 依存のみを install (devDependencies を除外)。
# --no-audit --no-fund で CI 中の余計な出力とネットワーク I/O を減らし、
# npm cache も削除してイメージサイズを最小化する。
COPY package*.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force

# builder からビルド成果物のみをコピー
COPY --from=builder /app/dist ./dist

# node:22-alpine にプリセットされている非 root ユーザで実行し、
# コンテナランタイム上での特権を最小化する
USER node

EXPOSE 8000

CMD ["node", "dist/index.js"]
