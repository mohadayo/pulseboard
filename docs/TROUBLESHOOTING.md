# トラブルシューティング

PulseBoard (`analytics-api` / `health-checker` / `api-gateway`) の
ローカル開発 / Docker Compose 実行時によく遭遇する問題と対処方法をまとめています。

アーキテクチャの全体像は [ARCHITECTURE.md](./ARCHITECTURE.md) を参照してください。

## 1. `docker compose up` が起動しない / 途中で失敗する

### 症状

```
Error response from daemon: ... port is already allocated
```

### 原因

`docker-compose.yml` で公開しているポート
(`api-gateway: 3000` / `analytics-api: 8000` / `health-checker: 8080`) が
ホスト側で他プロセスによって使われています。

### 対処

1. どのプロセスが占有しているか確認する:

   ```sh
   lsof -i :3000
   lsof -i :8000
   lsof -i :8080
   ```

2. 該当プロセスを停止するか、`.env` などでポート番号を変更する
3. 直近まで起動していた PulseBoard 自体が残っている場合は
   `docker compose down` で完全に停止する

## 2. `analytics-api` の pytest が失敗する

### 症状

- `flake8 --max-line-length=120` が E501 を返す
- `pytest` が `ImportError` / `ModuleNotFoundError` を返す

### 原因

- 依存が更新されているのに `requirements-dev.txt` を再インストールしていない
- `PYTHONPATH` が `analytics-api/` を認識していない

### 対処

```sh
cd analytics-api
pip install -r requirements-dev.txt
flake8 --max-line-length=120 --exclude=__pycache__ main.py
pytest -v
```

CI と全く同じコマンドを流すこと。ローカルで通っていても
`--max-line-length=120` の指定を忘れると CI と乖離するので注意。

## 3. `health-checker` (Go) の `go vet` / `go test` が失敗する

### 症状

- `go: cannot find main module` エラー
- `go vet` が `undeclared name` を返す

### 対処

`health-checker` ディレクトリで実行する必要があります。

```sh
cd health-checker
go mod download
go vet ./...
go test -v ./...
```

Go のバージョンは CI と揃えて **1.22 系** を使用してください
(`go version` で確認)。

## 4. `api-gateway` の `npm test` / `npm run lint` が失敗する

### 症状

- `npm ci` が `EACCES` / lockfile mismatch で失敗する
- `npm test` がタイムアウトする

### 対処

```sh
cd api-gateway
rm -rf node_modules
npm ci
npm run lint
npm test
```

Node.js のバージョンは CI に合わせて **22 系** を使用してください
(`nvm use 22` 等で切り替え)。
`package-lock.json` を直接編集しないこと (Dependabot 経由か
`npm install <pkg>` で更新)。

## 5. `analytics-api` から `health-checker` に接続できない

### 症状

`analytics-api` のログに `connection refused` / `no such host` が出る。

### 原因

`docker-compose.yml` で定義されたサービス名 (`health-checker`) ではなく
`localhost` や `127.0.0.1` を指してしまっている。

### 対処

`.env.example` を参考に、コンテナ内では以下のようにサービス名で参照する:

```
HEALTH_CHECKER_URL=http://health-checker:8080
```

ホスト OS から直接アクセスする場合のみ
`http://localhost:8080` を使用します。

## 6. Docker イメージのビルドが極端に遅い / 途中で OOM

### 対処

- Docker Desktop / colima のメモリ割り当てを 4GB 以上に増やす
- `docker system prune -a` で不要なイメージ / キャッシュを削除する
- `docker compose build --parallel` はメモリを消費するため、
  低スペック環境では `--parallel` を外して逐次ビルドする

## 7. CI が緑にならない (PR のブロック)

### チェックリスト

- `flake8` の警告が残っていないか (`--max-line-length=120`)
- `pytest -v` がローカルで全通するか
- `go vet ./...` `go test -v ./...` が両方通るか
- `npm run lint` / `npm test` が両方通るか
- 依存ファイル (`requirements-dev.txt` / `go.mod` / `package.json`) を
  更新した場合は、対応するロックファイル / キャッシュキーも同時に更新されているか

CI 定義は [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
にあります。ローカル実行時は同ファイルで指定されている
Python / Go / Node のバージョンに揃えてください。

## 関連ドキュメント

- [ARCHITECTURE.md](./ARCHITECTURE.md) — 3 サービス構成とデータフロー
- [../README.md](../README.md) — セットアップ手順
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — 開発フロー
- [../SECURITY.md](../SECURITY.md) — セキュリティ問題の報告
