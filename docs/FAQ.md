# よくある質問 (FAQ)

PulseBoard を初めて触る人 / たまに触る人が疑問に思いがちなポイントを Q&A 形式でまとめています。
障害対応の切り分けは [TROUBLESHOOTING.md](./TROUBLESHOOTING.md)、
アーキテクチャ全体像は [ARCHITECTURE.md](./ARCHITECTURE.md) を参照してください。

## プロジェクト全体について

### Q1. なぜ Python / Go / TypeScript の 3 言語構成なのか

PulseBoard は「マイクロサービス構成のリアルタイムヘルス監視ダッシュボード」であり、
各サービスに求められる特性が異なるためです。

| サービス          | 言語       | 主な理由                                                     |
| ----------------- | ---------- | ------------------------------------------------------------ |
| `analytics-api`   | Python     | データ集計・分析ロジックの記述性と、pandas 等のエコシステム   |
| `health-checker`  | Go         | 多数のエンドポイントを並行にヘルスチェックする低コストな並行処理 |
| `api-gateway`     | TypeScript | フロント寄りの JSON API 集約とスキーマ表現の親和性             |

「同じ言語に揃えたほうがシンプル」という判断もあり得ますが、本リポジトリでは
各サービスが独立して進化することを重視しています。詳しくは
[ARCHITECTURE.md](./ARCHITECTURE.md) を参照してください。

### Q2. 3 サービスの責務の使い分けが分からない

ざっくりした呼び分けは以下です。

- `health-checker` (Go) — **観測する側**。対象サービスに対して周期的にヘルスチェックを投げ、生死・遅延を記録する。
- `analytics-api` (Python) — **集計する側**。`health-checker` が集めた生データから、SLO / SLI / エラーレート等の派生指標を計算する。
- `api-gateway` (TypeScript) — **公開する側**。フロントエンドや外部クライアントに、上記 2 サービスの結果を統合された JSON API として提供する。

「どのサービスに実装するか迷ったら、その処理が『観測』『集計』『公開』のどれに近いか」を判断基準にしてください。

## ローカル開発について

### Q3. 開発のためにどのランタイムを入れればよいか

CI と揃えるのが最も安全です。現在の CI で使用しているバージョンは以下です
(最新の値は必ず [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) を確認してください)。

- Python: **3.x 系** (`analytics-api`)
- Go: **1.22 系** (`health-checker`)
- Node.js: **22 系** (`api-gateway`)

`nvm` / `pyenv` / `goenv` などのバージョン管理ツールの利用を推奨します。

### Q4. 開発中も 3 サービス全部を必ず起動する必要があるか

いいえ。触っているサービスに応じて起動範囲を狭められます。

- 単体テストだけ回したいなら、対象サービスのディレクトリ内で
  `pytest` / `go test` / `npm test` を実行するのが最短。
- 他サービスとの結合を確認したいときのみ `docker compose up` で全体を立ち上げる。
- 1 サービスだけを Docker Compose で起動したい場合は
  `docker compose up analytics-api` のようにサービス名を指定できる。

### Q5. `.env` は何をコピーすればよいか

`.env.example` をリポジトリルートで `.env` にコピーして開始します。

```sh
cp .env.example .env
```

コンテナ間通信では `localhost` ではなく **サービス名** で参照する点に注意してください
(詳細は [TROUBLESHOOTING.md #5](./TROUBLESHOOTING.md#5-analytics-api-から-health-checker-に接続できない) を参照)。

### Q6. IDE 側の設定は必要か

必須ではありませんが、以下を推奨します。

- Python: `flake8` を CI と同じ `--max-line-length=120` で動かせるようにする
- Go: `gofmt` / `go vet` を保存時に走らせる
- TypeScript: リポジトリ内の ESLint 設定を IDE 側でも読み込む

これらは CI 側で検査される内容と等価なので、ローカルで気付けるようにしておくと
Pull Request 前の手戻りが減ります。

## CI・ビルドについて

### Q7. どのファイルを触ったら、どの CI ジョブが走るのか

現時点では、CI は **サービスごとにジョブが分かれている想定**です。
詳細は [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) を直接確認してください。
ジョブ設計が更新された場合は、この FAQ よりもワークフローファイルの内容が正となります。

### Q8. ローカルで通ったのに CI が落ちる時、まず疑うべきことは

以下を順にチェックしてください。

1. ランタイムのバージョンが CI と一致しているか (Q3)
2. `flake8` の引数が CI と揃っているか (`--max-line-length=120`)
3. `requirements-dev.txt` / `go.mod` / `package-lock.json` を更新後、
   ローカルで再インストールしたか
4. 依存の追加後、対応するロックファイルもコミットに含めたか

さらに詳しくは [TROUBLESHOOTING.md #7](./TROUBLESHOOTING.md#7-ci-が緑にならない-pr-のブロック) を参照してください。

## ドキュメント・貢献について

### Q9. 新しいドキュメントはどこに置けばよいか

- **複数サービスにまたがる話題** → `docs/` 配下に配置し、[`docs/README.md`](./README.md) の目次に追記
- **サービス固有の話題** → そのサービスのディレクトリ配下に README を置く
- **リポジトリ運営に関する話題** (行動規範 / セキュリティ報告 / 貢献フロー) → リポジトリルートの
  `CODE_OF_CONDUCT.md` / `SECURITY.md` / `CONTRIBUTING.md` に集約

### Q10. コミットメッセージや PR タイトルの規約はあるか

`CONTRIBUTING.md` に集約されています。ここでは触れませんが、
プレフィックス (`feat:` / `fix:` / `docs:` / `chore:` など) を付けるスタイルが
既存コミット履歴から読み取れます。既存の PR タイトルを参考にしてください。

## 関連ドキュメント

- [ARCHITECTURE.md](./ARCHITECTURE.md) — 3 サービス構成とデータフロー
- [RUNBOOK.md](./RUNBOOK.md) — 起動・停止・再起動などの定常運用手順
- [OBSERVABILITY.md](./OBSERVABILITY.md) — ログ・メトリクス・ヘルス指標の観測
- [TROUBLESHOOTING.md](./TROUBLESHOOTING.md) — 障害切り分けと対処
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — コントリビュートの流れ
