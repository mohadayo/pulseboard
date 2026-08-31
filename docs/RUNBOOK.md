# 運用ランブック (Runbook)

Pulseboard の運用中に発生し得るインシデントへの**初動対応**をまとめたランブックです。
症状ベースの逆引きは [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) を、監視・メトリクス設計は [`docs/OBSERVABILITY.md`](./OBSERVABILITY.md) を、システム全体像は [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md) を参照してください。

## 1. インシデント初動フロー

アラートを受信した / 障害の報告を受けた際は、以下の順で対応します。

1. **一次受信** — アラート内容 / 発生時刻 / 影響サービスを記録する
2. **影響範囲の確認**
   - `api-gateway` のヘルスチェック応答
   - `analytics-api` の `/health` (もしくは `/metrics/count` 等の代表エンドポイント) 応答
   - `health-checker` の直近チェック結果
3. **暫定対応** — 影響が拡大する場合はまず該当サービスをリスタート (下記 §2) して 5xx 停止を最優先とする
4. **原因調査** — [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) の症状マトリクスから該当項目を辿る
5. **恒久対応** — 修正 PR の作成 / 設定変更 / 依存側への連絡
6. **事後まとめ** — 影響時間・原因・再発防止策を Issue または内部ドキュメントに記録

## 2. サービス別リカバリ手順

いずれも `docker-compose` を使ったローカル / 開発環境ベースの手順です。本番環境ではオーケストレーション基盤に置き換えて解釈してください。

### 2.1 api-gateway

- **再起動:** `docker-compose restart api-gateway`
- **ログ確認:** `docker-compose logs --tail=200 -f api-gateway`
- **設定リロード:** コンテナ内 `nginx -s reload` 相当が可能な構成であればそれを、なければ再起動
- **ロールバック:** 直前タグに `docker-compose.yml` の `image:` を戻して `docker-compose up -d api-gateway`

### 2.2 analytics-api

- **再起動:** `docker-compose restart analytics-api`
- **ログ確認:** `docker-compose logs --tail=200 -f analytics-api`
- **単体ヘルス確認:** ゲートウェイをバイパスしてコンテナに直接アクセスしレスポンスを確認
- **依存 DB の疎通確認:** 接続文字列と資格情報を `.env` と突き合わせる

### 2.3 health-checker

- **再起動:** `docker-compose restart health-checker`
- **ログ確認:** `docker-compose logs --tail=200 -f health-checker`
- **チェック対象の設定確認:** 設定ファイル / 環境変数から監視先 URL のリストを確認
- **一時停止:** 誤検知が続く場合は `docker-compose stop health-checker` で停止し、アラート抑止

## 3. よく使う運用コマンド集

```bash
# 全サービスの状態
docker-compose ps

# 全サービスのログを直近 200 行 + 追従
docker-compose logs --tail=200 -f

# 特定サービスのみ再起動
docker-compose restart <service>

# 特定サービスのみ再ビルドして再起動
docker-compose up -d --build <service>

# コンテナ内シェル
docker-compose exec <service> sh

# ボリューム / ネットワーク状態
docker volume ls
docker network ls
```

## 4. エスカレーション基準

以下のいずれかに該当したらオンコール担当を追加でエスカレーションします。

| 条件                                       | 対応レベル |
| ------------------------------------------ | ---------- |
| ユーザ影響のある 5xx が 5 分以上継続       | Sev-1      |
| 単一サービスの機能限定的障害               | Sev-2      |
| 監視系の誤検知 / ラウンドトリップ遅延の増加 | Sev-3      |
| セキュリティに関する疑いのある事象         | 即エスカレーション ([`SECURITY.md`](../SECURITY.md) 参照) |

Sev-1 の場合はステータス告知の要否も同時に判断してください。

## 5. 関連ドキュメント

- [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md) — システム全体像 / サービス責務境界
- [`docs/OBSERVABILITY.md`](./OBSERVABILITY.md) — メトリクス / ログ / トレースの運用設計
- [`docs/TROUBLESHOOTING.md`](./TROUBLESHOOTING.md) — 症状 → 原因 → 対処の逆引き
- [`SECURITY.md`](../SECURITY.md) — 脆弱性報告と対応
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — 変更 PR の作成ルール

## 6. 更新方針

- 新しいアラートを追加した / インシデントで初動が不足した際は、本ファイルに恒久項目として追加する
- 再現手順が固まっていない対処は `TROUBLESHOOTING.md` へ、フロー / 手順として確立したものを本ファイルへ収める
