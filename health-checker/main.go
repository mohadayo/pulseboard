package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

type CheckResult struct {
	Service        string  `json:"service"`
	URL            string  `json:"url"`
	Status         string  `json:"status"`
	ResponseTimeMs float64 `json:"response_time_ms"`
	Timestamp      float64 `json:"timestamp"`
	Error          string  `json:"error,omitempty"`
}

type ServiceTarget struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

func GetEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func envSeconds(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}

func methodAllowed(w http.ResponseWriter, r *http.Request, allowed ...string) bool {
	for _, m := range allowed {
		if r.Method == m {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
	return false
}

func CheckService(client *http.Client, target ServiceTarget) CheckResult {
	start := time.Now()
	// analytics-api 側は Python の time.time() が返す float（マイクロ秒粒度）を
	// 前提に時間絞り込み・ソートを行うため、こちらも秒未満の精度を維持する。
	// time.Now().Unix() だと整数秒に丸められ、1 秒以内の並列チェックで
	// 全レコードの timestamp が同値になってしまう。
	result := CheckResult{
		Service:   target.Name,
		URL:       target.URL,
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
	}

	resp, err := client.Get(target.URL)
	elapsed := time.Since(start)
	result.ResponseTimeMs = float64(elapsed.Milliseconds())

	if err != nil {
		result.Status = "unhealthy"
		result.Error = err.Error()
		log.Printf("[WARN] Service %s is unhealthy: %v", target.Name, err)
		return result
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		result.Status = "healthy"
		log.Printf("[INFO] Service %s is healthy (%.0fms)", target.Name, result.ResponseTimeMs)
	} else {
		result.Status = "unhealthy"
		result.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		log.Printf("[WARN] Service %s returned status %d", target.Name, resp.StatusCode)
	}

	return result
}

// reportMetricRetryPolicy は ReportMetric の試行回数とバックオフ初期値をまとめた値。
// 環境変数で上書きされる。
type reportMetricRetryPolicy struct {
	maxAttempts int
	backoff     time.Duration
}

func loadRetryPolicy() reportMetricRetryPolicy {
	return reportMetricRetryPolicy{
		maxAttempts: envIntAtLeastOne("METRIC_REPORT_MAX_ATTEMPTS", 3),
		backoff:     envMillis("METRIC_REPORT_BACKOFF_MS", 100*time.Millisecond),
	}
}

func envIntAtLeastOne(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			return n
		}
	}
	return fallback
}

func envMillis(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return fallback
}

// newCheckHTTPClient はサービス /health のチェック・analytics-api への
// メトリクス送信に使う共通 HTTP クライアントを生成する。
//
// タイムアウトは CHECK_HTTP_TIMEOUT_SECONDS 環境変数で上書き可能。
// 既定 5 秒。0 / 不正値の場合は envSeconds の挙動で既定値にフォールバック。
//
// 旧実装では makeCheckHandler と main の 2 箇所で
// `&http.Client{Timeout: 5 * time.Second}` がハードコードされており、
// 片方だけ変更されると drift する状態だった。本関数で 1 箇所に集約する。
func newCheckHTTPClient() *http.Client {
	return &http.Client{
		Timeout: envSeconds("CHECK_HTTP_TIMEOUT_SECONDS", 5*time.Second),
	}
}

// shouldRetryStatus は HTTP ステータスコードがリトライに値するかを返す。
// 5xx は一時的障害として再試行し、429 (Too Many Requests) と 408 (Request
// Timeout) も対象。それ以外の 4xx はクライアント不備なので即時失敗とする。
//
// 408 は RFC 7231 §6.5.7 で「サーバが受信完了できなかった。クライアントは
// 修正せずに再試行できる」と定義された遷移性エラーであり、analytics-api が
// 高負荷・ネットワーク揺らぎで返した場合に報告をドロップせず再送するために
// retry 対象に含めている。
func shouldRetryStatus(code int) bool {
	if code >= 500 && code < 600 {
		return true
	}
	if code == http.StatusTooManyRequests {
		return true
	}
	if code == http.StatusRequestTimeout {
		return true
	}
	return false
}

// ReportMetric は analytics-api に 1 件のメトリクスを POST する。
// 一時的失敗（接続エラー / 5xx / 429）に対しては指数バックオフで自動リトライする。
// 試行回数・バックオフ初期値は METRIC_REPORT_MAX_ATTEMPTS / METRIC_REPORT_BACKOFF_MS で上書き可。
func ReportMetric(client *http.Client, analyticsURL string, result CheckResult) error {
	return reportMetricWithPolicy(client, analyticsURL, result, loadRetryPolicy())
}

func reportMetricWithPolicy(
	client *http.Client,
	analyticsURL string,
	result CheckResult,
	policy reportMetricRetryPolicy,
) error {
	payload := map[string]interface{}{
		"service":          result.Service,
		"status":           result.Status,
		"response_time_ms": result.ResponseTimeMs,
		"timestamp":        result.Timestamp,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		resp, err := client.Post(analyticsURL+"/metrics", "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			log.Printf(
				"[WARN] Report attempt %d/%d failed for %s: %v",
				attempt, policy.maxAttempts, result.Service, err,
			)
		} else {
			statusCode := resp.StatusCode
			resp.Body.Close()
			if statusCode == http.StatusCreated {
				log.Printf(
					"[INFO] Reported metric for %s to analytics (attempt %d)",
					result.Service, attempt,
				)
				return nil
			}
			lastErr = fmt.Errorf("analytics API returned %d", statusCode)
			if !shouldRetryStatus(statusCode) {
				// 4xx (429 除く) は即時失敗。リトライしない。
				log.Printf(
					"[WARN] Report for %s aborted (non-retryable %d)",
					result.Service, statusCode,
				)
				return lastErr
			}
			log.Printf(
				"[WARN] Report attempt %d/%d for %s returned %d",
				attempt, policy.maxAttempts, result.Service, statusCode,
			)
		}

		if attempt >= policy.maxAttempts {
			break
		}
		// 指数バックオフ: backoff * 2^(attempt-1)
		sleep := policy.backoff * (1 << (attempt - 1))
		time.Sleep(sleep)
	}
	log.Printf(
		"[WARN] Failed to report metric for %s after %d attempt(s): %v",
		result.Service, policy.maxAttempts, lastErr,
	)
	return lastErr
}

// parseExtraTargets は EXTRA_TARGETS 環境変数の値をパースし、追加ターゲットを返す。
//
// 期待する形式は JSON 配列で、各要素は `{"name":"","url":""}`。空文字列・
// パース失敗・型ミスマッチ・name/url いずれかが空のエントリは無視し、
// プロセスは常にデフォルトターゲットのみで起動できるように fail-open にする
// （運用時に誤った JSON を渡してもコンテナが再起動ループしない）。
//
// 復帰値の第 2 引数はパースに失敗した場合のエラー。呼び元は警告ログのみに使い、
// 起動可否の判定には使わない（設定ミス通知の観点で有用だが致命傷ではない）。
func parseExtraTargets(raw string) ([]ServiceTarget, error) {
	if raw == "" {
		return nil, nil
	}
	var entries []ServiceTarget
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, err
	}
	valid := make([]ServiceTarget, 0, len(entries))
	for _, e := range entries {
		if e.Name == "" || e.URL == "" {
			continue
		}
		valid = append(valid, e)
	}
	return valid, nil
}

func NewTargets() []ServiceTarget {
	analyticsURL := GetEnv("ANALYTICS_URL", "http://localhost:8001")
	gatewayURL := GetEnv("GATEWAY_URL", "http://localhost:8000")
	targets := []ServiceTarget{
		{Name: "analytics-api", URL: analyticsURL + "/health"},
		{Name: "api-gateway", URL: gatewayURL + "/health"},
	}
	// EXTRA_TARGETS で追加ターゲットを末尾に append する。
	// パース失敗時は警告ログのみ出してデフォルトターゲットで起動を続行する。
	if extras, err := parseExtraTargets(os.Getenv("EXTRA_TARGETS")); err != nil {
		log.Printf("[WARN] Ignoring EXTRA_TARGETS due to parse error: %v", err)
	} else if len(extras) > 0 {
		targets = mergeExtraTargets(targets, extras)
	}
	return targets
}

// mergeExtraTargets はデフォルトターゲットと EXTRA_TARGETS 由来の追加ターゲットを合成する。
//
// name の重複は「先勝ち」でスキップする：
//  1. デフォルトの `analytics-api` / `api-gateway` と衝突する extra は捨てる
//     （旧実装だと両方 append されて `checkAndReportTargets` が 1 サイクルで同名メトリクス
//     を 2 件 analytics-api に送り、ダッシュボードの total_checks / uptime_pct が二重計上
//     でズレていた）
//  2. EXTRA_TARGETS 自体の内部での自己重複（同一 name が複数回登場）は最初の 1 件のみ採用
//
// スキップ時は `[WARN]` ログを出して silent drop を避け、`/targets` エンドポイントを
// 叩かなくても運用時ログから気付けるようにする。fail-open 方針は維持し、
// スキップは起動失敗にはしない。
//
// テスト容易性のためにパッケージレベル関数として切り出し、`NewTargets` から呼び出す。
func mergeExtraTargets(defaults, extras []ServiceTarget) []ServiceTarget {
	seen := make(map[string]struct{}, len(defaults)+len(extras))
	for _, t := range defaults {
		seen[t.Name] = struct{}{}
	}
	merged := make([]ServiceTarget, 0, len(defaults)+len(extras))
	merged = append(merged, defaults...)
	for _, e := range extras {
		if _, dup := seen[e.Name]; dup {
			log.Printf(
				"[WARN] Ignoring EXTRA_TARGETS entry %q (%s): name already registered",
				e.Name, e.URL,
			)
			continue
		}
		seen[e.Name] = struct{}{}
		merged = append(merged, e)
	}
	return merged
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if !methodAllowed(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status:  "healthy",
		Service: "health-checker",
	})
}

// makeTargetsHandler は起動時に構築した監視対象一覧を JSON で返すハンドラを生成する。
//
// 運用中に「ANALYTICS_URL / GATEWAY_URL / EXTRA_TARGETS が本当に反映されたか」を
// コンテナに入らず curl 一発で確認できるようにする。EXTRA_TARGETS の JSON が壊れて
// silent drop されていた等のミス設定を、実行中プロセス側で即座に検知できる。
//
// targets はプロセス起動時に main() で一度だけ構築され、以降は不変で読み取り
// 専用なので、ハンドラ側でロックは不要。/health と同じく GET / HEAD のみ許可。
func makeTargetsHandler(targets []ServiceTarget) http.HandlerFunc {
	// nil に対して JSON エンコードすると "null" になり、"count":0,"targets":null
	// になってしまう。クライアントはループ前に nil チェックが要らないよう、
	// 起動時に必ず空スライスへ正規化してエンコードする。
	safe := targets
	if safe == nil {
		safe = []ServiceTarget{}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !methodAllowed(w, r, http.MethodGet, http.MethodHead) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count":   len(safe),
			"targets": safe,
		})
	}
}

// checkAndReportTargets はターゲット群を並列にチェックし、結果を入力順で返す。
// 各ターゲットの metrics 報告も並列に実行する。
func checkAndReportTargets(client *http.Client, targets []ServiceTarget, analyticsURL string) ([]CheckResult, int) {
	results := make([]CheckResult, len(targets))
	var reported int32

	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(idx int, target ServiceTarget) {
			defer wg.Done()
			result := CheckService(client, target)
			results[idx] = result
			if err := ReportMetric(client, analyticsURL, result); err == nil {
				atomic.AddInt32(&reported, 1)
			}
		}(i, t)
	}
	wg.Wait()

	return results, int(reported)
}

// RuntimeConfig は health-checker の実行時設定を表す。
// 環境変数由来の値をプロセス起動時に一度だけ読み取り、以降は不変で参照する。
// `/config` エンドポイントはこの構造体をそのまま JSON エンコードして返す。
type RuntimeConfig struct {
	AnalyticsURL            string       `json:"analytics_url"`
	CheckIntervalSeconds    int          `json:"check_interval_seconds"`
	CheckHTTPTimeoutSeconds int          `json:"check_http_timeout_seconds"`
	MetricReportMaxAttempts int          `json:"metric_report_max_attempts"`
	MetricReportBackoffMs   int          `json:"metric_report_backoff_ms"`
	Server                  ServerConfig `json:"server"`
	ShutdownTimeoutSeconds  int          `json:"shutdown_timeout_seconds"`
}

// ServerConfig は net/http.Server 側のタイムアウト設定をまとめる。
// `/config` レスポンスで "server" ネストとして出す。
type ServerConfig struct {
	ReadHeaderTimeoutSeconds int `json:"read_header_timeout_seconds"`
	ReadTimeoutSeconds       int `json:"read_timeout_seconds"`
	WriteTimeoutSeconds      int `json:"write_timeout_seconds"`
	IdleTimeoutSeconds       int `json:"idle_timeout_seconds"`
}

// loadRuntimeConfig は環境変数から実行時設定を構築する。
// main() の env 読み取り箇所と同じデフォルト値・単位換算ロジックを使い、
// `/config` の返り値と実際の挙動が drift しないようにする。
func loadRuntimeConfig() RuntimeConfig {
	policy := loadRetryPolicy()
	return RuntimeConfig{
		AnalyticsURL:            GetEnv("ANALYTICS_URL", "http://localhost:8001"),
		CheckIntervalSeconds:    int(envSeconds("CHECK_INTERVAL_SECONDS", 0) / time.Second),
		CheckHTTPTimeoutSeconds: int(envSeconds("CHECK_HTTP_TIMEOUT_SECONDS", 5*time.Second) / time.Second),
		MetricReportMaxAttempts: policy.maxAttempts,
		MetricReportBackoffMs:   int(policy.backoff / time.Millisecond),
		Server: ServerConfig{
			ReadHeaderTimeoutSeconds: int(envSeconds("CHECKER_READ_HEADER_TIMEOUT", 5*time.Second) / time.Second),
			ReadTimeoutSeconds:       int(envSeconds("CHECKER_READ_TIMEOUT", 15*time.Second) / time.Second),
			WriteTimeoutSeconds:      int(envSeconds("CHECKER_WRITE_TIMEOUT", 15*time.Second) / time.Second),
			IdleTimeoutSeconds:       int(envSeconds("CHECKER_IDLE_TIMEOUT", 60*time.Second) / time.Second),
		},
		ShutdownTimeoutSeconds: int(envSeconds("SHUTDOWN_TIMEOUT_SECONDS", 30*time.Second) / time.Second),
	}
}

// makeConfigHandler は実行時設定を JSON で返すハンドラを生成する。
//
// 運用中に「本当に env が反映されたか」を curl 一発で確認できるようにする。
// 例えば `CHECK_INTERVAL_SECONDS=60` のつもりが `CHECK_INTERAVAL_SECONDS`
// と typo していた場合、`/config` を叩けば `check_interval_seconds: 0`
// と表示され即座に気付ける。
//
// config はプロセス起動時に一度だけ構築されて以降は不変なので、
// ハンドラ側でロックは不要。/targets / /health と同じく GET / HEAD のみ許可。
func makeConfigHandler(config RuntimeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !methodAllowed(w, r, http.MethodGet, http.MethodHead) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(config)
	}
}

func makeCheckHandler(targets []ServiceTarget, analyticsURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !methodAllowed(w, r, http.MethodGet, http.MethodPost) {
			return
		}
		client := newCheckHTTPClient()

		results, reported := checkAndReportTargets(client, targets, analyticsURL)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"checked_at": time.Now().UTC().Format(time.RFC3339),
			"results":    results,
			"reported":   reported,
		})
	}
}

// runPeriodicChecks はバックグラウンドで一定間隔ごとにすべてのターゲットを
// チェックし、結果を analytics-api に report する。
//
// 起動時に即時 1 回チェックを実施し、その後 interval ごとに繰り返す。
// 外部から `/check` を呼ばなくても、docker compose up しただけでメトリクスが
// 蓄積されるようにする目的。CHECK_INTERVAL_SECONDS=0（既定）のときは
// main() から呼ばれず、これまで通り `/check` 呼び出し時のみチェックが走る
// （後方互換）。
//
// ctx がキャンセルされた時点でループを抜ける。進行中のチェックは HTTP
// クライアントのタイムアウトで打ち切られるため、shutdown 経路を阻害しない。
func runPeriodicChecks(
	ctx context.Context,
	client *http.Client,
	targets []ServiceTarget,
	analyticsURL string,
	interval time.Duration,
) {
	log.Printf("[INFO] Periodic checks enabled (interval=%s)", interval)
	checkAndReportTargets(client, targets, analyticsURL)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("[INFO] Periodic checks stopping")
			return
		case <-ticker.C:
			checkAndReportTargets(client, targets, analyticsURL)
		}
	}
}

func main() {
	port := GetEnv("CHECKER_PORT", "8002")
	targets := NewTargets()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/targets", makeTargetsHandler(targets))
	mux.HandleFunc("/config", makeConfigHandler(loadRuntimeConfig()))
	analyticsURL := GetEnv("ANALYTICS_URL", "http://localhost:8001")
	mux.HandleFunc("/check", makeCheckHandler(targets, analyticsURL))

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: envSeconds("CHECKER_READ_HEADER_TIMEOUT", 5*time.Second),
		ReadTimeout:       envSeconds("CHECKER_READ_TIMEOUT", 15*time.Second),
		WriteTimeout:      envSeconds("CHECKER_WRITE_TIMEOUT", 15*time.Second),
		IdleTimeout:       envSeconds("CHECKER_IDLE_TIMEOUT", 60*time.Second),
	}

	shutdownTimeout := envSeconds("SHUTDOWN_TIMEOUT_SECONDS", 30*time.Second)
	// CHECK_INTERVAL_SECONDS > 0 でバックグラウンド定期チェックを有効化する。
	// 0 / 未設定 / 不正値はゼロとして扱い、これまで通り `/check` 呼び出し時のみ動作。
	checkInterval := envSeconds("CHECK_INTERVAL_SECONDS", 0)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[INFO] Health Checker starting on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Server failed: %v", err)
		}
	}()

	if checkInterval > 0 {
		periodicClient := newCheckHTTPClient()
		go runPeriodicChecks(ctx, periodicClient, targets, analyticsURL, checkInterval)
	}

	<-ctx.Done()
	stop()
	log.Printf("[INFO] Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("[FATAL] Forced shutdown: %v", err)
	}
	log.Println("[INFO] Server stopped")
}
