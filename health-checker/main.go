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
	result := CheckResult{
		Service:   target.Name,
		URL:       target.URL,
		Timestamp: float64(time.Now().Unix()),
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

// shouldRetryStatus は HTTP ステータスコードがリトライに値するかを返す。
// 5xx は一時的障害として再試行し、429 (Too Many Requests) も対象。
// それ以外の 4xx はクライアント不備なので即時失敗とする。
func shouldRetryStatus(code int) bool {
	if code >= 500 && code < 600 {
		return true
	}
	if code == http.StatusTooManyRequests {
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

func NewTargets() []ServiceTarget {
	analyticsURL := GetEnv("ANALYTICS_URL", "http://localhost:8001")
	gatewayURL := GetEnv("GATEWAY_URL", "http://localhost:8000")
	return []ServiceTarget{
		{Name: "analytics-api", URL: analyticsURL + "/health"},
		{Name: "api-gateway", URL: gatewayURL + "/health"},
	}
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

func makeCheckHandler(targets []ServiceTarget, analyticsURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !methodAllowed(w, r, http.MethodGet, http.MethodPost) {
			return
		}
		client := &http.Client{Timeout: 5 * time.Second}

		results, reported := checkAndReportTargets(client, targets, analyticsURL)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"checked_at": time.Now().UTC().Format(time.RFC3339),
			"results":    results,
			"reported":   reported,
		})
	}
}

func main() {
	port := GetEnv("CHECKER_PORT", "8002")
	targets := NewTargets()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[INFO] Health Checker starting on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Server failed: %v", err)
		}
	}()

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
