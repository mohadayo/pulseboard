package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("expected status healthy, got %s", resp.Status)
	}
	if resp.Service != "health-checker" {
		t.Errorf("expected service health-checker, got %s", resp.Service)
	}
}

func TestCheckServiceHealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer server.Close()

	client := server.Client()
	target := ServiceTarget{Name: "test-svc", URL: server.URL + "/health"}
	result := CheckService(client, target)

	if result.Status != "healthy" {
		t.Errorf("expected healthy, got %s", result.Status)
	}
	if result.Service != "test-svc" {
		t.Errorf("expected test-svc, got %s", result.Service)
	}
	if result.ResponseTimeMs < 0 {
		t.Errorf("response time should be >= 0, got %f", result.ResponseTimeMs)
	}
}

func TestCheckServiceUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := server.Client()
	target := ServiceTarget{Name: "bad-svc", URL: server.URL + "/health"}
	result := CheckService(client, target)

	if result.Status != "unhealthy" {
		t.Errorf("expected unhealthy, got %s", result.Status)
	}
	if result.Error != "HTTP 500" {
		t.Errorf("expected 'HTTP 500', got '%s'", result.Error)
	}
}

func TestCheckServiceConnectionError(t *testing.T) {
	client := &http.Client{}
	target := ServiceTarget{Name: "down-svc", URL: "http://127.0.0.1:1/health"}
	result := CheckService(client, target)

	if result.Status != "unhealthy" {
		t.Errorf("expected unhealthy, got %s", result.Status)
	}
	if result.Error == "" {
		t.Error("expected non-empty error")
	}
}

func TestReportMetricSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Errorf("expected /metrics, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected application/json content type")
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client := server.Client()
	result := CheckResult{Service: "test", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1234567890}
	err := ReportMetric(client, server.URL, result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckHandler(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer backend.Close()

	mockAnalytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockAnalytics.Close()

	targets := []ServiceTarget{
		{Name: "svc-a", URL: backend.URL + "/health"},
	}

	handler := makeCheckHandler(targets, mockAnalytics.URL)
	req := httptest.NewRequest("GET", "/check", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]interface{}
	json.NewDecoder(w.Body).Decode(&body)
	results, ok := body["results"].([]interface{})
	if !ok || len(results) != 1 {
		t.Fatalf("expected 1 result, got %v", body["results"])
	}
	reported := int(body["reported"].(float64))
	if reported != 1 {
		t.Errorf("expected 1 reported, got %d", reported)
	}
}

func TestHealthHandler_MethodNotAllowed(t *testing.T) {
	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		req := httptest.NewRequest(method, "/health", nil)
		w := httptest.NewRecorder()
		healthHandler(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", method, w.Code)
		}
		allow := w.Header().Get("Allow")
		if allow == "" {
			t.Errorf("method %s: expected Allow header to be set", method)
		}
	}
}

func TestHealthHandler_HeadAllowed(t *testing.T) {
	req := httptest.NewRequest("HEAD", "/health", nil)
	w := httptest.NewRecorder()
	healthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for HEAD, got %d", w.Code)
	}
}

func TestCheckHandler_MethodNotAllowed(t *testing.T) {
	handler := makeCheckHandler(nil, "http://localhost")
	for _, method := range []string{"PUT", "DELETE", "PATCH"} {
		req := httptest.NewRequest(method, "/check", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", method, w.Code)
		}
	}
}

func TestEnvSeconds_Default(t *testing.T) {
	got := envSeconds("DEFINITELY_NOT_SET_TIMEOUT_VAR", 7*time.Second)
	if got != 7*time.Second {
		t.Errorf("expected fallback 7s, got %v", got)
	}
}

func TestEnvSeconds_Override(t *testing.T) {
	t.Setenv("CUSTOM_TIMEOUT_VAR", "12")
	got := envSeconds("CUSTOM_TIMEOUT_VAR", 5*time.Second)
	if got != 12*time.Second {
		t.Errorf("expected 12s, got %v", got)
	}
}

func TestEnvSeconds_InvalidFallsBack(t *testing.T) {
	t.Setenv("BAD_TIMEOUT_VAR", "abc")
	got := envSeconds("BAD_TIMEOUT_VAR", 4*time.Second)
	if got != 4*time.Second {
		t.Errorf("expected 4s fallback for invalid value, got %v", got)
	}
	t.Setenv("NEG_TIMEOUT_VAR", "-3")
	got = envSeconds("NEG_TIMEOUT_VAR", 4*time.Second)
	if got != 4*time.Second {
		t.Errorf("expected 4s fallback for negative value, got %v", got)
	}
}

func TestGetEnv(t *testing.T) {
	if got := GetEnv("DEFINITELY_NOT_SET_XYZ", "fallback"); got != "fallback" {
		t.Errorf("expected fallback, got %s", got)
	}
	t.Setenv("TEST_ENV_VAR_ABC", "custom")
	if got := GetEnv("TEST_ENV_VAR_ABC", "fallback"); got != "custom" {
		t.Errorf("expected custom, got %s", got)
	}
}

func TestReportMetricFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := server.Client()
	result := CheckResult{Service: "test", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1234567890}
	err := ReportMetric(client, server.URL, result)
	if err == nil {
		t.Fatal("expected error for non-201 response, got nil")
	}
}

func TestCheckHandler_ReportingFails(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer backend.Close()

	mockAnalytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockAnalytics.Close()

	targets := []ServiceTarget{
		{Name: "svc-a", URL: backend.URL + "/health"},
	}

	handler := makeCheckHandler(targets, mockAnalytics.URL)
	req := httptest.NewRequest("GET", "/check", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var body map[string]interface{}
	json.NewDecoder(w.Body).Decode(&body)
	reported := int(body["reported"].(float64))
	if reported != 0 {
		t.Errorf("expected 0 reported when analytics fails, got %d", reported)
	}
}

// TestCheckHandler_RunsInParallel は makeCheckHandler が複数のターゲットを
// 並列にチェックすることを検証する。各バックエンドが意図的に delay 秒待たせる
// ため、直列なら total ≈ delay × N かかるが、並列なら ≈ delay で完了する。
func TestCheckHandler_RunsInParallel(t *testing.T) {
	const delay = 200 * time.Millisecond
	const numTargets = 4

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer slow.Close()

	mockAnalytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockAnalytics.Close()

	targets := make([]ServiceTarget, numTargets)
	for i := 0; i < numTargets; i++ {
		targets[i] = ServiceTarget{Name: "svc-" + strconv.Itoa(i), URL: slow.URL + "/health"}
	}

	handler := makeCheckHandler(targets, mockAnalytics.URL)
	req := httptest.NewRequest("GET", "/check", nil)
	w := httptest.NewRecorder()

	start := time.Now()
	handler(w, req)
	elapsed := time.Since(start)

	// 直列実行なら delay × numTargets ≈ 800ms 以上かかる。
	// 並列実行なら、各リクエストの delay (200ms) ＋ オーバーヘッド程度で完了する。
	threshold := time.Duration(numTargets-1) * delay
	if elapsed >= threshold {
		t.Errorf("expected parallel execution (< %v), but took %v — handler may be serial", threshold, elapsed)
	}

	var body map[string]interface{}
	json.NewDecoder(w.Body).Decode(&body)
	results := body["results"].([]interface{})
	if len(results) != numTargets {
		t.Fatalf("expected %d results, got %d", numTargets, len(results))
	}
	reported := int(body["reported"].(float64))
	if reported != numTargets {
		t.Errorf("expected %d reported, got %d", numTargets, reported)
	}
}

// TestCheckHandler_PreservesTargetOrder は並列実行でも結果スライスの順序が
// 入力ターゲットの順序と一致することを検証する。
func TestCheckHandler_PreservesTargetOrder(t *testing.T) {
	// 各サーバを別々の遅延で応答させ、もし順序保証がなければ結果順がバラつく
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer fast.Close()

	medium := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer medium.Close()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer slow.Close()

	mockAnalytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockAnalytics.Close()

	targets := []ServiceTarget{
		{Name: "first", URL: slow.URL + "/health"},
		{Name: "second", URL: fast.URL + "/health"},
		{Name: "third", URL: medium.URL + "/health"},
	}

	handler := makeCheckHandler(targets, mockAnalytics.URL)
	req := httptest.NewRequest("GET", "/check", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	var body map[string]interface{}
	json.NewDecoder(w.Body).Decode(&body)
	results := body["results"].([]interface{})
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expectedOrder := []string{"first", "second", "third"}
	for i, want := range expectedOrder {
		got := results[i].(map[string]interface{})["service"].(string)
		if got != want {
			t.Errorf("result[%d]: expected service=%q, got %q", i, want, got)
		}
	}
}

func TestCheckHandler_MultipleTargets(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer healthy.Close()

	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer unhealthy.Close()

	mockAnalytics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer mockAnalytics.Close()

	targets := []ServiceTarget{
		{Name: "svc-ok", URL: healthy.URL + "/health"},
		{Name: "svc-bad", URL: unhealthy.URL + "/health"},
	}

	handler := makeCheckHandler(targets, mockAnalytics.URL)
	req := httptest.NewRequest("GET", "/check", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	var body map[string]interface{}
	json.NewDecoder(w.Body).Decode(&body)
	results := body["results"].([]interface{})
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	reported := int(body["reported"].(float64))
	if reported != 2 {
		t.Errorf("expected 2 reported, got %d", reported)
	}
}


// テスト用の高速リトライポリシー。指数バックオフは活かしつつ実時間は最小化する。
var testRetryPolicy = reportMetricRetryPolicy{maxAttempts: 3, backoff: 1 * time.Millisecond}

// TestReportMetric_RetriesOn5xxThenSucceeds は最初の試行で 500 を返し、
// 2 回目で 201 を返すサーバに対し、リトライにより最終的に成功することを確認する。
func TestReportMetric_RetriesOn5xxThenSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	result := CheckResult{Service: "flaky", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}
	if err := reportMetricWithPolicy(server.Client(), server.URL, result, testRetryPolicy); err != nil {
		t.Fatalf("expected success after retry, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("expected 2 calls, got %d", got)
	}
}

// TestReportMetric_RetriesOn429 は 429 Too Many Requests もリトライ対象であることを確認する。
func TestReportMetric_RetriesOn429(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	result := CheckResult{Service: "ratelimited", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}
	if err := reportMetricWithPolicy(server.Client(), server.URL, result, testRetryPolicy); err != nil {
		t.Fatalf("expected success after retry on 429, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("expected 3 calls, got %d", got)
	}
}

// TestReportMetric_DoesNotRetryOn4xx は 400 番台（429 除く）が即時失敗となり、
// 不要なリトライをしないことを確認する。
func TestReportMetric_DoesNotRetryOn4xx(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	result := CheckResult{Service: "bad", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}
	err := reportMetricWithPolicy(server.Client(), server.URL, result, testRetryPolicy)
	if err == nil {
		t.Fatal("expected error for 400, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected exactly 1 call (no retry on 4xx), got %d", got)
	}
}

// TestReportMetric_GivesUpAfterMaxAttempts は持続的に 503 を返すサーバに対し、
// 試行回数を使い切って失敗することを確認する。
func TestReportMetric_GivesUpAfterMaxAttempts(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	policy := reportMetricRetryPolicy{maxAttempts: 4, backoff: 1 * time.Millisecond}
	result := CheckResult{Service: "down", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}
	err := reportMetricWithPolicy(server.Client(), server.URL, result, policy)
	if err == nil {
		t.Fatal("expected failure after exhausting retries, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 4 {
		t.Errorf("expected 4 attempts, got %d", got)
	}
}

// TestReportMetric_SingleAttemptDisablesRetry は maxAttempts=1 にすると
// リトライが無効になることを確認する。
func TestReportMetric_SingleAttemptDisablesRetry(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	policy := reportMetricRetryPolicy{maxAttempts: 1, backoff: 1 * time.Millisecond}
	result := CheckResult{Service: "once", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}
	err := reportMetricWithPolicy(server.Client(), server.URL, result, policy)
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("expected 1 attempt when maxAttempts=1, got %d", got)
	}
}

// TestReportMetric_UsesExponentialBackoff は試行間の sleep が指数的に伸びることを
// 経過時間で間接的に検証する。
func TestReportMetric_UsesExponentialBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// 3 試行 → 試行間に 2 回 sleep → 50ms + 100ms = 150ms 程度かかる想定。
	policy := reportMetricRetryPolicy{maxAttempts: 3, backoff: 50 * time.Millisecond}
	result := CheckResult{Service: "slow", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}

	start := time.Now()
	_ = reportMetricWithPolicy(server.Client(), server.URL, result, policy)
	elapsed := time.Since(start)

	if elapsed < 140*time.Millisecond {
		t.Errorf("expected at least 140ms (50+100), got %v — backoff not exponential?", elapsed)
	}
	// 余裕を持って上限 1s 以内
	if elapsed > 1*time.Second {
		t.Errorf("expected under 1s, got %v", elapsed)
	}
}

// TestShouldRetryStatus は分類関数の境界条件を確認する。
func TestShouldRetryStatus(t *testing.T) {
	cases := []struct {
		code int
		want bool
	}{
		{500, true},
		{502, true},
		{503, true},
		{599, true},
		{429, true},
		{400, false},
		{401, false},
		{404, false},
		{409, false},
		{201, false},
		{200, false},
		{301, false},
	}
	for _, c := range cases {
		if got := shouldRetryStatus(c.code); got != c.want {
			t.Errorf("shouldRetryStatus(%d): got %v, want %v", c.code, got, c.want)
		}
	}
}

// TestEnvIntAtLeastOne は env パーサが 0 や負数を fallback に戻すことを確認する。
func TestEnvIntAtLeastOne(t *testing.T) {
	t.Setenv("RETRY_OK", "5")
	if got := envIntAtLeastOne("RETRY_OK", 1); got != 5 {
		t.Errorf("got %d, want 5", got)
	}
	t.Setenv("RETRY_ZERO", "0")
	if got := envIntAtLeastOne("RETRY_ZERO", 7); got != 7 {
		t.Errorf("0 should fall back: got %d, want 7", got)
	}
	t.Setenv("RETRY_NEG", "-3")
	if got := envIntAtLeastOne("RETRY_NEG", 7); got != 7 {
		t.Errorf("negative should fall back: got %d, want 7", got)
	}
	t.Setenv("RETRY_BAD", "xyz")
	if got := envIntAtLeastOne("RETRY_BAD", 9); got != 9 {
		t.Errorf("non-numeric should fall back: got %d, want 9", got)
	}
}

// TestEnvMillis は ミリ秒 env パーサの挙動を確認する。
func TestEnvMillis(t *testing.T) {
	t.Setenv("BACKOFF_OK", "250")
	if got := envMillis("BACKOFF_OK", 100*time.Millisecond); got != 250*time.Millisecond {
		t.Errorf("got %v, want 250ms", got)
	}
	t.Setenv("BACKOFF_ZERO", "0")
	if got := envMillis("BACKOFF_ZERO", 100*time.Millisecond); got != 0 {
		t.Errorf("zero allowed: got %v", got)
	}
	t.Setenv("BACKOFF_BAD", "abc")
	if got := envMillis("BACKOFF_BAD", 100*time.Millisecond); got != 100*time.Millisecond {
		t.Errorf("non-numeric fallback: got %v", got)
	}
}
