package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /config が起動時に構築した RuntimeConfig を JSON で返すことを確認する。
// 検証点:
//   - HTTP 200
//   - Content-Type: application/json
//   - 各キーが期待値と一致
func TestConfigHandlerReturnsRuntimeConfig(t *testing.T) {
	config := RuntimeConfig{
		AnalyticsURL:            "http://analytics.example:8001",
		CheckIntervalSeconds:    30,
		CheckHTTPTimeoutSeconds: 5,
		MetricReportMaxAttempts: 3,
		MetricReportBackoffMs:   100,
		Server: ServerConfig{
			ReadHeaderTimeoutSeconds: 5,
			ReadTimeoutSeconds:       15,
			WriteTimeoutSeconds:      15,
			IdleTimeoutSeconds:       60,
		},
		ShutdownTimeoutSeconds: 30,
	}
	handler := makeConfigHandler(config)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type: got %q, want application/json prefix", ct)
	}

	var body RuntimeConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body != config {
		t.Errorf("body: got %+v, want %+v", body, config)
	}
}

// HEAD /config も 200 で応答し、既存 /targets と同じメソッド許可規約を満たすこと。
func TestConfigHandlerAllowsHeadMethod(t *testing.T) {
	handler := makeConfigHandler(RuntimeConfig{})
	req := httptest.NewRequest(http.MethodHead, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
}

// GET / HEAD 以外のメソッドは 405 を返し、Allow ヘッダを付与すること。
func TestConfigHandlerRejectsUnsupportedMethods(t *testing.T) {
	handler := makeConfigHandler(RuntimeConfig{})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/config", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("method=%s status: got %d, want 405", method, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
			t.Errorf("method=%s Allow header: got %q, want to contain GET", method, allow)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodHead) {
			t.Errorf("method=%s Allow header: got %q, want to contain HEAD", method, allow)
		}
	}
}

// loadRuntimeConfig() は env 未設定時にドキュメントされているデフォルト値を返すこと。
// 各デフォルト値は main() 内の env 読み取り箇所と同一である必要がある（drift 防止）。
func TestLoadRuntimeConfigDefaultsMatchMain(t *testing.T) {
	// env は Setenv せず既定値のみで検査する。
	// Go test は各テストで env を隔離しないので、CI に依存しないよう
	// 各キーを明示的にクリアする。
	t.Setenv("ANALYTICS_URL", "")
	t.Setenv("CHECK_INTERVAL_SECONDS", "")
	t.Setenv("CHECK_HTTP_TIMEOUT_SECONDS", "")
	t.Setenv("METRIC_REPORT_MAX_ATTEMPTS", "")
	t.Setenv("METRIC_REPORT_BACKOFF_MS", "")
	t.Setenv("CHECKER_READ_HEADER_TIMEOUT", "")
	t.Setenv("CHECKER_READ_TIMEOUT", "")
	t.Setenv("CHECKER_WRITE_TIMEOUT", "")
	t.Setenv("CHECKER_IDLE_TIMEOUT", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")

	cfg := loadRuntimeConfig()

	if cfg.AnalyticsURL != "http://localhost:8001" {
		t.Errorf("AnalyticsURL default: got %q, want http://localhost:8001", cfg.AnalyticsURL)
	}
	if cfg.CheckIntervalSeconds != 0 {
		t.Errorf("CheckIntervalSeconds default: got %d, want 0", cfg.CheckIntervalSeconds)
	}
	if cfg.CheckHTTPTimeoutSeconds != 5 {
		t.Errorf("CheckHTTPTimeoutSeconds default: got %d, want 5", cfg.CheckHTTPTimeoutSeconds)
	}
	if cfg.MetricReportMaxAttempts != 3 {
		t.Errorf("MetricReportMaxAttempts default: got %d, want 3", cfg.MetricReportMaxAttempts)
	}
	if cfg.MetricReportBackoffMs != 100 {
		t.Errorf("MetricReportBackoffMs default: got %d, want 100", cfg.MetricReportBackoffMs)
	}
	if cfg.Server.ReadHeaderTimeoutSeconds != 5 {
		t.Errorf("ReadHeaderTimeout default: got %d, want 5", cfg.Server.ReadHeaderTimeoutSeconds)
	}
	if cfg.Server.ReadTimeoutSeconds != 15 {
		t.Errorf("ReadTimeout default: got %d, want 15", cfg.Server.ReadTimeoutSeconds)
	}
	if cfg.Server.WriteTimeoutSeconds != 15 {
		t.Errorf("WriteTimeout default: got %d, want 15", cfg.Server.WriteTimeoutSeconds)
	}
	if cfg.Server.IdleTimeoutSeconds != 60 {
		t.Errorf("IdleTimeout default: got %d, want 60", cfg.Server.IdleTimeoutSeconds)
	}
	if cfg.ShutdownTimeoutSeconds != 30 {
		t.Errorf("ShutdownTimeout default: got %d, want 30", cfg.ShutdownTimeoutSeconds)
	}
}

// env で上書きされた値が RuntimeConfig に反映されること。
// 「本当に env が効いたか」を確認するのが本エンドポイントの目的なので、
// 主要 env の反映を統合的にテストする。
func TestLoadRuntimeConfigReflectsEnvOverrides(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics.example:9001")
	t.Setenv("CHECK_INTERVAL_SECONDS", "45")
	t.Setenv("CHECK_HTTP_TIMEOUT_SECONDS", "10")
	t.Setenv("METRIC_REPORT_MAX_ATTEMPTS", "5")
	t.Setenv("METRIC_REPORT_BACKOFF_MS", "250")
	t.Setenv("CHECKER_READ_HEADER_TIMEOUT", "3")
	t.Setenv("CHECKER_READ_TIMEOUT", "20")
	t.Setenv("CHECKER_WRITE_TIMEOUT", "25")
	t.Setenv("CHECKER_IDLE_TIMEOUT", "90")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "12")

	cfg := loadRuntimeConfig()

	if cfg.AnalyticsURL != "http://analytics.example:9001" {
		t.Errorf("AnalyticsURL: got %q", cfg.AnalyticsURL)
	}
	if cfg.CheckIntervalSeconds != 45 {
		t.Errorf("CheckIntervalSeconds: got %d, want 45", cfg.CheckIntervalSeconds)
	}
	if cfg.CheckHTTPTimeoutSeconds != 10 {
		t.Errorf("CheckHTTPTimeoutSeconds: got %d, want 10", cfg.CheckHTTPTimeoutSeconds)
	}
	if cfg.MetricReportMaxAttempts != 5 {
		t.Errorf("MetricReportMaxAttempts: got %d, want 5", cfg.MetricReportMaxAttempts)
	}
	if cfg.MetricReportBackoffMs != 250 {
		t.Errorf("MetricReportBackoffMs: got %d, want 250", cfg.MetricReportBackoffMs)
	}
	if cfg.Server.ReadHeaderTimeoutSeconds != 3 {
		t.Errorf("ReadHeaderTimeout: got %d, want 3", cfg.Server.ReadHeaderTimeoutSeconds)
	}
	if cfg.Server.ReadTimeoutSeconds != 20 {
		t.Errorf("ReadTimeout: got %d, want 20", cfg.Server.ReadTimeoutSeconds)
	}
	if cfg.Server.WriteTimeoutSeconds != 25 {
		t.Errorf("WriteTimeout: got %d, want 25", cfg.Server.WriteTimeoutSeconds)
	}
	if cfg.Server.IdleTimeoutSeconds != 90 {
		t.Errorf("IdleTimeout: got %d, want 90", cfg.Server.IdleTimeoutSeconds)
	}
	if cfg.ShutdownTimeoutSeconds != 12 {
		t.Errorf("ShutdownTimeout: got %d, want 12", cfg.ShutdownTimeoutSeconds)
	}
}

// 不正値（負値・非数値）は既存 envSeconds / envIntAtLeastOne のフォールバック挙動により
// デフォルト値へ戻ること。運用時に typo で "3s" のような値を渡してもクラッシュせず、
// デフォルトの安全側パラメータで起動する fail-open が維持されていることを回帰する。
func TestLoadRuntimeConfigFallsBackOnInvalidValues(t *testing.T) {
	t.Setenv("CHECK_HTTP_TIMEOUT_SECONDS", "not-a-number")
	t.Setenv("METRIC_REPORT_BACKOFF_MS", "-50")
	t.Setenv("METRIC_REPORT_MAX_ATTEMPTS", "0") // envIntAtLeastOne は >=1 のみ受け入れる
	t.Setenv("CHECKER_READ_TIMEOUT", "abc")

	cfg := loadRuntimeConfig()

	if cfg.CheckHTTPTimeoutSeconds != 5 {
		t.Errorf("CheckHTTPTimeoutSeconds should fall back to 5 on invalid input, got %d", cfg.CheckHTTPTimeoutSeconds)
	}
	if cfg.MetricReportBackoffMs != 100 {
		t.Errorf("MetricReportBackoffMs should fall back to 100 on negative input, got %d", cfg.MetricReportBackoffMs)
	}
	if cfg.MetricReportMaxAttempts != 3 {
		t.Errorf("MetricReportMaxAttempts should fall back to 3 on <1 input, got %d", cfg.MetricReportMaxAttempts)
	}
	if cfg.Server.ReadTimeoutSeconds != 15 {
		t.Errorf("ReadTimeoutSeconds should fall back to 15 on invalid input, got %d", cfg.Server.ReadTimeoutSeconds)
	}
}

// makeConfigHandler は起動時にクロージャで固定した config を返し、
// 後から env を変更しても反映されないこと（ハンドラは不変を維持）。
// 運用中に env を書き換えても reload しない限り挙動は変わらないという設計を回帰する。
func TestConfigHandlerIsClosureOverConstructedConfig(t *testing.T) {
	original := RuntimeConfig{
		AnalyticsURL:            "http://original.example:8001",
		CheckIntervalSeconds:    30,
		CheckHTTPTimeoutSeconds: 5,
	}
	handler := makeConfigHandler(original)

	// env を書き換えても、既に構築済みのハンドラは影響を受けないはず。
	t.Setenv("ANALYTICS_URL", "http://mutated.example:9001")
	t.Setenv("CHECK_INTERVAL_SECONDS", "999")

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	var body RuntimeConfig
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.AnalyticsURL != "http://original.example:8001" {
		t.Errorf("handler should return the config captured at construction, got %q", body.AnalyticsURL)
	}
	if body.CheckIntervalSeconds != 30 {
		t.Errorf("handler should return the config captured at construction, got %d", body.CheckIntervalSeconds)
	}
}

// JSON レスポンスに `server` ネストが存在し、各キーがドキュメント通りの名前で
// 出力されること（tag drift を検知）。
func TestConfigHandlerJSONShapeIncludesServerNesting(t *testing.T) {
	config := RuntimeConfig{
		Server: ServerConfig{
			ReadHeaderTimeoutSeconds: 5,
			ReadTimeoutSeconds:       15,
			WriteTimeoutSeconds:      15,
			IdleTimeoutSeconds:       60,
		},
	}
	handler := makeConfigHandler(config)

	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	// 生 JSON にキー名が現れることを確認する。
	body := rec.Body.String()
	for _, key := range []string{
		"\"server\"",
		"\"read_header_timeout_seconds\"",
		"\"read_timeout_seconds\"",
		"\"write_timeout_seconds\"",
		"\"idle_timeout_seconds\"",
	} {
		if !strings.Contains(body, key) {
			t.Errorf("expected JSON to contain key %s, got: %s", key, body)
		}
	}
}

// トップレベルの JSON キー名が snake_case で、tag drift が無いこと。
func TestConfigHandlerJSONShapeUsesSnakeCase(t *testing.T) {
	handler := makeConfigHandler(RuntimeConfig{})
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body := rec.Body.String()
	expectedKeys := []string{
		"\"analytics_url\"",
		"\"check_interval_seconds\"",
		"\"check_http_timeout_seconds\"",
		"\"metric_report_max_attempts\"",
		"\"metric_report_backoff_ms\"",
		"\"shutdown_timeout_seconds\"",
	}
	for _, key := range expectedKeys {
		if !strings.Contains(body, key) {
			t.Errorf("expected JSON to contain snake_case key %s, got: %s", key, body)
		}
	}
	// camelCase / PascalCase を誤って出力していないこと
	unexpectedKeys := []string{
		"\"analyticsUrl\"",
		"\"AnalyticsURL\"",
		"\"checkIntervalSeconds\"",
		"\"CheckIntervalSeconds\"",
	}
	for _, key := range unexpectedKeys {
		if strings.Contains(body, key) {
			t.Errorf("unexpected non-snake_case key %s in JSON: %s", key, body)
		}
	}
}

// レスポンスに秘匿情報が含まれないことを回帰する（安全側の警戒）。
// 現状は ANALYTICS_URL 等の非秘匿情報のみだが、将来 secret を扱う env を
// 誤って RuntimeConfig に含めた場合の検知として、代表的な secret 由来のキー名や
// 値が漏出していないことを確認する。
func TestConfigHandlerDoesNotLeakSecrets(t *testing.T) {
	handler := makeConfigHandler(RuntimeConfig{
		AnalyticsURL: "http://analytics.example:8001",
	})
	req := httptest.NewRequest(http.MethodGet, "/config", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	body := strings.ToLower(rec.Body.String())
	forbidden := []string{"password", "token", "secret", "api_key", "apikey"}
	for _, needle := range forbidden {
		if strings.Contains(body, needle) {
			t.Errorf("config response should not contain %q (likely secret leak): %s", needle, rec.Body.String())
		}
	}
}
