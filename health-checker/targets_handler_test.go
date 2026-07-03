package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /targets が起動時に構築した ServiceTarget を JSON で返すことを確認する。
// 検証点:
//   - HTTP 200
//   - Content-Type: application/json
//   - `count` はスライスの長さと一致
//   - `targets` は input と同じ順序・内容
func TestTargetsHandlerReturnsConfiguredTargets(t *testing.T) {
	targets := []ServiceTarget{
		{Name: "analytics-api", URL: "http://localhost:8001/health"},
		{Name: "api-gateway", URL: "http://localhost:8000/health"},
	}
	handler := makeTargetsHandler(targets)

	req := httptest.NewRequest(http.MethodGet, "/targets", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type: got %q, want application/json prefix", ct)
	}

	var body struct {
		Count   int             `json:"count"`
		Targets []ServiceTarget `json:"targets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != len(targets) {
		t.Errorf("count: got %d, want %d", body.Count, len(targets))
	}
	if len(body.Targets) != len(targets) {
		t.Fatalf("targets len: got %d, want %d", len(body.Targets), len(targets))
	}
	for i, want := range targets {
		if body.Targets[i] != want {
			t.Errorf("targets[%d]: got %+v, want %+v", i, body.Targets[i], want)
		}
	}
}

// nil スライスを渡した場合、レスポンスの targets は "null" ではなく `[]` に
// 正規化されること。クライアントは常に配列としてループできるようになる。
func TestTargetsHandlerEmitsEmptyArrayForNil(t *testing.T) {
	handler := makeTargetsHandler(nil)
	req := httptest.NewRequest(http.MethodGet, "/targets", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}

	// 生 JSON にも "null" が現れないことを確認（構造体デコードだと nil と [] が
	// 区別できないので、生テキスト側で正規化された表現を検証する）。
	if strings.Contains(rec.Body.String(), `"targets":null`) {
		t.Errorf("expected empty array for targets, got: %s", rec.Body.String())
	}

	var body struct {
		Count   int             `json:"count"`
		Targets []ServiceTarget `json:"targets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 0 {
		t.Errorf("count: got %d, want 0", body.Count)
	}
	if body.Targets == nil {
		t.Errorf("targets should be non-nil empty slice, got nil")
	}
	if len(body.Targets) != 0 {
		t.Errorf("targets len: got %d, want 0", len(body.Targets))
	}
}

// HEAD リクエストも 200 で応答し、既存 /health と同じメソッド許可規約を満たすこと。
// HEAD ではボディを返さないのが仕様だが、net/http の httptest は Recorder で
// ボディを覗けるので、少なくともステータスとメソッド許可の副作用を確認する。
func TestTargetsHandlerAllowsHeadMethod(t *testing.T) {
	handler := makeTargetsHandler([]ServiceTarget{{Name: "x", URL: "y"}})
	req := httptest.NewRequest(http.MethodHead, "/targets", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
}

// GET / HEAD 以外のメソッドは 405 を返し、Allow ヘッダを付与すること。
// 既存 methodAllowed のセマンティクスと整合させる。
func TestTargetsHandlerRejectsUnsupportedMethods(t *testing.T) {
	handler := makeTargetsHandler([]ServiceTarget{{Name: "x", URL: "y"}})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/targets", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("method=%s status: got %d, want 405", method, rec.Code)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, http.MethodGet) {
			t.Errorf("method=%s Allow header: got %q, want to contain GET", method, allow)
		}
	}
}

// NewTargets() が EXTRA_TARGETS を反映した結果を targets ハンドラで観測できることを
// 統合的に確認する。運用時の主眼である「EXTRA_TARGETS の JSON が反映されたか」を
// エンドツーエンドで検証する。
func TestTargetsHandlerReflectsExtraTargetsFromEnv(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics.example:8001")
	t.Setenv("GATEWAY_URL", "http://gw.example:8000")
	t.Setenv("EXTRA_TARGETS", `[{"name":"user-svc","url":"http://user.example/health"}]`)

	targets := NewTargets()
	handler := makeTargetsHandler(targets)

	req := httptest.NewRequest(http.MethodGet, "/targets", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}

	var body struct {
		Count   int             `json:"count"`
		Targets []ServiceTarget `json:"targets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if body.Count != 3 {
		t.Fatalf("count: got %d, want 3 (analytics + gateway + user-svc)", body.Count)
	}
	// EXTRA_TARGETS は末尾に append される。
	last := body.Targets[len(body.Targets)-1]
	if last.Name != "user-svc" || last.URL != "http://user.example/health" {
		t.Errorf("last target: got %+v, want user-svc", last)
	}
}
