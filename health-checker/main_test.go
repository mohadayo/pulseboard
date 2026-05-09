package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

