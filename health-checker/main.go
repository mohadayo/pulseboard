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

func ReportMetric(client *http.Client, analyticsURL string, result CheckResult) error {
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

	resp, err := client.Post(analyticsURL+"/metrics", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[WARN] Failed to report metric for %s: %v", result.Service, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("analytics API returned %d", resp.StatusCode)
	}
	log.Printf("[INFO] Reported metric for %s to analytics", result.Service)
	return nil
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status:  "healthy",
		Service: "health-checker",
	})
}

func makeCheckHandler(targets []ServiceTarget, analyticsURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client := &http.Client{Timeout: 5 * time.Second}
		results := make([]CheckResult, 0, len(targets))

		for _, t := range targets {
			result := CheckService(client, t)
			results = append(results, result)
		}

		reported := 0
		for _, result := range results {
			if err := ReportMetric(client, analyticsURL, result); err == nil {
				reported++
			}
		}

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
		Addr:    ":" + port,
		Handler: mux,
	}

	shutdownTimeout := 30 * time.Second
	if v := os.Getenv("SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			shutdownTimeout = time.Duration(n) * time.Second
		}
	}

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
