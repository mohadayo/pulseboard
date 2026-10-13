package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestCheckService_ReusesTCPConnection は CheckService が HTTP keep-alive を
// 阻害せず、連続チェックで TCP 接続を使い回せることを確認する。
//
// 旧実装では `defer resp.Body.Close()` だけで body を読み切っていなかったため、
// net/http.Client は接続を keep-alive プールへ戻さず破棄していた。本番の
// `CHECK_INTERVAL_SECONDS` 設定で periodic ループが毎 interval 新規 TCP
// ハンドシェイクを張り直すコストが積み上がる。
//
// 検出方法: httptest サーバの `ConnState` フックで接続の total 数を数え、
// 連続 3 回のチェックで 1 本しか張られていない（＝再利用された）ことを確認する。
func TestCheckService_ReusesTCPConnection(t *testing.T) {
	var totalConns int32

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		// `/health` が返す典型的な小ボディ。これを読み切らないと keep-alive が切れる。
		_, _ = io.WriteString(w, `{"status":"healthy","service":"test"}`)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&totalConns, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	// httptest.Server.Client() は内部 Transport の MaxIdleConnsPerHost を持ち、
	// 既定で keep-alive 有効のクライアントを返す。
	client := srv.Client()
	target := ServiceTarget{Name: "svc", URL: srv.URL + "/health"}

	for i := 0; i < 3; i++ {
		result := CheckService(client, target)
		if result.Status != "healthy" {
			t.Fatalf("iteration %d: expected healthy, got %q (error=%q)", i, result.Status, result.Error)
		}
	}

	if got := atomic.LoadInt32(&totalConns); got != 1 {
		t.Errorf(
			"expected TCP connection reuse (totalConns=1 after 3 checks), got %d — "+
				"body may not be drained before close, which disables keep-alive",
			got,
		)
	}
}

// TestReportMetric_ReusesTCPConnection は reportMetricWithPolicy が
// 連続 POST で TCP 接続を使い回せることを確認する。
//
// 旧実装は `resp.Body.Close()` を直接呼んでおり、body が小さくても
// 読み切っていない場合に net/http.Client が接続をドロップする挙動に該当した。
// 連投リトライ・ペリオディックループ経由で analytics-api に大量に投げる経路で
// ハンドシェイクが積み上がらないよう、drain を保証する回帰テスト。
func TestReportMetric_ReusesTCPConnection(t *testing.T) {
	var totalConns int32

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// 本番 analytics-api の 201 Created レスポンスを模す。
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"ok":true,"id":"abc123"}`)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&totalConns, 1)
		}
	}
	srv.Start()
	defer srv.Close()

	client := srv.Client()
	policy := reportMetricRetryPolicy{maxAttempts: 1, backoff: 1 * time.Millisecond}
	result := CheckResult{Service: "svc", Status: "healthy", ResponseTimeMs: 10, Timestamp: 1}

	for i := 0; i < 3; i++ {
		if err := reportMetricWithPolicy(client, srv.URL, result, policy); err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
	}

	if got := atomic.LoadInt32(&totalConns); got != 1 {
		t.Errorf(
			"expected TCP connection reuse (totalConns=1 after 3 reports), got %d — "+
				"body may not be drained before close, which disables keep-alive",
			got,
		)
	}
}

// TestDrainAndClose_ReadsAllBytes は drainAndClose がボディを読み切ることを
// 直接確認する。ioutil 挙動の回帰（将来 io.Copy を外す等）を防ぐ目的。
func TestDrainAndClose_ReadsAllBytes(t *testing.T) {
	payload := strings.Repeat("x", 4096)
	body := &countingReadCloser{reader: strings.NewReader(payload)}

	drainAndClose(body)

	if !body.closed {
		t.Error("drainAndClose must close the body")
	}
	if body.read != len(payload) {
		t.Errorf("drainAndClose must read all bytes; got %d, want %d", body.read, len(payload))
	}
}

// countingReadCloser は Read バイト数と Close 呼び出し有無を記録する簡易
// io.ReadCloser 実装。drainAndClose がボディを読み切り、かつ閉じたことを
// 1 つのオブジェクトで同時に検証するために使う。
type countingReadCloser struct {
	reader *strings.Reader
	read   int
	closed bool
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	c.read += n
	return n, err
}

func (c *countingReadCloser) Close() error {
	c.closed = true
	return nil
}
