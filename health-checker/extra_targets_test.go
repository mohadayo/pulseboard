package main

import (
	"testing"
)

// parseExtraTargets の境界条件テスト。
//
// EXTRA_TARGETS 環境変数は運用時にオペレータが手書きで注入するため、
// パース失敗（不正 JSON・型不一致）・空エントリ（name/url 抜け）に対して
// fail-open で挙動を明示的に保証する。
func TestParseExtraTargets(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    []ServiceTarget
		wantErr bool
	}{
		{
			name: "empty string returns nil without error",
			raw:  "",
			want: nil,
		},
		{
			name: "empty JSON array returns empty slice",
			raw:  "[]",
			want: []ServiceTarget{},
		},
		{
			name: "single valid entry",
			raw:  `[{"name":"redis","url":"http://redis:6379/ping"}]`,
			want: []ServiceTarget{{Name: "redis", URL: "http://redis:6379/ping"}},
		},
		{
			name: "multiple valid entries preserve order",
			raw:  `[{"name":"a","url":"http://a"},{"name":"b","url":"http://b"},{"name":"c","url":"http://c"}]`,
			want: []ServiceTarget{
				{Name: "a", URL: "http://a"},
				{Name: "b", URL: "http://b"},
				{Name: "c", URL: "http://c"},
			},
		},
		{
			name: "entry missing name is skipped",
			raw:  `[{"name":"","url":"http://x"},{"name":"y","url":"http://y"}]`,
			want: []ServiceTarget{{Name: "y", URL: "http://y"}},
		},
		{
			name: "entry missing url is skipped",
			raw:  `[{"name":"x","url":""},{"name":"y","url":"http://y"}]`,
			want: []ServiceTarget{{Name: "y", URL: "http://y"}},
		},
		{
			name: "all entries missing required fields returns empty",
			raw:  `[{"name":"","url":""},{"name":"only-name"}]`,
			want: []ServiceTarget{},
		},
		{
			name:    "invalid JSON returns error",
			raw:     `not json`,
			wantErr: true,
		},
		{
			name:    "JSON object instead of array returns error",
			raw:     `{"name":"x","url":"http://x"}`,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseExtraTargets(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, got nil", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d entries, want %d: got=%+v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i].Name != tc.want[i].Name || got[i].URL != tc.want[i].URL {
					t.Errorf("entry[%d]: got %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// NewTargets が EXTRA_TARGETS 環境変数から追加ターゲットを取り込むことを検証する。
// デフォルトの 2 ターゲット (analytics-api, api-gateway) の後ろに append されること、
// パース失敗時はデフォルトターゲットのみで返ることを保証する。
func TestNewTargets_AppendsExtraTargetsFromEnv(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics")
	t.Setenv("GATEWAY_URL", "http://gateway")
	t.Setenv("EXTRA_TARGETS", `[{"name":"mailer","url":"http://mailer/healthz"},{"name":"redis","url":"http://redis:6379/ping"}]`)

	targets := NewTargets()

	if got, want := len(targets), 4; got != want {
		t.Fatalf("len(targets) = %d, want %d: %+v", got, want, targets)
	}
	if targets[0].Name != "analytics-api" {
		t.Errorf("targets[0].Name = %q, want analytics-api", targets[0].Name)
	}
	if targets[1].Name != "api-gateway" {
		t.Errorf("targets[1].Name = %q, want api-gateway", targets[1].Name)
	}
	if targets[2].Name != "mailer" || targets[2].URL != "http://mailer/healthz" {
		t.Errorf("targets[2] = %+v, want {mailer http://mailer/healthz}", targets[2])
	}
	if targets[3].Name != "redis" || targets[3].URL != "http://redis:6379/ping" {
		t.Errorf("targets[3] = %+v, want {redis http://redis:6379/ping}", targets[3])
	}
}

func TestNewTargets_InvalidExtraTargetsFallsBackToDefaults(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics")
	t.Setenv("GATEWAY_URL", "http://gateway")
	t.Setenv("EXTRA_TARGETS", `this is not json`)

	targets := NewTargets()

	if got, want := len(targets), 2; got != want {
		t.Fatalf("len(targets) = %d, want %d: %+v", got, want, targets)
	}
}

func TestNewTargets_EmptyExtraTargetsReturnsDefaults(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics")
	t.Setenv("GATEWAY_URL", "http://gateway")
	t.Setenv("EXTRA_TARGETS", "")

	targets := NewTargets()

	if got, want := len(targets), 2; got != want {
		t.Fatalf("len(targets) = %d, want %d: %+v", got, want, targets)
	}
}

// EXTRA_TARGETS がデフォルトターゲット (analytics-api / api-gateway) と
// 同じ name を持つ場合、`checkAndReportTargets` が 1 サイクルで同名メトリクスを
// 2 件送ってしまう二重計上を防ぐため、extras 側を捨てる。
// URL 面ではデフォルト（ANALYTICS_URL / GATEWAY_URL 由来）が正なので
// 先勝ちで残す。
func TestNewTargets_DropsExtraTargetsCollidingWithDefaults(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics")
	t.Setenv("GATEWAY_URL", "http://gateway")
	t.Setenv("EXTRA_TARGETS", `[
        {"name":"analytics-api","url":"http://old-analytics/health"},
        {"name":"api-gateway","url":"http://old-gateway/health"},
        {"name":"mailer","url":"http://mailer/healthz"}
    ]`)

	targets := NewTargets()

	if got, want := len(targets), 3; got != want {
		t.Fatalf("len(targets) = %d, want %d: %+v", got, want, targets)
	}
	if targets[0].Name != "analytics-api" || targets[0].URL != "http://analytics/health" {
		t.Errorf("targets[0] = %+v, want default {analytics-api http://analytics/health}", targets[0])
	}
	if targets[1].Name != "api-gateway" || targets[1].URL != "http://gateway/health" {
		t.Errorf("targets[1] = %+v, want default {api-gateway http://gateway/health}", targets[1])
	}
	if targets[2].Name != "mailer" || targets[2].URL != "http://mailer/healthz" {
		t.Errorf("targets[2] = %+v, want {mailer http://mailer/healthz}", targets[2])
	}
}

// EXTRA_TARGETS 自体の内部で同じ name が複数回登場した場合、
// 最初の 1 件のみ採用し、以降の同名エントリはスキップする。
// v1 と v2 が交互に上書きされて分析側でメトリクスが揺らぐ状態を防ぐ。
func TestNewTargets_DropsSelfDuplicatesWithinExtraTargets(t *testing.T) {
	t.Setenv("ANALYTICS_URL", "http://analytics")
	t.Setenv("GATEWAY_URL", "http://gateway")
	t.Setenv("EXTRA_TARGETS", `[
        {"name":"payments","url":"http://payments-v1/health"},
        {"name":"payments","url":"http://payments-v2/health"},
        {"name":"search","url":"http://search/health"}
    ]`)

	targets := NewTargets()

	if got, want := len(targets), 4; got != want {
		t.Fatalf("len(targets) = %d, want %d: %+v", got, want, targets)
	}
	if targets[2].Name != "payments" || targets[2].URL != "http://payments-v1/health" {
		t.Errorf("targets[2] = %+v, want first-wins {payments http://payments-v1/health}", targets[2])
	}
	if targets[3].Name != "search" || targets[3].URL != "http://search/health" {
		t.Errorf("targets[3] = %+v, want {search http://search/health}", targets[3])
	}
}

// mergeExtraTargets のユニットテスト（NewTargets 経由の統合テストと独立に、
// 単体関数レベルで first-wins と defaults 優先の挙動を保証する）。
func TestMergeExtraTargets(t *testing.T) {
	defaults := []ServiceTarget{
		{Name: "a", URL: "http://a-default"},
		{Name: "b", URL: "http://b-default"},
	}
	extras := []ServiceTarget{
		{Name: "a", URL: "http://a-extra"}, // 既定と衝突 → drop
		{Name: "c", URL: "http://c-extra"}, // 新規 → 採用
		{Name: "c", URL: "http://c-dup"},   // 自己重複 → drop
		{Name: "d", URL: "http://d-extra"}, // 新規 → 採用
	}

	got := mergeExtraTargets(defaults, extras)

	want := []ServiceTarget{
		{Name: "a", URL: "http://a-default"},
		{Name: "b", URL: "http://b-default"},
		{Name: "c", URL: "http://c-extra"},
		{Name: "d", URL: "http://d-extra"},
	}
	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}
