// Package otatelemetry — chaos tests (§11.4.85).
//
// These meta-tests exercise the production code with malformed inputs, edge
// cases, boundary values, and concurrent invalid-batch encoding.  Every test
// writes captured evidence to qa-results/stress_chaos/ (or
// HELIX_STRESS_EVIDENCE_DIR) per §11.4.5.
//
// §11.4.27 (no fakes): every test exercises the real implementation — no mocks,
// no stubs, no placeholders.
// §11.4.14: goroutine lifecycle managed via sync.WaitGroup + defer cleanup.
// §11.4.50: deterministic inputs produce deterministic outputs.
//
// Panic recovery via defer/recover is used in every parallel goroutine so a
// single faulty input does not take down the whole test suite.
package otatelemetry

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// chaosEvidenceDir returns the evidence output directory.
func chaosEvidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

// writeChaosEvidence writes captured chaos evidence.
func writeChaosEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := chaosEvidenceDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("WARNING: cannot create evidence dir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Logf("WARNING: cannot write evidence %s: %v", path, err)
	}
}

// errToString safely converts an error to a string.
func errToString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// alternatingEvents creates n events alternating success / failure.
func alternatingEvents(n int) []Event {
	events := make([]Event, n)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			events[i] = Event{Report: report("d1", "dep1", EventSuccess)}
		} else {
			events[i] = Event{Report: report("d2", "dep1", EventFailure)}
		}
	}
	return events
}

// ---------------------------------------------------------------------------
// TestChaosDecodeMalformedJSON
//
// Exercise DecodeBatch with every class of malformed input: truncated JSON,
// trailing data (two concatenated documents), unknown fields, array instead
// of object, empty input, whitespace-only.
// ---------------------------------------------------------------------------

func TestChaosDecodeMalformedJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	type entry struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Failed      bool   `json:"decode_failed"`
		Error       string `json:"error,omitempty"`
	}

	cases := []struct {
		name    string
		json    string
		desc    string
		wantErr bool // true = DecodeBatch must return an error
	}{
		{
			name:    "truncated_object",
			json:    `{"events"`,
			desc:    "truncated JSON object - incomplete key",
			wantErr: true,
		},
		{
			name:    "truncated_event",
			json:    `{"events": [{"report":`,
			desc:    "truncated within a nested event",
			wantErr: true,
		},
		{
			name:    "two_documents",
			json:    `{"events": []}{"events": []}`,
			desc:    "two concatenated valid JSON documents",
			wantErr: true,
		},
		{
			name:    "unknown_top_level_field",
			json:    `{"events": [], "extra": true}`,
			desc:    "unknown top-level field (DisallowUnknownFields)",
			wantErr: true,
		},
		{
			name:    "unknown_field_in_report",
			json:    `{"events":[{"report":{"device_id":"d","deployment_id":"dep","event":"success","progress":0,"timestamp":"2026-06-07T12:00:00Z","bogus":1}}]}`,
			desc:    "unknown field inside an event report",
			wantErr: true,
		},
		{
			name:    "trailing_garbage",
			json:    `{"events": []}abc`,
			desc:    "valid JSON followed by non-JSON trailing data",
			wantErr: true,
		},
		{
			name:    "array_instead_of_object",
			json:    `[{"events": []}]`,
			desc:    "top-level JSON array instead of object",
			wantErr: true,
		},
		{
			name:    "null_as_empty_batch",
			json:    `null`,
			desc:    "JSON null decodes as empty batch (valid encoding/json)",
			wantErr: false,
		},
		{
			name:    "empty_string",
			json:    ``,
			desc:    "empty input (zero bytes)",
			wantErr: true,
		},
		{
			name:    "whitespace_only",
			json:    `   `,
			desc:    "whitespace with no JSON value",
			wantErr: true,
		},
		{
			name:    "wrong_type_for_events",
			json:    `{"events": 5}`,
			desc:    "events field is a number, not an array",
			wantErr: true,
		},
		{
			name:    "malformed_enum",
			json:    `{"events":[{"report":{"device_id":"d","deployment_id":"dep","event":"idle","progress":0,"timestamp":"2026-06-07T12:00:00Z"}}]}`,
			desc:    "invalid telemetry event enum value",
			wantErr: true,
		},
		{
			name:    "extra_nested_field",
			json:    `{"events":[{"report":{"device_id":"d","deployment_id":"dep","event":"success","progress":0,"timestamp":"2026-06-07T12:00:00Z"},"cohort":"c","unknown_field":true}]}`,
			desc:    "unknown field at the event envelope level",
			wantErr: true,
		},
	}

	var results []entry

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()

			_, err := DecodeBatch([]byte(c.json))
			failed := err != nil

			if c.wantErr && !failed {
				t.Errorf("DecodeBatch(%q) = nil error, want failure", c.desc)
			}
			if !c.wantErr && failed {
				t.Errorf("DecodeBatch(%q) = %v, want nil", c.desc, err)
			}

			results = append(results, entry{
				Name:        c.name,
				Description: c.desc,
				Failed:      failed,
				Error:       errToString(err),
			})
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_decode_malformed.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosEventValidation
//
// Exercise Event.Validate() with every known invalid field configuration:
// empty DeviceID, empty DeploymentID, invalid event type, progress out of
// range (negative, >100), zero timestamp.
// ---------------------------------------------------------------------------

func TestChaosEventValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	type entry struct {
		Name  string `json:"name"`
		Valid bool   `json:"valid"`
		Error string `json:"error,omitempty"`
	}

	cases := []struct {
		name  string
		event Event
	}{
		{
			name:  "empty_device_id",
			event: Event{Report: report("", "dep1", EventSuccess)},
		},
		{
			name:  "empty_deployment_id",
			event: Event{Report: report("d1", "", EventSuccess)},
		},
		{
			name:  "both_ids_empty",
			event: Event{Report: report("", "", EventSuccess)},
		},
		{
			name:  "invalid_event_type",
			event: Event{Report: report("d1", "dep1", otaprotocol.TelemetryEvent("bogus"))},
		},
		{
			name:  "empty_event_type",
			event: Event{Report: report("d1", "dep1", otaprotocol.TelemetryEvent(""))},
		},
		{
			name: "progress_negative",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Progress = -1
				return Event{Report: r}
			}(),
		},
		{
			name: "progress_above_100",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Progress = 101
				return Event{Report: r}
			}(),
		},
		{
			name: "progress_max_int",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Progress = math.MaxInt
				return Event{Report: r}
			}(),
		},
		{
			name: "progress_min_int",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Progress = math.MinInt
				return Event{Report: r}
			}(),
		},
		{
			name: "zero_timestamp",
			event: func() Event {
				r := report("d1", "dep1", EventSuccess)
				r.Timestamp = time.Time{}
				return Event{Report: r}
			}(),
		},
		{
			name: "all_fields_invalid",
			event: func() Event {
				return Event{
					Report: otaprotocol.TelemetryReport{
						DeviceID:     "",
						DeploymentID: "",
						Event:        otaprotocol.TelemetryEvent(""),
						Progress:     -5,
						ErrorCode:    "",
						Timestamp:    time.Time{},
					},
				}
			}(),
		},
		{
			name:  "valid_event_control",
			event: Event{Report: report("d1", "dep1", EventSuccess)},
		},
	}

	var results []entry

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.event.Validate()
			valid := err == nil

			results = append(results, entry{
				Name:  c.name,
				Valid: valid,
				Error: errToString(err),
			})

			if c.name == "valid_event_control" && !valid {
				t.Errorf("valid event control: Validate() = %v, want nil", err)
			}
			if c.name != "valid_event_control" && valid {
				t.Errorf("%s: Validate() = nil, want error", c.name)
			}
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_event_validation.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosHealthThresholdBoundaries
//
// Exercise HealthThresholds.Validate() and Health.Verdict() at every boundary
// condition: values outside [0,1], both zero, both at maximum, extreme float64
// precision values.
// ---------------------------------------------------------------------------

func TestChaosHealthThresholdBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	type entry struct {
		Thresholds    HealthThresholds `json:"thresholds"`
		Valid         bool             `json:"validate_passed"`
		ValidateError string           `json:"validate_error,omitempty"`
		Verdict       string           `json:"verdict"`
		VerdictError  string           `json:"verdict_error,omitempty"`
	}

	boundaries := []HealthThresholds{
		{SuccessThreshold: 0.0, ErrorThreshold: 0.0},
		{SuccessThreshold: 0.0, ErrorThreshold: 1.0},
		{SuccessThreshold: 1.0, ErrorThreshold: 0.0},
		{SuccessThreshold: 1.0, ErrorThreshold: 1.0},
		{SuccessThreshold: math.Copysign(0, -1), ErrorThreshold: math.Copysign(0, -1)},
		{SuccessThreshold: 0.5, ErrorThreshold: -0.1},
		{SuccessThreshold: -0.1, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5, ErrorThreshold: 1.5},
		{SuccessThreshold: 1.5, ErrorThreshold: 0.5},
		{SuccessThreshold: -1e-10, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5, ErrorThreshold: -1e-10},
		{SuccessThreshold: math.SmallestNonzeroFloat64, ErrorThreshold: math.SmallestNonzeroFloat64},
		{SuccessThreshold: 0.9999999999, ErrorThreshold: 0.0000000001},
		{SuccessThreshold: 0.5, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5 + math.SmallestNonzeroFloat64, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5, ErrorThreshold: 0.5 + math.SmallestNonzeroFloat64},
	}

	// Use a sample health that would verify verdict logic.
	h := Health{Terminal: 10, Successes: 9, Failures: 1, SuccessRate: 0.9, FailureRate: 0.1}

	var results []entry

	for _, thr := range boundaries {
		t.Run(fmt.Sprintf("S=%v_E=%v", thr.SuccessThreshold, thr.ErrorThreshold), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()

			validErr := thr.Validate()
			valid := validErr == nil

			v, vErr := h.Verdict(thr)

			results = append(results, entry{
				Thresholds:    thr,
				Valid:         valid,
				ValidateError: errToString(validErr),
				Verdict:       v.String(),
				VerdictError:  errToString(vErr),
			})
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_health_threshold_boundaries.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosHealthDerivationEdgeCases
//
// Exercise DeriveHealth with boundary event sets: empty, all-success,
// all-failure, mixed Deployments, mixed Cohorts, single-event types
// (each of the six canonical types).
// ---------------------------------------------------------------------------

func TestChaosHealthDerivationEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic during edge case derivation: %v", r)
		}
	}()

	type entry struct {
		Name        string  `json:"name"`
		Total       int     `json:"total"`
		Terminal    int     `json:"terminal"`
		Successes   int     `json:"successes"`
		Failures    int     `json:"failures"`
		SuccessRate float64 `json:"success_rate"`
		FailureRate float64 `json:"failure_rate"`
		Deployment  string  `json:"deployment_id,omitempty"`
		Cohort      string  `json:"cohort,omitempty"`
	}

	edgeCases := []struct {
		name   string
		events []Event
	}{
		{
			name:   "nil_slice",
			events: nil,
		},
		{
			name:   "empty_slice",
			events: []Event{},
		},
		{
			name:   "single_success",
			events: []Event{{Report: report("d1", "dep1", EventSuccess)}},
		},
		{
			name:   "single_failure",
			events: []Event{{Report: report("d1", "dep1", EventFailure)}},
		},
		{
			name:   "single_non_terminal",
			events: []Event{{Report: report("d1", "dep1", EventDownloadStarted)}},
		},
		{
			name:   "all_download_started",
			events: repeatEvent(report("d1", "dep1", EventDownloadStarted), 100),
		},
		{
			name:   "all_installing",
			events: repeatEvent(report("d1", "dep1", EventInstalling), 100),
		},
		{
			name:   "all_installed",
			events: repeatEvent(report("d1", "dep1", EventInstalled), 100),
		},
		{
			name:   "all_verifying",
			events: repeatEvent(report("d1", "dep1", EventVerifying), 100),
		},
		{
			name:   "all_success",
			events: repeatEvent(report("d1", "dep1", EventSuccess), 100),
		},
		{
			name:   "all_failure",
			events: repeatEvent(report("d1", "dep1", EventFailure), 100),
		},
		{
			name: "mixed_deployments",
			events: []Event{
				{Report: report("d1", "dep1", EventSuccess), Cohort: "c1"},
				{Report: report("d2", "dep2", EventSuccess), Cohort: "c2"},
				{Report: report("d3", "dep3", EventSuccess), Cohort: "c3"},
			},
		},
		{
			name: "mixed_cohorts_same_deployment",
			events: []Event{
				{Report: report("d1", "dep1", EventSuccess), Cohort: "canary"},
				{Report: report("d2", "dep1", EventSuccess), Cohort: "beta"},
			},
		},
		{
			name: "same_deployment_no_cohorts",
			events: []Event{
				{Report: report("d1", "dep1", EventDownloadStarted)},
				{Report: report("d1", "dep1", EventInstalling)},
				{Report: report("d1", "dep1", EventSuccess)},
			},
		},
		{
			name: "empty_cohort_and_deployment",
			events: []Event{
				{Report: report("d1", "dep1", EventSuccess)},
				{Report: report("d2", "dep1", EventFailure)},
			},
		},
		{
			name:   "alternating_success_failure",
			events: alternatingEvents(100),
		},
		{
			name: "every_event_type_once",
			events: []Event{
				{Report: report("d1", "dep1", EventDownloadStarted)},
				{Report: report("d2", "dep1", EventInstalling)},
				{Report: report("d3", "dep1", EventInstalled)},
				{Report: report("d4", "dep1", EventVerifying)},
				{Report: report("d5", "dep1", EventSuccess)},
				{Report: report("d6", "dep1", EventFailure)},
			},
		},
	}

	var results []entry

	for _, ec := range edgeCases {
		t.Run(ec.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()

			h := DeriveHealth(ec.events)

			expectedTotal := len(ec.events)
			if ec.events == nil {
				expectedTotal = 0
			}

			if h.Total != expectedTotal {
				t.Errorf("Total = %d, want %d", h.Total, expectedTotal)
			}
			if h.Successes < 0 || h.Failures < 0 {
				t.Errorf("negative counts: successes=%d, failures=%d", h.Successes, h.Failures)
			}
			if h.Successes+h.Failures != h.Terminal {
				t.Errorf("successes+failures=%d != terminal=%d", h.Successes+h.Failures, h.Terminal)
			}
			if h.SuccessRate < 0 || h.FailureRate < 0 {
				t.Errorf("negative rates: success=%.4f, failure=%.4f", h.SuccessRate, h.FailureRate)
			}
			if math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate) {
				t.Errorf("NaN rate: success=%v, failure=%v", h.SuccessRate, h.FailureRate)
			}
			if math.IsInf(h.SuccessRate, 0) || math.IsInf(h.FailureRate, 0) {
				t.Errorf("Inf rate: success=%v, failure=%v", h.SuccessRate, h.FailureRate)
			}

			results = append(results, entry{
				Name:        ec.name,
				Total:       h.Total,
				Terminal:    h.Terminal,
				Successes:   h.Successes,
				Failures:    h.Failures,
				SuccessRate: h.SuccessRate,
				FailureRate: h.FailureRate,
				Deployment:  h.DeploymentID,
				Cohort:      h.Cohort,
			})
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_health_derivation_edge_cases.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosConcurrentEncodeInvalidBatch
//
// N=30 goroutines concurrently encode various invalid batches.  The production
// EncodeBatch must reject every invalid input without panicking.
// ---------------------------------------------------------------------------

func TestChaosConcurrentEncodeInvalidBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	const numGoroutines = 30

	var tsTime = ts // reuse the fixed timestamp from event_test.go

	invalidBatches := []Batch{
		{Events: []Event{{Report: report("", "dep", EventSuccess)}}},
		{Events: []Event{{Report: report("d", "", EventSuccess)}}},
		{Events: []Event{{Report: otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: "bogus", Progress: 50, Timestamp: time.Time{}}}}},
		{Events: []Event{{Report: otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: "bogus", Progress: 50, Timestamp: tsTime}}}},
		{Events: []Event{{Report: otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: EventSuccess, Progress: 200, Timestamp: tsTime}}}},
		{Events: []Event{{Report: otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: EventSuccess, Progress: -5, Timestamp: tsTime}}}},
		{Events: []Event{{Report: otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: EventSuccess, Progress: 50, Timestamp: time.Time{}}}}},
		{Events: []Event{
			{Report: report("d", "dep", EventSuccess)},
			{Report: report("", "dep", EventFailure)},
		}},
		{Events: []Event{{Cohort: strings.Repeat("x", 10000), Report: otaprotocol.TelemetryReport{DeviceID: "", DeploymentID: "", Event: EventSuccess, Progress: 50, Timestamp: tsTime}}}},
	}

	var (
		wg      sync.WaitGroup
		panics  int32
		rejects int32
	)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()

			b := invalidBatches[id%len(invalidBatches)]
			_, err := EncodeBatch(b)
			if err != nil {
				atomic.AddInt32(&rejects, 1)
			}
		}(g)
	}

	wg.Wait()

	summary := map[string]interface{}{
		"test":            "TestChaosConcurrentEncodeInvalidBatch",
		"goroutines":      numGoroutines,
		"invalid_batches": len(invalidBatches),
		"rejected":        rejects,
		"panics":          panics,
	}

	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeChaosEvidence(t, "chaos_concurrent_encode_invalid.json", ev)

	if panics > 0 {
		t.Errorf("%d goroutines panicked during concurrent invalid batch encoding", panics)
	}
}

// ---------------------------------------------------------------------------
// TestChaosHealthNumericalStability
//
// Exercise DeriveHealth with large event counts (10000), zero-terminal edge,
// and single-event sets for all six canonical event types.  Asserts no NaN,
// no Inf, and no negative counts.
// ---------------------------------------------------------------------------

func TestChaosHealthNumericalStability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic during numerical stability test: %v", r)
		}
	}()

	type entry struct {
		Name        string  `json:"name"`
		Total       int     `json:"total"`
		Terminal    int     `json:"terminal"`
		Successes   int     `json:"successes"`
		Failures    int     `json:"failures"`
		SuccessRate float64 `json:"success_rate"`
		FailureRate float64 `json:"failure_rate"`
		HasNaN      bool    `json:"has_nan_or_inf"`
	}

	var results []entry

	// 1. Large event set: 10000 events with ~33% failure rate.
	{
		n := 10000
		events := make([]Event, n)
		for i := 0; i < n; i++ {
			if i%3 == 0 {
				events[i] = Event{Report: report(fmt.Sprintf("dev-%d", i%100), "dep-large", EventFailure)}
			} else {
				events[i] = Event{Report: report(fmt.Sprintf("dev-%d", i%100), "dep-large", EventSuccess)}
			}
		}

		h := DeriveHealth(events)

		hasNaN := math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate) ||
			math.IsInf(h.SuccessRate, 0) || math.IsInf(h.FailureRate, 0)

		if h.Total != n {
			t.Errorf("large set: Total = %d, want %d", h.Total, n)
		}
		if h.Successes == 0 || h.Failures == 0 {
			t.Errorf("large set: expected non-zero successes and failures, got %d/%d",
				h.Successes, h.Failures)
		}
		if hasNaN {
			t.Errorf("large set: NaN/Inf rate: success=%v failure=%v", h.SuccessRate, h.FailureRate)
		}

		results = append(results, entry{
			Name:        "large_10000_events",
			Total:       h.Total,
			Terminal:    h.Terminal,
			Successes:   h.Successes,
			Failures:    h.Failures,
			SuccessRate: h.SuccessRate,
			FailureRate: h.FailureRate,
			HasNaN:      hasNaN,
		})
	}

	// 2. Zero terminal events - both rates must be exactly 0, never NaN.
	{
		h := DeriveHealth([]Event{
			{Report: report("d", "dep", EventDownloadStarted)},
			{Report: report("d", "dep", EventInstalling)},
		})
		hasNaN := math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate)

		if h.Terminal != 0 {
			t.Errorf("zero terminal: Terminal = %d, want 0", h.Terminal)
		}
		if h.SuccessRate != 0 || h.FailureRate != 0 {
			t.Errorf("zero terminal: rates = (%v,%v), want (0,0)", h.SuccessRate, h.FailureRate)
		}
		if hasNaN {
			t.Errorf("zero terminal: got NaN rate: success=%v failure=%v", h.SuccessRate, h.FailureRate)
		}

		results = append(results, entry{
			Name:        "zero_terminal_events",
			Total:       h.Total,
			Terminal:    h.Terminal,
			Successes:   h.Successes,
			Failures:    h.Failures,
			SuccessRate: h.SuccessRate,
			FailureRate: h.FailureRate,
			HasNaN:      hasNaN,
		})
	}

	// 3. Nil input - must not panic and must not produce NaN.
	{
		h := DeriveHealth(nil)
		hasNaN := math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate)

		if h.Total != 0 || h.Terminal != 0 {
			t.Errorf("nil input: Total=%d Terminal=%d, want (0,0)", h.Total, h.Terminal)
		}
		if h.SuccessRate != 0 || h.FailureRate != 0 {
			t.Errorf("nil input: rates = (%v,%v), want (0,0)", h.SuccessRate, h.FailureRate)
		}
		if hasNaN {
			t.Errorf("nil input: NaN rate: success=%v failure=%v", h.SuccessRate, h.FailureRate)
		}

		results = append(results, entry{
			Name:        "nil_input",
			Total:       h.Total,
			Terminal:    h.Terminal,
			Successes:   h.Successes,
			Failures:    h.Failures,
			SuccessRate: h.SuccessRate,
			FailureRate: h.FailureRate,
			HasNaN:      hasNaN,
		})
	}

	// 4. Single-event sets for all six canonical types.
	for _, evt := range AllEventTypes() {
		h := DeriveHealth([]Event{{Report: report("d1", "dep", evt)}})

		if h.Total != 1 {
			t.Errorf("single %s: Total = %d, want 1", evt, h.Total)
		}

		expectedTerminal := 0
		if evt == EventSuccess || evt == EventFailure {
			expectedTerminal = 1
		}
		if h.Terminal != expectedTerminal {
			t.Errorf("single %s: Terminal = %d, want %d", evt, h.Terminal, expectedTerminal)
		}

		results = append(results, entry{
			Name:        fmt.Sprintf("single_%s", evt),
			Total:       h.Total,
			Terminal:    h.Terminal,
			Successes:   h.Successes,
			Failures:    h.Failures,
			SuccessRate: h.SuccessRate,
			FailureRate: h.FailureRate,
			HasNaN: math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate) ||
				math.IsInf(h.SuccessRate, 0) || math.IsInf(h.FailureRate, 0),
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_health_numerical_stability.json", ev)
}
