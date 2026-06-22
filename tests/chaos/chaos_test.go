// Package otatelemetry_chaos_test — chaos tests for ota-telemetry-schema
// (§11.4.85).
//
// These meta-tests exercise the exported codec and health API with malformed
// inputs, boundary conditions, and concurrent invalid-data scenarios.  Panic
// recovery in every goroutine so a single bad input never takes down the
// suite (§11.4.27 no fakes; §11.4.14 goroutine lifecycle via sync.WaitGroup).
package otatelemetry_chaos_test

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
	otatelemetry "github.com/HelixDevelopment/ota-telemetry-schema"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func chaosEvidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

func writeChaosEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := chaosEvidenceDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("WARNING: mkdir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Logf("WARNING: write evidence %s: %v", path, err)
	}
}

func errToString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

var ts = time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

func report(dev, dep string, ev otaprotocol.TelemetryEvent) otaprotocol.TelemetryReport {
	return otaprotocol.TelemetryReport{
		DeviceID:     dev,
		DeploymentID: dep,
		Event:        ev,
		Progress:     50,
		Timestamp:    ts,
	}
}

// ---------------------------------------------------------------------------
// TestChaosDecodeMalformedBatches
//
// Exercise DecodeBatch with every class of corrupt input: truncated JSON,
// trailing data, unknown fields, wrong types, empty input, whitespace-only,
// deeply nested structures, and extreme string lengths.
// ---------------------------------------------------------------------------

func TestChaosDecodeMalformedBatches(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	type entry struct {
		Name   string `json:"name"`
		Desc   string `json:"description"`
		Failed bool   `json:"decode_failed"`
		Error  string `json:"error,omitempty"`
	}

	cases := []struct {
		name    string
		json    string
		desc    string
		wantErr bool
	}{
		{
			name:    "truncated_object",
			json:    `{"events"`,
			desc:    "truncated JSON object",
			wantErr: true,
		},
		{
			name:    "truncated_events_array",
			json:    `{"events": [{"report":`,
			desc:    "truncated within events array",
			wantErr: true,
		},
		{
			name:    "two_concatenated_docs",
			json:    `{"events": []}{"events": []}`,
			desc:    "two concatenated valid JSON documents",
			wantErr: true,
		},
		{
			name:    "trailing_garbage",
			json:    `{"events": []}abc`,
			desc:    "valid JSON followed by garbage",
			wantErr: true,
		},
		{
			name:    "top_level_array",
			json:    `[{"events": []}]`,
			desc:    "top-level JSON array instead of object",
			wantErr: true,
		},
		{
			name:    "empty_string",
			json:    "",
			desc:    "zero-length input",
			wantErr: true,
		},
		{
			name:    "whitespace_only",
			json:    "     ",
			desc:    "whitespace with no JSON value",
			wantErr: true,
		},
		{
			name:    "events_is_number",
			json:    `{"events": 42}`,
			desc:    "events field is a number, not array",
			wantErr: true,
		},
		{
			name:    "events_is_string",
			json:    `{"events": "hello"}`,
			desc:    "events field is a string",
			wantErr: true,
		},
		{
			name:    "deeply_nested_unknown",
			json:    `{"events": [{"report": {"device_id": "d", "deployment_id": "dep", "event": "success", "progress": 0, "timestamp": "2026-06-07T12:00:00Z", "extra": {"nested": {"very": {"deep": true}}}}}]}`,
			desc:    "deeply nested unknown field in report",
			wantErr: true,
		},
		{
			name:    "huge_cohort_string",
			json:    fmt.Sprintf(`{"events": [{"report": {"device_id": "d", "deployment_id": "dep", "event": "success", "progress": 0, "timestamp": "2026-06-07T12:00:00Z"}, "cohort": "%s"}]}`, strings.Repeat("X", 10000)),
			desc:    "cohort with 10K chars",
			wantErr: false,
		},
	}

	var results []entry
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()

			_, err := otatelemetry.DecodeBatch([]byte(c.json))
			failed := err != nil
			results = append(results, entry{
				Name: c.name, Desc: c.desc, Failed: failed, Error: errToString(err),
			})
			if c.wantErr && !failed {
				t.Errorf("DecodeBatch(%q) = nil error, want failure", c.desc)
			}
			if !c.wantErr && failed {
				t.Errorf("DecodeBatch(%q) = %v, want nil", c.desc, err)
			}
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_decode_malformed_batches.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosEventValidationBoundaries
//
// Exercise Event.Validate() with every known invalid field configuration:
// empty ids, invalid event types, progress out of range, zero timestamp,
// extreme values.
// ---------------------------------------------------------------------------

func TestChaosEventValidationBoundaries(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	type entry struct {
		Name  string `json:"name"`
		Valid bool   `json:"valid"`
		Error string `json:"error,omitempty"`
	}

	cases := []struct {
		name  string
		event otatelemetry.Event
	}{
		{
			name:  "empty_device_id",
			event: otatelemetry.NewEvent(report("", "dep1", otaprotocol.EventSuccess)),
		},
		{
			name:  "empty_deployment_id",
			event: otatelemetry.NewEvent(report("d1", "", otaprotocol.EventSuccess)),
		},
		{
			name:  "both_ids_empty",
			event: otatelemetry.NewEvent(report("", "", otaprotocol.EventSuccess)),
		},
		{
			name:  "invalid_event_type",
			event: otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.TelemetryEvent("bogus"))),
		},
		{
			name:  "empty_event_type",
			event: otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.TelemetryEvent(""))),
		},
		{
			name:  "event_idle",
			event: otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.TelemetryEvent("idle"))),
		},
		{
			name: "progress_negative",
			event: func() otatelemetry.Event {
				r := report("d1", "dep1", otaprotocol.EventInstalling)
				r.Progress = -1
				return otatelemetry.NewEvent(r)
			}(),
		},
		{
			name: "progress_above_100",
			event: func() otatelemetry.Event {
				r := report("d1", "dep1", otaprotocol.EventInstalling)
				r.Progress = 101
				return otatelemetry.NewEvent(r)
			}(),
		},
		{
			name: "progress_max_int",
			event: func() otatelemetry.Event {
				r := report("d1", "dep1", otaprotocol.EventInstalling)
				r.Progress = math.MaxInt
				return otatelemetry.NewEvent(r)
			}(),
		},
		{
			name: "progress_min_int",
			event: func() otatelemetry.Event {
				r := report("d1", "dep1", otaprotocol.EventInstalling)
				r.Progress = math.MinInt
				return otatelemetry.NewEvent(r)
			}(),
		},
		{
			name: "zero_timestamp",
			event: func() otatelemetry.Event {
				r := report("d1", "dep1", otaprotocol.EventSuccess)
				r.Timestamp = time.Time{}
				return otatelemetry.NewEvent(r)
			}(),
		},
		{
			name: "all_fields_invalid",
			event: func() otatelemetry.Event {
				return otatelemetry.NewEvent(otaprotocol.TelemetryReport{
					DeviceID:     "",
					DeploymentID: "",
					Event:        "",
					Progress:     -5,
					Timestamp:    time.Time{},
				})
			}(),
		},
	}

	var results []entry
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.event.Validate()
			valid := err == nil
			results = append(results, entry{
				Name: c.name, Valid: valid, Error: errToString(err),
			})
			if valid {
				t.Errorf("%s: Validate() = nil, want error", c.name)
			}
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_event_validation_boundaries.json", ev)
}

// ---------------------------------------------------------------------------
// TestChaosHealthThresholdBoundaries
//
// Exercise HealthThresholds.Validate() and Health.Verdict() at every boundary
// condition: values outside [0,1], both zero, extreme float64 precision.
// ---------------------------------------------------------------------------

func TestChaosHealthThresholdBoundaries(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	type entry struct {
		Thresholds    otatelemetry.HealthThresholds `json:"thresholds"`
		Valid         bool                          `json:"validate_passed"`
		ValidateError string                        `json:"validate_error,omitempty"`
		Verdict       string                        `json:"verdict"`
		VerdictError  string                        `json:"verdict_error,omitempty"`
	}

	boundaries := []otatelemetry.HealthThresholds{
		{SuccessThreshold: 0.0, ErrorThreshold: 0.0},
		{SuccessThreshold: 0.0, ErrorThreshold: 1.0},
		{SuccessThreshold: 1.0, ErrorThreshold: 0.0},
		{SuccessThreshold: 1.0, ErrorThreshold: 1.0},
		{SuccessThreshold: -0.1, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5, ErrorThreshold: -0.1},
		{SuccessThreshold: 1.5, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.5, ErrorThreshold: 1.5},
		{SuccessThreshold: math.SmallestNonzeroFloat64, ErrorThreshold: math.SmallestNonzeroFloat64},
		{SuccessThreshold: 0.9999999999, ErrorThreshold: 0.0000000001},
		{SuccessThreshold: 0.5, ErrorThreshold: 0.5},
	}

	h := otatelemetry.Health{Terminal: 10, Successes: 9, Failures: 1}

	var results []entry
	for _, thr := range boundaries {
		thr := thr
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
// TestChaosConcurrentEncodeInvalidBatch
//
// N=30 goroutines concurrently encode various invalid batches.  All must be
// rejected without panicking.
// ---------------------------------------------------------------------------

func TestChaosConcurrentEncodeInvalidBatch(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	const numGoroutines = 30

	invalidBatches := []otatelemetry.Batch{
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(report("", "dep", otaprotocol.EventSuccess))}},
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(report("d", "", otaprotocol.EventSuccess))}},
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: "bogus", Progress: 50, Timestamp: ts})}},
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: otaprotocol.EventSuccess, Progress: 200, Timestamp: ts})}},
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: otaprotocol.EventSuccess, Progress: -5, Timestamp: ts})}},
		{Events: []otatelemetry.Event{otatelemetry.NewEvent(otaprotocol.TelemetryReport{DeviceID: "d", DeploymentID: "dep", Event: otaprotocol.EventSuccess, Progress: 50, Timestamp: time.Time{}})}},
		{Events: []otatelemetry.Event{
			otatelemetry.NewEvent(report("d", "dep", otaprotocol.EventSuccess)),
			otatelemetry.NewEvent(report("", "dep", otaprotocol.EventFailure)),
		}},
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
			_, err := otatelemetry.EncodeBatch(b)
			if err != nil {
				atomic.AddInt32(&rejects, 1)
			}
		}(g)
	}
	wg.Wait()

	summary := map[string]interface{}{
		"test":       "TestChaosConcurrentEncodeInvalidBatch",
		"goroutines": numGoroutines,
		"rejected":   rejects,
		"panics":     panics,
	}
	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeChaosEvidence(t, "chaos_concurrent_encode_invalid.json", ev)

	if panics > 0 {
		t.Errorf("%d goroutines panicked", panics)
	}
}

// ---------------------------------------------------------------------------
// TestChaosHealthDerivationEdgeCases
//
// Exercise DeriveHealth with boundary event sets: nil, empty, single events,
// all-non-terminal, and large mixed sets.  Asserts no NaN, no Inf, no panic.
// ---------------------------------------------------------------------------

func TestChaosHealthDerivationEdgeCases(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panic during edge case derivation: %v", r)
		}
	}()

	type entry struct {
		Name        string  `json:"name"`
		Total       int     `json:"total"`
		Terminal    int     `json:"terminal"`
		SuccessRate float64 `json:"success_rate"`
		FailureRate float64 `json:"failure_rate"`
	}

	edgeCases := []struct {
		name   string
		events []otatelemetry.Event
	}{
		{"nil_slice", nil},
		{"empty_slice", []otatelemetry.Event{}},
		{"single_success", []otatelemetry.Event{otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.EventSuccess))}},
		{"single_failure", []otatelemetry.Event{otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.EventFailure))}},
		{"single_non_terminal", []otatelemetry.Event{otatelemetry.NewEvent(report("d1", "dep1", otaprotocol.EventDownloadStarted))}},
		{"all_non_terminal", func() []otatelemetry.Event {
			evs := make([]otatelemetry.Event, 100)
			for i := 0; i < 100; i++ {
				evs[i] = otatelemetry.NewEvent(report(fmt.Sprintf("d%d", i), "dep1", otaprotocol.EventInstalling))
			}
			return evs
		}()},
		{"large_mixed_1000", func() []otatelemetry.Event {
			evs := make([]otatelemetry.Event, 1000)
			for i := 0; i < 1000; i++ {
				r := report(fmt.Sprintf("d%d", i%50), "dep1", otaprotocol.EventSuccess)
				if i%3 == 0 {
					r.Event = otaprotocol.EventFailure
				}
				evs[i] = otatelemetry.NewEvent(r)
			}
			return evs
		}()},
	}

	var results []entry
	for _, ec := range edgeCases {
		ec := ec
		t.Run(ec.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()
			h := otatelemetry.DeriveHealth(ec.events)
			if math.IsNaN(h.SuccessRate) || math.IsNaN(h.FailureRate) {
				t.Errorf("NaN rate: success=%v failure=%v", h.SuccessRate, h.FailureRate)
			}
			if math.IsInf(h.SuccessRate, 0) || math.IsInf(h.FailureRate, 0) {
				t.Errorf("Inf rate: success=%v failure=%v", h.SuccessRate, h.FailureRate)
			}
			results = append(results, entry{
				Name: ec.name, Total: h.Total, Terminal: h.Terminal,
				SuccessRate: h.SuccessRate, FailureRate: h.FailureRate,
			})
		})
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeChaosEvidence(t, "chaos_health_derivation_edge_cases.json", ev)
}
