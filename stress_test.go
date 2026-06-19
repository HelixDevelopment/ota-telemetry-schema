// Package otatelemetry — stress tests (§11.4.85).
//
// These are meta-tests — they exercise the production code through concurrent
// and bulk-volume patterns. They write captured evidence to
// qa-results/stress_chaos/ (or HELIX_STRESS_EVIDENCE_DIR) per §11.4.5.
// No I/O boundary is crossed by the production code itself (§11.4.28);
// only the test harness writes evidence files.
//
// §11.4.27 (no fakes): every test exercises the real implementation — no mocks,
// no stubs, no placeholders.
// §11.4.50: all N goroutines produce identical exit + evidence.
// §11.4.14: goroutine lifecycle is managed via sync.WaitGroup.
package otatelemetry

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// stressEvidenceDir returns the evidence output directory.
func stressEvidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

// writeStressEvidence writes captured evidence to a file under the evidence
// dir. Errors writing evidence are non-fatal (the test result is the primary
// signal), but are surfaced via t.Log so the operator can investigate.
func writeStressEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := stressEvidenceDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("WARNING: cannot create evidence dir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Logf("WARNING: cannot write evidence %s: %v", path, err)
	}
}

// randomReport constructs a valid TelemetryReport with pseudo-random values
// seeded from a deterministic per-goroutine source.
func randomReport(rng *rand.Rand, suffix int) otaprotocol.TelemetryReport {
	events := AllEventTypes()
	ev := events[rng.Intn(len(events))]
	return otaprotocol.TelemetryReport{
		DeviceID:     fmt.Sprintf("dev-%d", suffix),
		DeploymentID: fmt.Sprintf("dep-%d", suffix%5),
		Event:        ev,
		Progress:     rng.Intn(101),
		Timestamp:    time.Date(2026, 6, 19, 0, 0, rng.Intn(60), 0, time.UTC),
	}
}

// generateEvents builds n events sharing the same deployment and cohort.
func generateEvents(rng *rand.Rand, n int, depID, cohort string) []Event {
	events := make([]Event, n)
	types := AllEventTypes()
	for i := 0; i < n; i++ {
		ev := types[rng.Intn(len(types))]
		r := otaprotocol.TelemetryReport{
			DeviceID:     fmt.Sprintf("dev-%d", rng.Intn(100)),
			DeploymentID: depID,
			Event:        ev,
			Progress:     rng.Intn(101),
			Timestamp:    time.Date(2026, 6, 19, 0, 0, rng.Intn(60), 0, time.UTC),
		}
		events[i] = Event{Report: r, Cohort: cohort}
	}
	return events
}

// repeatEvent creates n events all carrying the same report.
func repeatEvent(r otaprotocol.TelemetryReport, n int) []Event {
	events := make([]Event, n)
	for i := 0; i < n; i++ {
		events[i] = Event{Report: r}
	}
	return events
}

// ---------------------------------------------------------------------------
// TestStressConcurrentEncodeDecode
//
// N=100 goroutines each create valid telemetry reports, wrap them in events,
// encode to a batch, decode back, and assert round-trip equality.  Latency
// distribution (p50/p95/p99) is recorded as captured evidence.
// ---------------------------------------------------------------------------

func TestStressConcurrentEncodeDecode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	const (
		numGoroutines  = 100
		eventsPerBatch = 50
	)

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		latencies  []time.Duration
		errCount   int32
		iterations int32
	)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&errCount, 1)
				}
			}()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))

			start := time.Now()

			// Build batch.
			events := make([]Event, eventsPerBatch)
			for i := 0; i < eventsPerBatch; i++ {
				r := randomReport(rng, gid*eventsPerBatch+i)
				events[i] = NewEvent(r)
			}
			batch := Batch{Events: events}

			// Encode.
			data, err := EncodeBatch(batch)
			if err != nil {
				atomic.AddInt32(&errCount, 1)
				return
			}

			// Decode.
			decoded, err := DecodeBatch(data)
			if err != nil {
				atomic.AddInt32(&errCount, 1)
				return
			}

			// Round-trip assertion: the report (canonical wire fields) must be
			// byte-identical; cohort / system_health / error_message may be empty
			// but must still round-trip.
			if len(decoded.Events) != len(batch.Events) {
				atomic.AddInt32(&errCount, 1)
				return
			}
			for i := range batch.Events {
				if decoded.Events[i].Report != batch.Events[i].Report {
					atomic.AddInt32(&errCount, 1)
					return
				}
			}

			elapsed := time.Since(start)

			mu.Lock()
			latencies = append(latencies, elapsed)
			iterations++
			mu.Unlock()
		}(g)
	}

	wg.Wait()

	// Compute latency distribution.
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var total time.Duration
	for _, d := range latencies {
		total += d
	}

	summary := map[string]interface{}{
		"test":             "TestStressConcurrentEncodeDecode",
		"goroutines":       numGoroutines,
		"events_per_batch": eventsPerBatch,
		"successful":       iterations,
		"errors":           errCount,
		"mean_latency":     fmt.Sprintf("%v", total/time.Duration(max(1, len(latencies)))),
	}
	if len(latencies) > 0 {
		summary["p50_latency"] = fmt.Sprintf("%v", latencies[len(latencies)*50/100])
		summary["p95_latency"] = fmt.Sprintf("%v", latencies[len(latencies)*95/100])
		summary["p99_latency"] = fmt.Sprintf("%v", latencies[len(latencies)*99/100])
		summary["min_latency"] = fmt.Sprintf("%v", latencies[0])
		summary["max_latency"] = fmt.Sprintf("%v", latencies[len(latencies)-1])
	}

	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_concurrent_encode_decode.json", ev)

	t.Logf("encode/decode stress: %d goroutines, %d events each, %d errors, p50=%v p95=%v p99=%v",
		numGoroutines, eventsPerBatch, errCount,
		summary["p50_latency"], summary["p95_latency"], summary["p99_latency"])

	if errCount > 0 {
		t.Errorf("%d goroutines encountered errors", errCount)
	}
	if iterations == 0 {
		t.Fatal("no successful iterations recorded")
	}
}

// ---------------------------------------------------------------------------
// TestStressConcurrentHealthDerivation
//
// N=50 goroutines each derive Health from 1000 events.  All outputs must be
// identical (same deterministic input) — concurrency must not corrupt the
// pure DeriveHealth function.
// ---------------------------------------------------------------------------

func TestStressConcurrentHealthDerivation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	const (
		numGoroutines = 50
		eventsPer     = 1000
	)

	// Pre-generate a deterministic event set — all goroutines derive from the
	// same input slice (read-only), which exercises concurrent reads on the
	// same backing array.
	baseEvents := generateEvents(rand.New(rand.NewSource(42)), eventsPer, "stress-dep", "stress-cohort")

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []Health
		panics  int32
	)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()

			h := DeriveHealth(baseEvents)

			mu.Lock()
			results = append(results, h)
			mu.Unlock()
		}()
	}

	wg.Wait()

	// All results must be identical (same input slice, pure function).
	for i := 1; i < len(results); i++ {
		if results[i].Total != results[0].Total {
			t.Errorf("result[%d].Total = %d, want %d", i, results[i].Total, results[0].Total)
		}
		if results[i].Terminal != results[0].Terminal {
			t.Errorf("result[%d].Terminal = %d, want %d", i, results[i].Terminal, results[0].Terminal)
		}
		if results[i].Successes != results[0].Successes {
			t.Errorf("result[%d].Successes = %d, want %d", i, results[i].Successes, results[0].Successes)
		}
		if results[i].Failures != results[0].Failures {
			t.Errorf("result[%d].Failures = %d, want %d", i, results[i].Failures, results[0].Failures)
		}
	}

	summary := map[string]interface{}{
		"test":            "TestStressConcurrentHealthDerivation",
		"goroutines":      numGoroutines,
		"events_per_call": eventsPer,
		"panics":          panics,
		"result_count":    len(results),
		"all_identical": func() bool {
			for i := 1; i < len(results); i++ {
				if results[i].Total != results[0].Total {
					return false
				}
			}
			return true
		}(),
	}
	if len(results) > 0 {
		summary["total"] = results[0].Total
		summary["terminal"] = results[0].Terminal
		summary["successes"] = results[0].Successes
		summary["failures"] = results[0].Failures
		summary["success_rate"] = results[0].SuccessRate
		summary["failure_rate"] = results[0].FailureRate
	}

	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_concurrent_health_derivation.json", ev)

	if panics > 0 {
		t.Errorf("%d goroutines panicked during concurrent DeriveHealth", panics)
	}
	if len(results) != numGoroutines {
		t.Errorf("got %d results, want %d", len(results), numGoroutines)
	}
}

// ---------------------------------------------------------------------------
// TestStressConcurrentBatchValidation
//
// N=100 goroutines concurrently call Validate() on the same Batch object.
// The batch's events slice is read-only — concurrent Validate must never
// race, deadlock, or produce incorrect results.
// ---------------------------------------------------------------------------

func TestStressConcurrentBatchValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	const (
		numGoroutines = 100
		eventsPer     = 100
	)

	rng := rand.New(rand.NewSource(99))
	batch := Batch{Events: generateEvents(rng, eventsPer, "stress-dep", "stress-cohort")}

	var (
		wg       sync.WaitGroup
		failures int32
		panics   int32
	)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()

			if err := batch.Validate(); err != nil {
				atomic.AddInt32(&failures, 1)
			}
		}()
	}

	wg.Wait()

	summary := map[string]interface{}{
		"test":                "TestStressConcurrentBatchValidation",
		"goroutines":          numGoroutines,
		"events_in_batch":     eventsPer,
		"validation_failures": failures,
		"panics":              panics,
		"all_passed":          failures == 0 && panics == 0,
	}

	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_concurrent_batch_validation.json", ev)

	if panics > 0 {
		t.Errorf("%d goroutines panicked", panics)
	}
	if failures > 0 {
		t.Errorf("%d out of %d concurrent Validate() calls reported failure on a valid batch",
			failures, numGoroutines)
	}
}

// ---------------------------------------------------------------------------
// TestStressMixedHealthVerdict
//
// Generate event sets with controlled success/failure ratios and compute
// Verdict() against various thresholds.  Each combination is deterministic
// and captured as evidence.
// ---------------------------------------------------------------------------

func TestStressMixedHealthVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Parallel()

	thresholds := []HealthThresholds{
		{SuccessThreshold: 0.9, ErrorThreshold: 0.1},
		{SuccessThreshold: 0.8, ErrorThreshold: 0.2},
		{SuccessThreshold: 0.95, ErrorThreshold: 0.05},
		{SuccessThreshold: 0.5, ErrorThreshold: 0.5},
		{SuccessThreshold: 0.0, ErrorThreshold: 1.0},
		{SuccessThreshold: 1.0, ErrorThreshold: 0.0},
		{SuccessThreshold: 0.7, ErrorThreshold: 0.15},
	}

	// failureRatios covers every meaningful regime: zero errors, below error
	// threshold, at error threshold, above error threshold, all errors.
	failureRatios := []float64{0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.5, 0.75, 1.0}

	type entry struct {
		FailureRatio float64          `json:"failure_ratio"`
		Thresholds   HealthThresholds `json:"thresholds"`
		Health       Health           `json:"health"`
		Verdict      string           `json:"verdict"`
		Error        string           `json:"error,omitempty"`
	}

	var results []entry

	for _, fr := range failureRatios {
		// Build n=1000 events with the target failure ratio.
		n := 1000
		events := make([]Event, n)
		successes := 0
		failures := 0
		for i := 0; i < n; i++ {
			if float64(i) < fr*float64(n) {
				events[i] = Event{Report: report("d", "dep-mixed", EventFailure)}
				failures++
			} else {
				events[i] = Event{Report: report("d", "dep-mixed", EventSuccess)}
				successes++
			}
		}

		h := DeriveHealth(events)
		if h.Successes != successes || h.Failures != failures {
			t.Errorf("fr=%v: expected %d successes, %d failures; got %d, %d",
				fr, successes, failures, h.Successes, h.Failures)
		}

		for _, thr := range thresholds {
			v, err := h.Verdict(thr)
			e := entry{
				FailureRatio: roundTo6(fr),
				Thresholds:   thr,
				Health:       h,
				Verdict:      v.String(),
			}
			if err != nil {
				e.Error = err.Error()
			}
			results = append(results, e)
		}
	}

	// Verify safety invariant: when failure_rate >= error_threshold (>0),
	// every verdict must be HALT regardless of success rate.
	for _, r := range results {
		if r.Error != "" {
			continue
		}
		thr := r.Thresholds
		if thr.ErrorThreshold > 0 && r.Health.FailureRate >= thr.ErrorThreshold {
			if r.Verdict != VerdictHalt.String() {
				t.Errorf("safety invariant violation: failure_rate=%v >= error_threshold=%v but verdict=%q",
					r.Health.FailureRate, thr.ErrorThreshold, r.Verdict)
			}
		}
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeStressEvidence(t, "stress_mixed_health_verdict.json", ev)

	t.Logf("mixed health verdict stress: %d combinations across %d failure ratios and %d thresholds",
		len(results), len(failureRatios), len(thresholds))
}

// roundTo6 rounds f to 6 decimal places for stable JSON output.
func roundTo6(f float64) float64 {
	return math.Round(f*1e6) / 1e6
}

// max returns the larger of two ints (built-in in Go 1.26+, kept for clarity).
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
