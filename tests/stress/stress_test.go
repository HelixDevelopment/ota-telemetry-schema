// Package otatelemetry_stress_test — stress tests for ota-telemetry-schema
// (§11.4.85).
//
// These meta-tests exercise the exported codec and health-derivation API under
// sustained concurrent load, recording p50/p95/p99 latency as captured
// evidence.  No mocks, no stubs (§11.4.27).  Every test uses the real
// implementation.
package otatelemetry_stress_test

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
	otatelemetry "github.com/HelixDevelopment/ota-telemetry-schema"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func stressEvidenceDir() string {
	if d := os.Getenv("HELIX_STRESS_EVIDENCE_DIR"); d != "" {
		return d
	}
	return "qa-results/stress_chaos"
}

func writeStressEvidence(t *testing.T, name string, data []byte) {
	t.Helper()
	dir := stressEvidenceDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("WARNING: mkdir %s: %v", dir, err)
		return
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Logf("WARNING: write evidence %s: %v", path, err)
	}
}

func randomReport(rng *rand.Rand, suffix int) otaprotocol.TelemetryReport {
	events := []otaprotocol.TelemetryEvent{
		otaprotocol.EventDownloadStarted, otaprotocol.EventInstalling,
		otaprotocol.EventInstalled, otaprotocol.EventVerifying,
		otaprotocol.EventSuccess, otaprotocol.EventFailure,
	}
	ev := events[rng.Intn(len(events))]
	return otaprotocol.TelemetryReport{
		DeviceID:     fmt.Sprintf("dev-%d", suffix),
		DeploymentID: fmt.Sprintf("dep-%d", suffix%10),
		Event:        ev,
		Progress:     rng.Intn(101),
		Timestamp:    time.Date(2026, 6, 19, 0, 0, rng.Intn(60), 0, time.UTC),
	}
}

// generateEvents builds n events sharing the same deployment.
func generateEvents(rng *rand.Rand, n int, depID string) []otatelemetry.Event {
	events := make([]otatelemetry.Event, n)
	for i := 0; i < n; i++ {
		r := randomReport(rng, i)
		r.DeploymentID = depID
		events[i] = otatelemetry.NewEvent(r)
	}
	return events
}

// ---------------------------------------------------------------------------
// TestStressSustainedEncodeDecode
//
// N=200 iterations of encode-then-decode a batch of 20 events, recording
// round-trip latency.  Verifies event count preservation.
// ---------------------------------------------------------------------------

func TestStressSustainedEncodeDecode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const (
		iterations     = 200
		eventsPerBatch = 20
	)

	rng := rand.New(rand.NewSource(42))
	var latencies []time.Duration
	var errCount int

	for i := 0; i < iterations; i++ {
		events := make([]otatelemetry.Event, eventsPerBatch)
		for j := 0; j < eventsPerBatch; j++ {
			r := randomReport(rng, i*eventsPerBatch+j)
			events[j] = otatelemetry.NewEvent(r)
		}

		start := time.Now()
		data, err := otatelemetry.EncodeBatch(otatelemetry.Batch{Events: events})
		if err != nil {
			errCount++
			continue
		}
		decoded, err := otatelemetry.DecodeBatch(data)
		if err != nil {
			errCount++
			continue
		}
		elapsed := time.Since(start)
		latencies = append(latencies, elapsed)

		if len(decoded.Events) != eventsPerBatch {
			t.Errorf("iteration %d: decoded %d events, want %d", i, len(decoded.Events), eventsPerBatch)
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p50, p95, p99 time.Duration
	if len(latencies) > 0 {
		n := len(latencies)
		p50 = latencies[n*50/100]
		p95 = latencies[n*95/100]
		p99 = latencies[n*99/100]
	}

	summary := map[string]interface{}{
		"test":         "TestStressSustainedEncodeDecode",
		"iterations":   iterations,
		"events_batch": eventsPerBatch,
		"errors":       errCount,
		"p50_latency":  p50.String(),
		"p95_latency":  p95.String(),
		"p99_latency":  p99.String(),
	}
	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_sustained_encode_decode.json", ev)

	if errCount > 0 {
		t.Errorf("%d errors out of %d iterations", errCount, iterations)
	}
}

// ---------------------------------------------------------------------------
// TestStressConcurrentEncode
//
// N=100 goroutines each encode a unique batch of 10 events.  No errors
// expected.  Latency distribution recorded.
// ---------------------------------------------------------------------------

func TestStressConcurrentEncode(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const (
		goroutines = 100
		eventsPer  = 10
	)

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		latencies []time.Duration
		errCount  int32
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&errCount, 1)
				}
			}()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))
			events := make([]otatelemetry.Event, eventsPer)
			for i := 0; i < eventsPer; i++ {
				events[i] = otatelemetry.NewEvent(randomReport(rng, gid*eventsPer+i))
			}

			start := time.Now()
			_, err := otatelemetry.EncodeBatch(otatelemetry.Batch{Events: events})
			elapsed := time.Since(start)

			mu.Lock()
			latencies = append(latencies, elapsed)
			mu.Unlock()

			if err != nil {
				atomic.AddInt32(&errCount, 1)
			}
		}(g)
	}
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	summary := map[string]interface{}{
		"test":       "TestStressConcurrentEncode",
		"goroutines": goroutines,
		"events_per": eventsPer,
		"errors":     errCount,
	}
	if len(latencies) > 0 {
		n := len(latencies)
		summary["p50_latency"] = latencies[n*50/100].String()
		summary["p95_latency"] = latencies[n*95/100].String()
		summary["p99_latency"] = latencies[n*99/100].String()
	}
	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_concurrent_encode.json", ev)

	if errCount > 0 {
		t.Errorf("%d encoding errors", errCount)
	}
}

// ---------------------------------------------------------------------------
// TestStressConcurrentHealthDerivation
//
// N=50 goroutines each derive health from 500 events.  All must produce
// identical results from the same deterministic input.
// ---------------------------------------------------------------------------

func TestStressConcurrentHealthDerivation(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	const (
		goroutines = 50
		eventsPer  = 500
	)

	rng := rand.New(rand.NewSource(99))
	baseEvents := generateEvents(rng, eventsPer, "stress-dep-health")

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []otatelemetry.Health
		panics  int32
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					atomic.AddInt32(&panics, 1)
				}
			}()
			h := otatelemetry.DeriveHealth(baseEvents)
			mu.Lock()
			results = append(results, h)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// All results must be identical.
	for i := 1; i < len(results); i++ {
		if results[i].Total != results[0].Total {
			t.Errorf("result[%d].Total = %d, want %d", i, results[i].Total, results[0].Total)
		}
	}

	summary := map[string]interface{}{
		"test":       "TestStressConcurrentHealthDerivation",
		"goroutines": goroutines,
		"events_per": eventsPer,
		"panics":     panics,
		"results":    len(results),
		"all_identical": len(results) > 0 && func() bool {
			for i := 1; i < len(results); i++ {
				if results[i].Total != results[0].Total {
					return false
				}
			}
			return true
		}(),
	}
	ev, _ := json.MarshalIndent(summary, "", "  ")
	writeStressEvidence(t, "stress_concurrent_health.json", ev)

	if panics > 0 {
		t.Errorf("%d goroutines panicked", panics)
	}
}

// ---------------------------------------------------------------------------
// TestStressMixedHealthVerdict
//
// Generate event sets with controlled success/failure ratios across multiple
// threshold configurations.  Each combination is captured as evidence.
// ---------------------------------------------------------------------------

func TestStressMixedHealthVerdict(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("stress test skipped in short mode")
	}

	thresholds := []otatelemetry.HealthThresholds{
		{SuccessThreshold: 0.9, ErrorThreshold: 0.1},
		{SuccessThreshold: 0.8, ErrorThreshold: 0.2},
		{SuccessThreshold: 0.95, ErrorThreshold: 0.05},
		{SuccessThreshold: 0.7, ErrorThreshold: 0.15},
	}
	failureRatios := []float64{0.0, 0.05, 0.1, 0.15, 0.2, 0.5, 1.0}

	type entry struct {
		FailureRatio float64                       `json:"failure_ratio"`
		Thresholds   otatelemetry.HealthThresholds `json:"thresholds"`
		Verdict      string                        `json:"verdict"`
		Error        string                        `json:"error,omitempty"`
	}

	var results []entry

	for _, fr := range failureRatios {
		n := 1000
		events := make([]otatelemetry.Event, n)
		for i := 0; i < n; i++ {
			r := randomReport(rand.New(rand.NewSource(int64(i))), i)
			if float64(i) < fr*float64(n) {
				r.Event = otaprotocol.EventFailure
			} else {
				r.Event = otaprotocol.EventSuccess
			}
			events[i] = otatelemetry.NewEvent(r)
		}

		h := otatelemetry.DeriveHealth(events)
		for _, thr := range thresholds {
			v, err := h.Verdict(thr)
			e := entry{
				FailureRatio: math.Round(fr*1e6) / 1e6,
				Thresholds:   thr,
				Verdict:      v.String(),
			}
			if err != nil {
				e.Error = err.Error()
			}
			results = append(results, e)
		}
	}

	ev, _ := json.MarshalIndent(results, "", "  ")
	writeStressEvidence(t, "stress_mixed_health_verdict.json", ev)
}
