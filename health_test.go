package otatelemetry

import (
	"errors"
	"math"
	"testing"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

func ev(dep, cohort string, e otaprotocol.TelemetryEvent) Event {
	return Event{Report: report("d", dep, e), Cohort: cohort}
}

const eps = 1e-9

func TestDeriveHealthCountsAndRates(t *testing.T) {
	events := []Event{
		ev("dep1", "canary", EventDownloadStarted),
		ev("dep1", "canary", EventInstalling),
		ev("dep1", "canary", EventSuccess),
		ev("dep1", "canary", EventSuccess),
		ev("dep1", "canary", EventSuccess),
		ev("dep1", "canary", EventFailure),
	}
	h := DeriveHealth(events)

	if h.Total != 6 {
		t.Errorf("Total = %d, want 6", h.Total)
	}
	if h.Terminal != 4 {
		t.Errorf("Terminal = %d, want 4", h.Terminal)
	}
	if h.Successes != 3 {
		t.Errorf("Successes = %d, want 3", h.Successes)
	}
	if h.Failures != 1 {
		t.Errorf("Failures = %d, want 1", h.Failures)
	}
	if math.Abs(h.SuccessRate-0.75) > eps {
		t.Errorf("SuccessRate = %v, want 0.75", h.SuccessRate)
	}
	if math.Abs(h.FailureRate-0.25) > eps {
		t.Errorf("FailureRate = %v, want 0.25", h.FailureRate)
	}
	if h.DeploymentID != "dep1" {
		t.Errorf("DeploymentID = %q, want dep1", h.DeploymentID)
	}
	if h.Cohort != "canary" {
		t.Errorf("Cohort = %q, want canary", h.Cohort)
	}

	// All six event ids must be present in counts.
	for _, e := range AllEventTypes() {
		if _, ok := h.CountsByEvent[e]; !ok {
			t.Errorf("CountsByEvent missing key %q", e)
		}
	}
	if h.CountsByEvent[EventSuccess] != 3 {
		t.Errorf("count[success] = %d, want 3", h.CountsByEvent[EventSuccess])
	}
	if h.CountsByEvent[EventDownloadStarted] != 1 {
		t.Errorf("count[download_started] = %d, want 1", h.CountsByEvent[EventDownloadStarted])
	}
	if h.CountsByEvent[EventVerifying] != 0 {
		t.Errorf("count[verifying] = %d, want 0", h.CountsByEvent[EventVerifying])
	}
}

func TestDeriveHealthNoTerminalEvents(t *testing.T) {
	h := DeriveHealth([]Event{
		ev("dep1", "c", EventDownloadStarted),
		ev("dep1", "c", EventInstalling),
	})
	if h.Terminal != 0 {
		t.Errorf("Terminal = %d, want 0", h.Terminal)
	}
	if h.SuccessRate != 0 || h.FailureRate != 0 {
		t.Errorf("rates = (%v,%v), want (0,0)", h.SuccessRate, h.FailureRate)
	}
}

func TestDeriveHealthEmpty(t *testing.T) {
	h := DeriveHealth(nil)
	if h.Total != 0 || h.Terminal != 0 {
		t.Errorf("empty health = %+v", h)
	}
	if len(h.CountsByEvent) != len(AllEventTypes()) {
		t.Errorf("CountsByEvent has %d keys, want %d", len(h.CountsByEvent), len(AllEventTypes()))
	}
}

func TestDeriveHealthMixedScopeUnlabeled(t *testing.T) {
	h := DeriveHealth([]Event{
		ev("dep1", "canary", EventSuccess),
		ev("dep2", "beta", EventSuccess),
	})
	if h.DeploymentID != "" {
		t.Errorf("mixed DeploymentID = %q, want empty", h.DeploymentID)
	}
	if h.Cohort != "" {
		t.Errorf("mixed Cohort = %q, want empty", h.Cohort)
	}
}

func TestHealthThresholdsValidate(t *testing.T) {
	tests := []struct {
		name    string
		t       HealthThresholds
		wantErr bool
	}{
		{"valid", HealthThresholds{0.9, 0.1}, false},
		{"bounds 0 and 1", HealthThresholds{1, 0}, false},
		{"success below 0", HealthThresholds{-0.1, 0.1}, true},
		{"success above 1", HealthThresholds{1.1, 0.1}, true},
		{"error below 0", HealthThresholds{0.9, -0.1}, true},
		{"error above 1", HealthThresholds{0.9, 1.1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.t.Validate()
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidThresholds) {
					t.Errorf("Validate() = %v, want ErrInvalidThresholds", err)
				}
				return
			}
			if err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

// TestHealthThresholdsValidateRejectsNaN reproduces a real defect: NaN
// comparisons in Go always evaluate false (NaN < 0, NaN > 1, NaN == 0 are all
// false), so HealthThresholds.Validate()'s naive `< 0 || > 1` range checks
// silently accept a NaN threshold as "valid" instead of rejecting it as
// malformed. A NaN fraction is never a valid [0,1] value and must be caught
// here — this is the module's schema-validation boundary for thresholds.
func TestHealthThresholdsValidateRejectsNaN(t *testing.T) {
	tests := []struct {
		name string
		t    HealthThresholds
	}{
		{"NaN success_threshold", HealthThresholds{SuccessThreshold: math.NaN(), ErrorThreshold: 0.1}},
		{"NaN error_threshold", HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: math.NaN()}},
		{"NaN both", HealthThresholds{SuccessThreshold: math.NaN(), ErrorThreshold: math.NaN()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.t.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil for NaN threshold %+v, want ErrInvalidThresholds (NaN is not a valid [0,1] fraction and must not silently pass)", tc.t)
			}
			if !errors.Is(err, ErrInvalidThresholds) {
				t.Errorf("error %v not matched by ErrInvalidThresholds", err)
			}
		})
	}
}

// TestVerdictSafetyUnderNaNErrorThreshold proves the safety-invariant
// consequence of the NaN-acceptance defect: with a NaN error_threshold that
// Validate() wrongly let through, Verdict() must still refuse to declare a
// non-halt outcome for a catastrophic (100%) failure rate. Root-cause fix is
// in HealthThresholds.Validate(): once it rejects NaN, Verdict()'s existing
// "return VerdictHold, err on invalid thresholds" path is exercised and the
// safety invariant is preserved (never silently ADVANCE/hold-without-error
// through a malformed threshold on a catastrophic failure rate).
func TestVerdictSafetyUnderNaNErrorThreshold(t *testing.T) {
	h := Health{Terminal: 10, Successes: 0, Failures: 10, SuccessRate: 0, FailureRate: 1.0}
	thr := HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: math.NaN()}

	v, err := h.Verdict(thr)
	if err == nil {
		t.Fatalf("Verdict() with NaN error_threshold at 100%% failure rate returned nil error (verdict=%q) — a malformed threshold must be rejected, not silently evaluated", v)
	}
	if !errors.Is(err, ErrInvalidThresholds) {
		t.Errorf("error %v not matched by ErrInvalidThresholds", err)
	}
	if v != VerdictHold {
		t.Errorf("Verdict() = %q on invalid-thresholds error path, want VerdictHold (the documented safe default)", v)
	}
}

func TestHealthVerdict(t *testing.T) {
	tests := []struct {
		name    string
		h       Health
		thr     HealthThresholds
		want    Verdict
		wantErr bool
	}{
		{
			name: "advance: high success, low error",
			h:    Health{Terminal: 10, Successes: 10, SuccessRate: 1.0, FailureRate: 0.0},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0.1},
			want: VerdictAdvance,
		},
		{
			name: "halt: error rate at threshold",
			h:    Health{Terminal: 10, Successes: 8, Failures: 2, SuccessRate: 0.8, FailureRate: 0.2},
			thr:  HealthThresholds{SuccessThreshold: 0.7, ErrorThreshold: 0.2},
			want: VerdictHalt,
		},
		{
			name: "safety invariant: halt wins over advance in same window",
			// Both success>=0.9 AND error>=0.1 would trigger; halt must win.
			h:    Health{Terminal: 10, Successes: 9, Failures: 1, SuccessRate: 0.9, FailureRate: 0.1},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0.1},
			want: VerdictHalt,
		},
		{
			name: "hold: success below bar, error within tolerance",
			h:    Health{Terminal: 10, Successes: 5, Failures: 1, SuccessRate: 0.5, FailureRate: 0.1},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0.2},
			want: VerdictHold,
		},
		{
			name: "zero error threshold: any failure halts",
			h:    Health{Terminal: 10, Successes: 9, Failures: 1, SuccessRate: 0.9, FailureRate: 0.1},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0},
			want: VerdictHalt,
		},
		{
			name: "zero error threshold: no failures advances",
			h:    Health{Terminal: 10, Successes: 10, SuccessRate: 1.0, FailureRate: 0},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0},
			want: VerdictAdvance,
		},
		{
			name: "no terminal events, zero success threshold advances",
			h:    Health{Terminal: 0, SuccessRate: 0, FailureRate: 0},
			thr:  HealthThresholds{SuccessThreshold: 0, ErrorThreshold: 0.1},
			want: VerdictAdvance,
		},
		{
			name: "no terminal events, positive success threshold holds",
			h:    Health{Terminal: 0, SuccessRate: 0, FailureRate: 0},
			thr:  HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0.1},
			want: VerdictHold,
		},
		{
			name:    "invalid thresholds returns error and safe hold",
			h:       Health{Terminal: 10, Successes: 10, SuccessRate: 1.0},
			thr:     HealthThresholds{SuccessThreshold: 2.0, ErrorThreshold: 0.1},
			want:    VerdictHold,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.h.Verdict(tc.thr)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidThresholds) {
					t.Errorf("Verdict() err = %v, want ErrInvalidThresholds", err)
				}
			} else if err != nil {
				t.Errorf("Verdict() unexpected err = %v", err)
			}
			if got != tc.want {
				t.Errorf("Verdict() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestVerdictString(t *testing.T) {
	cases := map[Verdict]string{
		VerdictHalt:    "halt",
		VerdictAdvance: "advance",
		VerdictHold:    "hold",
	}
	for v, want := range cases {
		if v.String() != want {
			t.Errorf("%v.String() = %q, want %q", v, v.String(), want)
		}
	}
}

// TestDeriveThenVerdictIntegration feeds a synthetic event stream through the
// full derive->verdict path (spec §10 layer 3), crossing an error threshold and
// asserting a halt signal is produced.
func TestDeriveThenVerdictIntegration(t *testing.T) {
	events := []Event{
		ev("dep1", "canary", EventDownloadStarted),
		ev("dep1", "canary", EventInstalling),
		ev("dep1", "canary", EventVerifying),
		ev("dep1", "canary", EventSuccess),
		ev("dep1", "canary", EventFailure),
		ev("dep1", "canary", EventFailure),
	}
	h := DeriveHealth(events)
	// 1 success, 2 failures over 3 terminal -> failure rate 0.667.
	v, err := h.Verdict(HealthThresholds{SuccessThreshold: 0.9, ErrorThreshold: 0.3})
	if err != nil {
		t.Fatalf("Verdict() err = %v", err)
	}
	if v != VerdictHalt {
		t.Fatalf("Verdict() = %q, want halt", v)
	}
}
