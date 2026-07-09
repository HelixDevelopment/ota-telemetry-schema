package otatelemetry

import (
	"fmt"
	"math"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// Verdict is the rollout-gate decision derived from a cohort's health against
// configured thresholds. It is what the rollout engine's telemetry-signal port
// consumes (spec §5).
type Verdict string

const (
	// VerdictHalt means the cohort's error rate met or exceeded the error
	// threshold: the rollout must halt/pause (and alert). Per the spec §5 safety
	// invariant, HALT WINS OVER ADVANCE.
	VerdictHalt Verdict = "halt"
	// VerdictAdvance means the cohort met its success threshold and did NOT
	// breach the error threshold: the rollout may advance to the next phase.
	VerdictAdvance Verdict = "advance"
	// VerdictHold means neither threshold decided the phase: success is below
	// the bar but errors are within tolerance — the operator decides
	// (spec §5: "hold/pause").
	VerdictHold Verdict = "hold"
)

// String returns the verdict token.
func (v Verdict) String() string { return string(v) }

// HealthThresholds are the per-phase success/error bars the rollout engine
// configures (spec §5). Both are fractions in [0,1].
type HealthThresholds struct {
	// SuccessThreshold is the minimum success rate (successes / terminal) at or
	// above which a phase may advance.
	SuccessThreshold float64 `json:"success_threshold"`
	// ErrorThreshold is the error rate (failures / terminal) at or above which a
	// phase must halt.
	ErrorThreshold float64 `json:"error_threshold"`
}

// Validate reports whether both thresholds are fractions within [0,1].
//
// NaN is checked explicitly and first: Go float comparisons with NaN (<, >,
// ==) always evaluate false, so a bare `< 0 || > 1` range check silently
// accepts a NaN threshold as "valid" — a real defect class distinct from the
// already-caught ±Inf case (±Inf trips the range check because Inf > 1 and
// -Inf < 0 are true). A NaN threshold is never a valid [0,1] fraction and
// must be rejected here, not allowed to reach Verdict()'s safety-invariant
// comparisons where it would silently fail to halt/advance either way.
func (t HealthThresholds) Validate() error {
	if math.IsNaN(t.SuccessThreshold) {
		return fmt.Errorf("%w: success_threshold is NaN", ErrInvalidThresholds)
	}
	if t.SuccessThreshold < 0 || t.SuccessThreshold > 1 {
		return fmt.Errorf("%w: success_threshold %v out of [0,1]", ErrInvalidThresholds, t.SuccessThreshold)
	}
	if math.IsNaN(t.ErrorThreshold) {
		return fmt.Errorf("%w: error_threshold is NaN", ErrInvalidThresholds)
	}
	if t.ErrorThreshold < 0 || t.ErrorThreshold > 1 {
		return fmt.Errorf("%w: error_threshold %v out of [0,1]", ErrInvalidThresholds, t.ErrorThreshold)
	}
	return nil
}

// Health is the pure, derived health signal for a deployment/cohort, computed
// from a set of events. It is the single source of truth the rollout engine and
// dashboard read (spec §4). All fields are derived; nothing here performs I/O.
type Health struct {
	// DeploymentID is the deployment the events belong to (empty when derived
	// across a mixed set).
	DeploymentID string `json:"deployment_id,omitempty"`
	// Cohort is the phase/cohort the events belong to (empty when not scoped).
	Cohort string `json:"cohort,omitempty"`
	// Total is the number of events considered.
	Total int `json:"total"`
	// Terminal is the number of events in a terminal state (success or failure):
	// the denominator of the rate math.
	Terminal int `json:"terminal"`
	// Successes is the count of terminal success events.
	Successes int `json:"successes"`
	// Failures is the count of terminal failure events.
	Failures int `json:"failures"`
	// CountsByEvent is the count of every canonical event id (all six keys are
	// always present, defaulting to 0).
	CountsByEvent map[otaprotocol.TelemetryEvent]int `json:"counts_by_event"`
	// SuccessRate is successes / terminal (0 when no terminal events).
	SuccessRate float64 `json:"success_rate"`
	// FailureRate is failures / terminal (0 when no terminal events).
	FailureRate float64 `json:"failure_rate"`
}

// DeriveHealth computes the pure health signal from a set of events. It does NOT
// validate the events (callers decode-then-validate via the codec); it only
// aggregates. Rates use the count of terminal events as the denominator, per
// spec §4 ("count of devices in cohort that reached a terminal state"). With no
// terminal events both rates are 0.
//
// DeploymentID/Cohort on the result are set only when all considered events
// agree on a single non-empty value, so a mixed set yields an unscoped Health
// rather than a misleading label.
func DeriveHealth(events []Event) Health {
	h := Health{CountsByEvent: newEventCounts()}

	depUniform, cohortUniform := true, true
	var depSeen, cohortSeen string
	depSet, cohortSet := false, false

	for _, e := range events {
		h.Total++
		h.CountsByEvent[e.Report.Event]++

		switch {
		case e.IsSuccess():
			h.Successes++
			h.Terminal++
		case e.IsFailure():
			h.Failures++
			h.Terminal++
		}

		if d := e.Report.DeploymentID; d != "" {
			if !depSet {
				depSeen, depSet = d, true
			} else if depSeen != d {
				depUniform = false
			}
		}
		if c := e.Cohort; c != "" {
			if !cohortSet {
				cohortSeen, cohortSet = c, true
			} else if cohortSeen != c {
				cohortUniform = false
			}
		}
	}

	if depSet && depUniform {
		h.DeploymentID = depSeen
	}
	if cohortSet && cohortUniform {
		h.Cohort = cohortSeen
	}

	if h.Terminal > 0 {
		h.SuccessRate = float64(h.Successes) / float64(h.Terminal)
		h.FailureRate = float64(h.Failures) / float64(h.Terminal)
	}
	return h
}

// Verdict derives the rollout-gate decision from this health against the given
// thresholds. It enforces the spec §5 SAFETY INVARIANT — halt wins over advance:
//
//  1. If failure_rate >= error_threshold -> HALT (even if success would also
//     advance in the same window).
//  2. Else if success_rate >= success_threshold -> ADVANCE.
//  3. Else -> HOLD (operator decision).
//
// With no terminal events both rates are 0, so the result is ADVANCE only if the
// success threshold is 0; otherwise HOLD. An invalid thresholds value returns
// ErrInvalidThresholds (defaulting to HOLD, the safe non-advancing decision).
func (h Health) Verdict(t HealthThresholds) (Verdict, error) {
	if err := t.Validate(); err != nil {
		return VerdictHold, err
	}
	// Safety invariant: halt is evaluated first and wins.
	if h.FailureRate >= t.ErrorThreshold && t.ErrorThreshold > 0 {
		return VerdictHalt, nil
	}
	// ErrorThreshold == 0 means "any failure halts".
	if t.ErrorThreshold == 0 && h.Failures > 0 {
		return VerdictHalt, nil
	}
	if h.SuccessRate >= t.SuccessThreshold {
		return VerdictAdvance, nil
	}
	return VerdictHold, nil
}

// newEventCounts returns a counts map pre-populated with every canonical event
// id at 0, so callers can read any key without an existence check.
func newEventCounts() map[otaprotocol.TelemetryEvent]int {
	m := make(map[otaprotocol.TelemetryEvent]int, len(AllEventTypes()))
	for _, ev := range AllEventTypes() {
		m[ev] = 0
	}
	return m
}
