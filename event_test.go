package otatelemetry

import (
	"errors"
	"testing"
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// ts is a fixed, non-zero timestamp used across tests for determinism (no real
// clock — this module is pure).
var ts = time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)

// report builds a valid TelemetryReport with the given event.
func report(dev, dep string, ev otaprotocol.TelemetryEvent) otaprotocol.TelemetryReport {
	return otaprotocol.TelemetryReport{
		DeviceID:     dev,
		DeploymentID: dep,
		Event:        ev,
		Progress:     50,
		Timestamp:    ts,
	}
}

func TestEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   Event
		wantErr error // a sentinel that errors.Is must match, or nil
	}{
		{
			name:  "valid",
			event: NewEvent(report("d1", "dep1", EventSuccess)),
		},
		{
			name:    "missing device id",
			event:   NewEvent(report("", "dep1", EventSuccess)),
			wantErr: otaprotocol.ErrMissingField,
		},
		{
			name:    "missing deployment id",
			event:   NewEvent(report("d1", "", EventSuccess)),
			wantErr: otaprotocol.ErrMissingField,
		},
		{
			name:    "invalid event token",
			event:   NewEvent(report("d1", "dep1", otaprotocol.TelemetryEvent("idle"))),
			wantErr: otaprotocol.ErrInvalidEnum,
		},
		{
			name:    "empty event token",
			event:   NewEvent(report("d1", "dep1", otaprotocol.TelemetryEvent(""))),
			wantErr: otaprotocol.ErrInvalidEnum,
		},
		{
			name: "progress out of range",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Progress = 101
				return NewEvent(r)
			}(),
			wantErr: otaprotocol.ErrInvalidValue,
		},
		{
			name: "zero timestamp",
			event: func() Event {
				r := report("d1", "dep1", EventInstalling)
				r.Timestamp = time.Time{}
				return NewEvent(r)
			}(),
			wantErr: otaprotocol.ErrMissingField,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error %v", tc.wantErr)
			}
			// Both the module sentinel and the underlying ota-protocol cause must
			// be matchable.
			if !errors.Is(err, ErrInvalidEvent) {
				t.Errorf("error %v not matched by ErrInvalidEvent", err)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error %v not matched by %v", err, tc.wantErr)
			}
		})
	}
}

func TestEventAccessors(t *testing.T) {
	e := Event{
		Report:       report("dev-9", "dep-9", EventVerifying),
		Cohort:       "canary",
		SystemHealth: "ok",
	}
	if got := e.DeviceID(); got != "dev-9" {
		t.Errorf("DeviceID() = %q", got)
	}
	if got := e.DeploymentID(); got != "dep-9" {
		t.Errorf("DeploymentID() = %q", got)
	}
	if got := e.EventType(); got != EventVerifying {
		t.Errorf("EventType() = %q", got)
	}
	if !e.Timestamp().Equal(ts) {
		t.Errorf("Timestamp() = %v, want %v", e.Timestamp(), ts)
	}
}

func TestEventTerminalClassification(t *testing.T) {
	tests := []struct {
		ev                         otaprotocol.TelemetryEvent
		terminal, success, failure bool
	}{
		{EventDownloadStarted, false, false, false},
		{EventInstalling, false, false, false},
		{EventInstalled, false, false, false},
		{EventVerifying, false, false, false},
		{EventSuccess, true, true, false},
		{EventFailure, true, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.ev.String(), func(t *testing.T) {
			e := NewEvent(report("d", "dep", tc.ev))
			if e.IsTerminal() != tc.terminal {
				t.Errorf("IsTerminal() = %v, want %v", e.IsTerminal(), tc.terminal)
			}
			if e.IsSuccess() != tc.success {
				t.Errorf("IsSuccess() = %v, want %v", e.IsSuccess(), tc.success)
			}
			if e.IsFailure() != tc.failure {
				t.Errorf("IsFailure() = %v, want %v", e.IsFailure(), tc.failure)
			}
		})
	}
}

func TestAllEventTypes(t *testing.T) {
	got := AllEventTypes()
	want := []otaprotocol.TelemetryEvent{
		EventDownloadStarted, EventInstalling, EventInstalled,
		EventVerifying, EventSuccess, EventFailure,
	}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d = %q, want %q", i, got[i], want[i])
		}
		if !got[i].Valid() {
			t.Errorf("event %q reported invalid by ota-protocol", got[i])
		}
	}
}
