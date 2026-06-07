// Package otatelemetry defines the telemetry event envelope, metric types, and
// codecs shared by the Helix OTA control plane (server) and the device agents,
// plus the PURE health-signal derivations the rollout engine consumes.
//
// Boundary (HelixConstitution §11.4.28, decoupling): this module is schema +
// (de)serialization + pure derivation ONLY. It performs NO I/O — no network, no
// disk, no HTTP, no database. Inputs arrive as []byte or io.Reader and results
// leave as []byte or io.Writer; everything else is pure functions over values.
//
// It reuses the canonical wire contracts from ota-protocol — the six-value
// TelemetryEvent enum and the TelemetryReport shape (device_id, deployment_id,
// event, progress, error_code, timestamp) — rather than re-declaring ids, so
// server and agents share one source of truth.
package otatelemetry

import (
	"errors"
	"time"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

// Re-exported telemetry event ids from ota-protocol so callers of this module
// do not need a second import to name the lifecycle events. These are aliases,
// not copies: the underlying type and values are ota-protocol's.
const (
	EventDownloadStarted = otaprotocol.EventDownloadStarted
	EventInstalling      = otaprotocol.EventInstalling
	EventInstalled       = otaprotocol.EventInstalled
	EventVerifying       = otaprotocol.EventVerifying
	EventSuccess         = otaprotocol.EventSuccess
	EventFailure         = otaprotocol.EventFailure
)

// Event is the telemetry envelope shared by server and agents. It composes the
// canonical ota-protocol.TelemetryReport (the locked wire fields) with optional
// shared, transport-free annotations that the health model and dashboard use
// but that are not part of the minimal device contract.
//
// The embedded Report carries the authoritative ids (DeviceID, DeploymentID,
// Event, Progress, ErrorCode, Timestamp). The envelope adds:
//   - Cohort: the rollout phase/cohort the device belongs to, so health can be
//     derived per cohort as well as per deployment (spec §4: "per phase/cohort").
//   - SystemHealth: the device-reported system_health field (spec §4) used for
//     problem detection/reporting.
//   - ErrorMessage: a human-readable companion to Report.ErrorCode.
//
// The envelope intentionally does not add any transport or storage concerns.
type Event struct {
	Report       otaprotocol.TelemetryReport `json:"report"`
	Cohort       string                      `json:"cohort,omitempty"`
	SystemHealth string                      `json:"system_health,omitempty"`
	ErrorMessage string                      `json:"error_message,omitempty"`
}

// Sentinel errors. Codec and validation failures wrap one of these so callers
// can branch with errors.Is. ErrInvalidEvent wraps the ota-protocol validation
// errors for malformed reports.
var (
	// ErrInvalidEvent indicates an event whose embedded report failed
	// validation (missing ids, invalid event token, out-of-range progress, or a
	// zero timestamp). The wrapped cause is the ota-protocol sentinel.
	ErrInvalidEvent = errors.New("otatelemetry: invalid event")
	// ErrDecode indicates the input bytes/stream were not the expected JSON
	// shape.
	ErrDecode = errors.New("otatelemetry: decode failed")
	// ErrEncode indicates an event could not be encoded (e.g. an invalid enum
	// that ota-protocol refuses to marshal).
	ErrEncode = errors.New("otatelemetry: encode failed")
	// ErrInvalidThresholds indicates a HealthThresholds value outside [0,1] or
	// otherwise malformed.
	ErrInvalidThresholds = errors.New("otatelemetry: invalid thresholds")
)

// NewEvent constructs an envelope from the canonical wire report.
func NewEvent(r otaprotocol.TelemetryReport) Event {
	return Event{Report: r}
}

// Validate validates the envelope. The embedded report must satisfy the
// canonical ota-protocol contract; on failure the underlying sentinel is
// preserved (so errors.Is against otaprotocol.ErrMissingField etc. still works)
// and ErrInvalidEvent is joined for module-level matching.
func (e Event) Validate() error {
	if err := otaprotocol.ValidateTelemetryReport(e.Report); err != nil {
		return errors.Join(ErrInvalidEvent, err)
	}
	return nil
}

// Event id accessors — thin pass-throughs to the embedded report so callers can
// read the shared ids without reaching into the wire type.

// DeviceID returns the reporting device id.
func (e Event) DeviceID() string { return e.Report.DeviceID }

// DeploymentID returns the deployment the event belongs to.
func (e Event) DeploymentID() string { return e.Report.DeploymentID }

// EventType returns the canonical telemetry event id.
func (e Event) EventType() otaprotocol.TelemetryEvent { return e.Report.Event }

// Timestamp returns the device-reported event time.
func (e Event) Timestamp() time.Time { return e.Report.Timestamp }

// IsTerminal reports whether the event is a terminal lifecycle state, i.e. the
// device reached success or failure for this deployment. Terminal events are
// the denominator of the cohort health math (spec §4).
func (e Event) IsTerminal() bool { return isTerminal(e.Report.Event) }

// IsSuccess reports whether the event is the terminal success state.
func (e Event) IsSuccess() bool { return e.Report.Event == EventSuccess }

// IsFailure reports whether the event is the terminal failure state.
func (e Event) IsFailure() bool { return e.Report.Event == EventFailure }

// isTerminal is the single definition of "terminal" used across the module.
func isTerminal(ev otaprotocol.TelemetryEvent) bool {
	return ev == EventSuccess || ev == EventFailure
}

// AllEventTypes returns the canonical, ordered set of telemetry event ids. It
// is the authoritative iteration order for per-event count reporting and is
// kept in lifecycle order.
func AllEventTypes() []otaprotocol.TelemetryEvent {
	return []otaprotocol.TelemetryEvent{
		EventDownloadStarted,
		EventInstalling,
		EventInstalled,
		EventVerifying,
		EventSuccess,
		EventFailure,
	}
}
