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

// CurrentTelemetrySchemaVersion is the single, authoritative schema version for
// the telemetry envelope. The server rejects any message whose schema_version
// does not match this value with a 400 error, so old-device messages with a
// lower version and future-device messages with a higher version are both
// rejected rather than silently misinterpreted.
//
// VERSION-BUMP PROCEDURE:
//   1. Bump this constant to N+1.
//   2. Add a second constant OldTelemetrySchemaVersionN = N (the previous value)
//      so the server can accept BOTH during a migration window.
//   3. Update Validate() to accept either the current version OR an old version
//      that is still within the active migration window.
//   4. Ship the device agent and server atomically or with a version-negotiation
//      handshake first.
//   5. Once all devices have migrated, remove the old-version acceptance and the
//      OldTelemetrySchemaVersionN constant.
const CurrentTelemetrySchemaVersion = 1

// Event is the telemetry envelope shared by server and agents. It composes the
// canonical ota-protocol.TelemetryReport (the locked wire fields) with optional
// shared, transport-free annotations that the health model and dashboard use
// but that are not part of the minimal device contract.
//
// The embedded Report carries the authoritative ids (DeviceID, DeploymentID,
// Event, Progress, ErrorCode, Timestamp). The envelope adds:
//   - SchemaVersion: the message-schema version for cross-version device↔server
//     evolution (int, starting at CurrentTelemetrySchemaVersion). The server
//     rejects any unknown version.
//   - Cohort: the rollout phase/cohort the device belongs to, so health can be
//     derived per cohort as well as per deployment (spec §4: "per phase/cohort").
//   - SystemHealth: the device-reported system_health field (spec §4) used for
//     problem detection/reporting.
//   - ErrorMessage: a human-readable companion to Report.ErrorCode.
//
// The envelope intentionally does not add any transport or storage concerns.
type Event struct {
	Report        otaprotocol.TelemetryReport `json:"report"`
	SchemaVersion int                         `json:"schema_version"`
	Cohort        string                      `json:"cohort,omitempty"`
	SystemHealth  string                      `json:"system_health,omitempty"`
	ErrorMessage  string                      `json:"error_message,omitempty"`
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
	// ErrUnknownSchemaVersion indicates the message's schema_version does not
	// match any version the server accepts.
	ErrUnknownSchemaVersion = errors.New("otatelemetry: unknown schema version")
)

// NewEvent constructs an envelope from the canonical wire report. SchemaVersion
// defaults to CurrentTelemetrySchemaVersion; callers that need to decode an
// older-version event from storage should use NewEventWithVersion.
func NewEvent(r otaprotocol.TelemetryReport) Event {
	return Event{Report: r, SchemaVersion: CurrentTelemetrySchemaVersion}
}

// NewEventWithVersion constructs an envelope with an explicit schema version.
// This is used when decoding persisted events that carry a version field (e.g.
// from storage or from a device that reported an older version during a
// migration window).
func NewEventWithVersion(r otaprotocol.TelemetryReport, version int) Event {
	return Event{Report: r, SchemaVersion: version}
}

// Validate validates the envelope. The embedded report must satisfy the
// canonical ota-protocol contract; on failure the underlying sentinel is
// preserved (so errors.Is against otaprotocol.ErrMissingField etc. still works)
// and ErrInvalidEvent is joined for module-level matching.
//
// SchemaVersion is validated: a zero value (legacy payload or test construct
// without an explicit version) is accepted; any version > Current is rejected.
// When a negative value or a version beyond Current is encountered,
// ErrUnknownSchemaVersion is returned so the server can reject it with a 400.
func (e Event) Validate() error {
	if e.SchemaVersion < 0 || e.SchemaVersion > CurrentTelemetrySchemaVersion {
		return ErrUnknownSchemaVersion
	}
	return e.ValidateReport()
}

// ValidateWithAcceptedVersions validates the envelope's report (the canonical
// ota-protocol contract) while accepting any of the given schema versions plus
// the current version. This is used during a schema-migration window when both
// old and new device formats must be accepted.
func (e Event) ValidateWithAcceptedVersions(accepted ...int) error {
	valid := e.SchemaVersion == CurrentTelemetrySchemaVersion
	for _, v := range accepted {
		if e.SchemaVersion == v {
			valid = true
			break
		}
	}
	if !valid {
		return ErrUnknownSchemaVersion
	}
	return e.ValidateReport()
}

// ValidateReport validates only the embedded ota-protocol report without
// checking the schema version. This is the shared validation path used by
// Validate and ValidateWithAcceptedVersions.
func (e Event) ValidateReport() error {
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
