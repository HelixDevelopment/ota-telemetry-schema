package otatelemetry

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	otaprotocol "github.com/HelixDevelopment/ota-protocol"
)

func sampleBatch() Batch {
	return Batch{Events: []Event{
		{Report: report("d1", "dep1", EventDownloadStarted), SchemaVersion: CurrentTelemetrySchemaVersion},
		{Report: report("d1", "dep1", EventSuccess), SchemaVersion: CurrentTelemetrySchemaVersion, Cohort: "canary"},
		{Report: report("d2", "dep1", EventFailure), SchemaVersion: CurrentTelemetrySchemaVersion, ErrorMessage: "boom"},
	}}
}

func TestEncodeDecodeBatchRoundTrip(t *testing.T) {
	in := sampleBatch()
	data, err := EncodeBatch(in)
	if err != nil {
		t.Fatalf("EncodeBatch() error = %v", err)
	}
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch() error = %v", err)
	}
	if len(out.Events) != len(in.Events) {
		t.Fatalf("round-trip len = %d, want %d", len(out.Events), len(in.Events))
	}
	for i := range in.Events {
		if out.Events[i] != in.Events[i] {
			t.Errorf("event[%d] = %+v, want %+v", i, out.Events[i], in.Events[i])
		}
	}
}

func TestEncodeBatchRejectsInvalidEvent(t *testing.T) {
	b := Batch{Events: []Event{
		{Report: report("d1", "dep1", EventSuccess), SchemaVersion: CurrentTelemetrySchemaVersion},
		{Report: report("", "dep1", EventSuccess), SchemaVersion: CurrentTelemetrySchemaVersion}, // missing device id
	}}
	_, err := EncodeBatch(b)
	if err == nil {
		t.Fatal("EncodeBatch() = nil error, want failure")
	}
	if !errors.Is(err, ErrEncode) {
		t.Errorf("error %v not matched by ErrEncode", err)
	}
	if !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("error %v not matched by ErrInvalidEvent", err)
	}
}

func TestEmptyBatchIsValid(t *testing.T) {
	data, err := EncodeBatch(Batch{})
	if err != nil {
		t.Fatalf("EncodeBatch(empty) error = %v", err)
	}
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch(empty) error = %v", err)
	}
	if len(out.Events) != 0 {
		t.Errorf("empty batch decoded %d events", len(out.Events))
	}
}

func TestDecodeBatchFailures(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"not json", "{", ErrDecode},
		{"wrong type", `{"events": 5}`, ErrDecode},
		{"unknown field", `{"events": [], "extra": 1}`, ErrDecode},
		{"trailing data", `{"events": []}{"events": []}`, ErrDecode},
		{"unknown schema version", `{"events":[{"schema_version":99,"report":{"device_id":"d","deployment_id":"dep","event":"success","progress":0,"timestamp":"2026-06-07T12:00:00Z"}}]}`, ErrUnknownSchemaVersion},
		{"invalid enum in event", `{"events":[{"schema_version":1,"report":{"device_id":"d","deployment_id":"dep","event":"idle","progress":0,"timestamp":"2026-06-07T12:00:00Z"}}]}`, otaprotocol.ErrInvalidEnum},
		{"missing field in event", `{"events":[{"schema_version":1,"report":{"device_id":"","deployment_id":"dep","event":"success","progress":0,"timestamp":"2026-06-07T12:00:00Z"}}]}`, ErrInvalidEvent},
		{"progress out of range", `{"events":[{"schema_version":1,"report":{"device_id":"d","deployment_id":"dep","event":"installing","progress":200,"timestamp":"2026-06-07T12:00:00Z"}}]}`, otaprotocol.ErrInvalidValue},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeBatch([]byte(tc.input))
			if err == nil {
				t.Fatalf("DecodeBatch(%q) = nil error, want %v", tc.input, tc.wantErr)
			}
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error %v not matched by %v", err, tc.wantErr)
			}
		})
	}
}

func TestDecodeBatchFromReaderError(t *testing.T) {
	_, err := DecodeBatchFrom(&errReader{})
	if err == nil {
		t.Fatal("DecodeBatchFrom(errReader) = nil error, want failure")
	}
	if !errors.Is(err, ErrDecode) {
		t.Errorf("error %v not matched by ErrDecode", err)
	}
}

func TestEncodeBatchToWriter(t *testing.T) {
	var buf bytes.Buffer
	if err := EncodeBatchTo(&buf, sampleBatch()); err != nil {
		t.Fatalf("EncodeBatchTo() error = %v", err)
	}
	out, err := DecodeBatchFrom(strings.NewReader(buf.String()))
	if err != nil {
		t.Fatalf("DecodeBatchFrom() error = %v", err)
	}
	if len(out.Events) != 3 {
		t.Errorf("decoded %d events, want 3", len(out.Events))
	}
}

func TestEncodeBatchToInvalid(t *testing.T) {
	var buf bytes.Buffer
	bad := Batch{Events: []Event{{Report: report("", "dep", EventSuccess), SchemaVersion: CurrentTelemetrySchemaVersion}}}
	if err := EncodeBatchTo(&buf, bad); !errors.Is(err, ErrEncode) {
		t.Errorf("EncodeBatchTo(invalid) error = %v, want ErrEncode", err)
	}
	if buf.Len() != 0 {
		t.Errorf("writer received %d bytes on validation failure, want 0", buf.Len())
	}
}

func TestEncodeBatchToWriterError(t *testing.T) {
	err := EncodeBatchTo(failWriter{}, sampleBatch())
	if !errors.Is(err, ErrEncode) {
		t.Errorf("EncodeBatchTo(failWriter) error = %v, want ErrEncode", err)
	}
}

// errReader always fails, exercising the decoder's read-error path.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

// failWriter always fails, exercising the writer-error path.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrShortWrite }
