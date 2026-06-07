package otatelemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// Batch is a set of telemetry events transmitted together. The control plane
// JSON is Brotli/gzip-compressed in transit, but compression is a transport
// concern: this module only defines the uncompressed JSON shape. Transport and
// compression live elsewhere (spec §2, §9).
type Batch struct {
	Events []Event `json:"events"`
}

// Validate validates every event in the batch, returning the first failure
// annotated with its index. An empty batch is valid (it carries no events).
func (b Batch) Validate() error {
	for i, e := range b.Events {
		if err := e.Validate(); err != nil {
			return fmt.Errorf("event[%d]: %w", i, err)
		}
	}
	return nil
}

// EncodeBatch encodes a batch to canonical JSON bytes. Every event is validated
// first so an invalid enum or malformed report cannot silently round-trip out
// (mirrors ota-protocol's marshal-time refusal). On a validation failure it
// returns nil and an ErrEncode-wrapped error.
func EncodeBatch(b Batch) ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEncode, err)
	}
	out, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEncode, err)
	}
	return out, nil
}

// DecodeBatch decodes JSON bytes into a Batch and validates every event. It
// rejects unknown fields and trailing data so malformed input is never silently
// accepted (spec §2: "reject malformed events; no silent acceptance"). Decode
// failures wrap ErrDecode; validation failures wrap ErrInvalidEvent (via
// Batch.Validate).
func DecodeBatch(data []byte) (Batch, error) {
	return DecodeBatchFrom(bytes.NewReader(data))
}

// DecodeBatchFrom decodes a Batch from an io.Reader (the only stream surface;
// the caller owns the reader's lifecycle — this module never opens or closes
// any handle). Semantics match DecodeBatch.
func DecodeBatchFrom(r io.Reader) (Batch, error) {
	var b Batch
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		return Batch{}, fmt.Errorf("%w: %w", ErrDecode, err)
	}
	// Reject trailing tokens after the first JSON value so two concatenated
	// documents (or junk) are not silently accepted.
	if dec.More() {
		return Batch{}, fmt.Errorf("%w: unexpected trailing data after batch", ErrDecode)
	}
	if err := b.Validate(); err != nil {
		return Batch{}, err
	}
	return b, nil
}

// EncodeBatchTo encodes a validated batch to an io.Writer. The caller owns the
// writer; this module performs no I/O setup or teardown.
func EncodeBatchTo(w io.Writer, b Batch) error {
	out, err := EncodeBatch(b)
	if err != nil {
		return err
	}
	if _, err := w.Write(out); err != nil {
		return fmt.Errorf("%w: %w", ErrEncode, err)
	}
	return nil
}
