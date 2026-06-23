package otatelemetry

import (
	"errors"
	"testing"
)

// FuzzDecodeBatch fuzzes the untrusted-JSON decode entry point. The control
// plane receives Brotli/gzip-decompressed JSON bytes from devices and feeds
// them straight into DecodeBatch (DisallowUnknownFields + trailing-token
// rejection + per-event validation). The property under test: ANY input must
// either round-trip into a valid Batch or return an error — it must NEVER
// panic, hang, or accept malformed input silently. A panic on attacker bytes
// is a denial-of-service crasher.
func FuzzDecodeBatch(f *testing.F) {
	seeds := [][]byte{
		// valid: empty batch
		[]byte(`{"events":[]}`),
		// valid: one well-formed event
		[]byte(`{"events":[{"report":{"device_id":"d1","deployment_id":"dep1","event":"success","progress":100,"timestamp":"2024-01-01T00:00:00Z"}}]}`),
		// edge: unknown field (must be rejected, not crash)
		[]byte(`{"events":[],"junk":1}`),
		// edge: trailing data after first value
		[]byte(`{"events":[]}{"events":[]}`),
		// edge: malformed JSON
		[]byte(`{"events":`),
		// edge: array instead of object
		[]byte(`[]`),
		// edge: invalid enum in event
		[]byte(`{"events":[{"report":{"device_id":"d","deployment_id":"x","event":"bogus","progress":0,"timestamp":"2024-01-01T00:00:00Z"}}]}`),
		// edge: out-of-range progress
		[]byte(`{"events":[{"report":{"device_id":"d","deployment_id":"x","event":"success","progress":99999,"timestamp":"2024-01-01T00:00:00Z"}}]}`),
		// edge: empty / null / whitespace
		[]byte(``),
		[]byte(`null`),
		[]byte(`   `),
		// edge: huge negative progress
		[]byte(`{"events":[{"report":{"device_id":"d","deployment_id":"x","event":"success","progress":-2147483648,"timestamp":"2024-01-01T00:00:00Z"}}]}`),
		// edge: embedded control/NUL bytes inside a string field
		append(append([]byte(`{"events":[{"report":{"device_id":"`), 0x00, 0x01, 0x1f),
			[]byte(`","deployment_id":"x","event":"success","progress":0,"timestamp":"2024-01-01T00:00:00Z"}}]}`)...),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// The only contract: never panic. The deferred recover converts any
		// panic into an explicit test failure with the offending input.
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodeBatch panicked on input %q: %v", data, r)
			}
		}()

		b, err := DecodeBatch(data)
		if err != nil {
			// Error is the expected outcome for malformed input. Sanity-check
			// the error sentinels are well-formed (errors.Is must not panic).
			_ = errors.Is(err, ErrDecode)
			_ = errors.Is(err, ErrInvalidEvent)
			return
		}
		// On success, the returned batch MUST itself revalidate cleanly — a
		// decode that returns nil error but a batch that fails Validate would
		// be a silent-acceptance bug.
		if verr := b.Validate(); verr != nil {
			t.Fatalf("DecodeBatch returned nil error but Batch.Validate failed: %v (input %q)", verr, data)
		}
	})
}
