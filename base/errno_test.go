package base

import "testing"

// TestBlobErrno_FitsInFiveBits ensures all errno values fit in 5 bits (< 32).
// This is critical since BlobErrno is stored in a 5-bit field in BlobRecord.Flags.
func TestBlobErrno_FitsInFiveBits(t *testing.T) {
	// Iterate through all defined errno values (0 to maxErrno-1)
	for i := BlobErrno(0); i < maxErrno; i++ {
		if i >= 32 {
			t.Errorf("%s (value=%d) exceeds 5-bit field limit (must be < 32)", i, i)
		}
		// Call String() to ensure stringer is generated correctly
		_ = i.String()
	}
}

func TestBlobErrno_OutOfRange(t *testing.T) {
	// Values >= maxErrno should not panic and should return a formatted string
	outOfRange := BlobErrno(50)
	got := outOfRange.String()
	want := "BlobErrno(50)"
	if got != want {
		t.Errorf("BlobErrno(50).String() = %q, want %q", got, want)
	}
}
