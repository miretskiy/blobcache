package index

// SegmentID is a strong type for segment identifiers.
// Using a distinct type prevents accidental parameter swapping with Offset.
type SegmentID uint32

// Offset represents a byte position within a segment file.
// Using a distinct type prevents accidental parameter swapping with SegmentID.
type Offset uint32

// RelocateMode defines the safety constraints for moving items during compaction.
type RelocateMode uint8

const (
	// RelocateLive fails if the item is found to be deleted (Ghost Guard).
	// Use when compacting live data items.
	RelocateLive RelocateMode = iota

	// RelocateTombstone fails if the item is found to be alive (Race Guard).
	// Use when compacting tombstone records.
	RelocateTombstone
)

// ExpectDeleted returns true if this mode expects the item to be deleted.
func (m RelocateMode) ExpectDeleted() bool {
	return m == RelocateTombstone
}
