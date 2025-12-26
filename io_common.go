package blobcache

import "github.com/ncw/directio"

const mask = directio.BlockSize - 1

// alignForHolePunch aligns offset and length to filesystem block boundaries
// Returns (alignedOffset, alignedLength, canPunch)
// canPunch is false if there are no complete blocks to punch
func alignForHolePunch(offset, length int64) (int64, int64, bool) {
	// Round offset UP to next block boundary (don't punch into previous blob)
	alignedOffset := (offset + mask) &^ mask
	length -= alignedOffset - offset

	// Skip if blob smaller than one block after adjustment
	if length < directio.BlockSize {
		return 0, 0, false
	}

	// Round length DOWN to block multiple (don't punch into next blob)
	length &^= mask

	return alignedOffset, length, true
}
