package blobcache

// isAligned always returns true on macOS since AlignSize is 0
func isAligned(block []byte) bool {
	return true
}
