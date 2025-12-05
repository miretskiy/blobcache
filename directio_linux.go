package blobcache

import (
	"unsafe"

	"github.com/ncw/directio"
)

// isAligned checks if block is aligned in memory for DirectIO
func isAligned(block []byte) bool {
	if len(block) == 0 {
		return true
	}
	alignment := int(uintptr(unsafe.Pointer(&block[0])) & uintptr(directio.AlignSize-1))
	return alignment == 0
}
