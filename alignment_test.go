package blobcache

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/miretskiy/dio/v2/collections"
)

// VerifyAlignment preserves the original xmap shard-size and implicit-padding
// checks. It is kept here because dependency _test.go helpers cannot be imported.
func VerifyAlignment[K comparable, V, E any]() error {
	var s collections.MapShard[K, V, E]
	totalSize := unsafe.Sizeof(s)

	// 1. Check Total Alignment (Must be multiple of 64)
	if totalSize%64 != 0 {
		return fmt.Errorf("collections: Shard size (%d) is not a multiple of 64; "+
			"add [%d]byte padding to Extra struct",
			totalSize, 64-(totalSize%64))
	}

	// 2. The Padding Detective (Check for compiler-inserted padding)
	// We inspect 'E' (the Extra struct) to see if the compiler added silent padding
	// after the last field to align the struct.
	eType := reflect.TypeOf(s.Extra)

	// Only inspect if it's a struct with fields
	if eType.Kind() == reflect.Struct && eType.NumField() > 0 {
		lastField := eType.Field(eType.NumField() - 1)

		// The generic offset calculation
		explicitEnd := lastField.Offset + lastField.Type.Size()

		if explicitEnd != eType.Size() {
			invisiblePad := eType.Size() - explicitEnd
			return fmt.Errorf("collections: implicit padding detected in %v: "+
				"compiler added %d bytes after field %q; "+
				"increase manual padding array by %d bytes",
				eType, invisiblePad, lastField.Name, invisiblePad)
		}
	}

	return nil
}
