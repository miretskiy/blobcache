package base

// Key is a type-safe wrapper for cache keys
// Provides compile-time safety to prevent mixing up keys with other []byte arguments
type Key []byte

// Raw returns the underlying byte slice
func (k Key) Raw() []byte {
	return k
}
