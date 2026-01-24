package blobcache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectContiguousRanges(t *testing.T) {
	tests := []struct {
		name     string
		input    []uint32
		expected [][]uint32
	}{
		{
			name:     "empty",
			input:    nil,
			expected: nil,
		},
		{
			name:     "single",
			input:    []uint32{5},
			expected: [][]uint32{{5}},
		},
		{
			name:     "contiguous_pair",
			input:    []uint32{1, 2},
			expected: [][]uint32{{1, 2}},
		},
		{
			name:     "contiguous_triple",
			input:    []uint32{10, 11, 12},
			expected: [][]uint32{{10, 11, 12}},
		},
		{
			name:     "gap_creates_two_ranges",
			input:    []uint32{1, 2, 3, 7, 8},
			expected: [][]uint32{{1, 2, 3}, {7, 8}},
		},
		{
			name:     "all_isolated",
			input:    []uint32{1, 5, 10, 20},
			expected: [][]uint32{{1}, {5}, {10}, {20}},
		},
		{
			name:     "mixed_ranges",
			input:    []uint32{1, 2, 3, 7, 8, 10, 100, 101},
			expected: [][]uint32{{1, 2, 3}, {7, 8}, {10}, {100, 101}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := selectContiguousRanges(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSegmentStats_WasteRatio(t *testing.T) {
	tests := []struct {
		name      string
		stats     SegmentStats
		expected  float64
		tolerance float64
	}{
		{
			name:      "empty_segment",
			stats:     SegmentStats{TombstoneCount: 0, LiveItemCount: 0},
			expected:  0.0,
			tolerance: 0.0,
		},
		{
			name:      "no_tombstones",
			stats:     SegmentStats{TombstoneCount: 0, LiveItemCount: 100},
			expected:  0.0,
			tolerance: 0.0,
		},
		{
			name:      "all_tombstones",
			stats:     SegmentStats{TombstoneCount: 100, LiveItemCount: 0},
			expected:  1.0,
			tolerance: 0.0,
		},
		{
			name:      "half_tombstones",
			stats:     SegmentStats{TombstoneCount: 50, LiveItemCount: 50},
			expected:  0.5,
			tolerance: 0.001,
		},
		{
			name:      "quarter_tombstones",
			stats:     SegmentStats{TombstoneCount: 25, LiveItemCount: 75},
			expected:  0.25,
			tolerance: 0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio := tt.stats.WasteRatio()
			if tt.tolerance == 0 {
				require.Equal(t, tt.expected, ratio)
			} else {
				require.InDelta(t, tt.expected, ratio, tt.tolerance)
			}
		})
	}
}

func TestSelectSegmentsForTombstoneCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := New(tmpDir,
		WithWAL(),
		WithMaxCachedSlabs(0),
	)
	require.NoError(t, err)
	cache.Start()
	defer cache.Close()

	// Initially no segments
	segments, err := cache.selectSegmentsForTombstoneCompaction(10)
	require.NoError(t, err)
	require.Empty(t, segments, "should be empty with no segments")

	// Write data to create a segment
	value := make([]byte, 10_000)
	for i := range 20 {
		key := []byte("key-" + string(rune('A'+i)))
		require.NoError(t, cache.Put(key, value))
	}
	cache.Drain()

	// No tombstones yet, should select nothing with threshold=10
	segments, err = cache.selectSegmentsForTombstoneCompaction(10)
	require.NoError(t, err)
	require.Empty(t, segments, "should be empty with no tombstones")

	// Delete 15 keys to create tombstones
	for i := range 15 {
		key := []byte("key-" + string(rune('A'+i)))
		require.NoError(t, cache.Delete(key))
	}

	// Now should select the segment with threshold=10
	segments, err = cache.selectSegmentsForTombstoneCompaction(10)
	require.NoError(t, err)
	require.Len(t, segments, 1, "should select one segment with 15 tombstones")

	// Higher threshold should not select
	segments, err = cache.selectSegmentsForTombstoneCompaction(20)
	require.NoError(t, err)
	require.Empty(t, segments, "should not select with threshold=20 when we have 15 tombstones")
}
