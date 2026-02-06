package blobcache

import (
	"fmt"
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

func TestCalculateDynamicGravity(t *testing.T) {
	tests := []struct {
		name         string
		physicalSize int64
		logicalSize  int64
		expected     int
	}{
		{
			name:         "zero_logical",
			physicalSize: 1000,
			logicalSize:  0,
			expected:     minGravity, // 4
		},
		{
			name:         "zero_physical",
			physicalSize: 0,
			logicalSize:  1000,
			expected:     minGravity, // 4
		},
		{
			name:         "fully_dense",
			physicalSize: 1000,
			logicalSize:  1000,
			expected:     minGravity, // ratio=1.0, gravity=1, clamped to 4
		},
		{
			name:         "50_percent_sparse",
			physicalSize: 500,
			logicalSize:  1000,
			expected:     minGravity, // ratio=0.5, gravity=2, clamped to 4
		},
		{
			name:         "75_percent_sparse",
			physicalSize: 250,
			logicalSize:  1000,
			expected:     4, // ratio=0.25, gravity=4
		},
		{
			name:         "87_percent_sparse",
			physicalSize: 125,
			logicalSize:  1000,
			expected:     8, // ratio=0.125, gravity=8
		},
		{
			name:         "90_percent_sparse",
			physicalSize: 100,
			logicalSize:  1000,
			expected:     10, // ratio=0.1, gravity=10
		},
		{
			name:         "97_percent_sparse",
			physicalSize: 30,
			logicalSize:  1000,
			expected:     34, // ratio=0.03, gravity=34 (elastic window)
		},
		{
			name:         "99_percent_sparse",
			physicalSize: 10,
			logicalSize:  1000,
			expected:     100, // ratio=0.01, gravity=100 (elastic window)
		},
		{
			name:         "extreme_sparse",
			physicalSize: 8,
			logicalSize:  1000,
			expected:     125, // ratio=0.008, gravity=125 (near maxGravity=128)
		},
		{
			name:         "beyond_elastic_limit",
			physicalSize: 1,
			logicalSize:  1000,
			expected:     125, // ratio=0.001, clamped to 0.008, gravity=125
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gravity := calculateDynamicGravity(tt.physicalSize, tt.logicalSize)
			require.Equal(t, tt.expected, gravity)
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
	segments := cache.selectSegmentsForTombstoneCompaction(DefaultTombstoneCompactionThreshold)
	require.Empty(t, segments, "should be empty with no segments")

	// Write enough data to create tombstones that exceed the threshold.
	// Each small write creates a separate item.
	value := make([]byte, 1_000)
	numItems := DefaultTombstoneCompactionThreshold + 20 // 120 items
	for i := range numItems {
		key := []byte(fmt.Sprintf("key-%04d", i))
		require.NoError(t, cache.Put(key, value))
	}
	cache.Drain()

	// No tombstones yet, should select nothing
	segments = cache.selectSegmentsForTombstoneCompaction(DefaultTombstoneCompactionThreshold)
	require.Empty(t, segments, "should be empty with no tombstones")

	// Delete enough keys to cross the threshold (100+)
	numDeletes := DefaultTombstoneCompactionThreshold + 5 // 105 deletes
	for i := range numDeletes {
		key := []byte(fmt.Sprintf("key-%04d", i))
		require.NoError(t, cache.Delete(key))
	}

	// Create additional segments to push the first segment past the cooling period.
	// With MaxCachedSlabs=0, coolingGap=2, so we need at least 2 more segment IDs.
	for range 3 {
		largeValue := make([]byte, 200_000) // Force new segments
		require.NoError(t, cache.Put([]byte("filler-key"), largeValue))
		cache.Drain()
	}

	// Now should select the segment that crossed the threshold
	segments = cache.selectSegmentsForTombstoneCompaction(DefaultTombstoneCompactionThreshold)
	require.Len(t, segments, 1, "should select one segment with 105 tombstones")
}
