package blobcache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestDynamicMergeThreshold(t *testing.T) {
	tests := []struct {
		name        string
		avgBlobSize int64
		expected    float64
	}{
		{"4KB_blobs", 4 * 1024, 0.90},
		{"32KB_blobs", 32 * 1024, 0.90},
		{"64KB_blobs", 64 * 1024, 0.90},
		{"128KB_blobs", 128 * 1024, 0.8166666666666667}, // 0.90 - (64K/192K)*0.25
		{"256KB_blobs", 256 * 1024, 0.65},
		{"1MB_blobs", 1024 * 1024, 0.65},
		{"zero_size", 0, 0.90},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold := dynamicMergeThreshold(tt.avgBlobSize)
			require.InDelta(t, tt.expected, threshold, 1e-10)
		})
	}
}


