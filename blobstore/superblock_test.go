package blobstore

import (
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func TestGeometry(t *testing.T) {
	for _, tc := range []struct {
		capacity, unit uint64
	}{
		{64 << 10, 4 << 10},   // Halved until the directory covers 16 units.
		{16 << 20, 1 << 20},   //
		{64 << 20, 4 << 20},   // The first size at the full unit.
		{1900 << 30, 4 << 20}, // An instance-store namespace.
	} {
		g := newGeometry(tc.capacity)
		require.Equal(t, tc.capacity, g.capacity)
		require.Equal(t, tc.unit, g.unit)
		require.GreaterOrEqual(t, uint64(g.slots()), (g.capacity+g.unit-1)/g.unit)
		require.Zero(t, g.dirBytes%align.BlockSize)
		require.Equal(t, superblockSize+g.dirBytes, g.dataStart)
		require.GreaterOrEqual(t, g.segmentBytes(), g.unit, "consecutive footers start in different units")
		require.LessOrEqual(t, g.segmentBytes(), uint64(maxSegment))
	}
	// 7 TB at 4 MiB units: 1,668,931 slots, a 13.4 MB directory written one
	// sector at a time.
	require.Equal(t, uint64(13352960), newGeometry(7e12).dirBytes)
	require.Equal(t, uint64(64<<10), newGeometry(64<<10+100).capacity, "capacity rounds down to a page")
}

func TestFitGeometry(t *testing.T) {
	for _, total := range []uint64{1 << 20, 64 << 20, 68987912192, 1487681880064} {
		g := fitGeometry(total)
		require.LessOrEqual(t, g.dataStart+g.capacity, total)
		require.Greater(t, g.capacity+g.dataStart+g.unit, total, "only the directory and rounding are unused")
	}
}

func TestGeometryPositions(t *testing.T) {
	g := newGeometry(16 << 20)
	require.EqualValues(t, g.dataStart+5<<20, g.offset(5<<20))
	require.EqualValues(t, g.dataStart+5<<20, g.offset(5<<20+3*g.capacity), "offsets repeat every lap")
	require.Equal(t, 5, g.slot(5<<20+2*g.capacity))
	require.Zero(t, g.tail(g.capacity-1))
	require.Equal(t, uint64(7), g.tail(g.capacity+7))
	require.Equal(t, uint64(align.BlockSize), footerSize(63))
	require.Equal(t, uint64(2*align.BlockSize), footerSize(64))
}

func TestSuperblockEncoding(t *testing.T) {
	g := newGeometry(64 << 20)
	page := make([]byte, superblockSize)
	g.encode(page)
	decoded, ok := decodeGeometry(page)
	require.True(t, ok)
	require.Equal(t, g, decoded)
	for _, i := range []int{0, 8, 39, 40} {
		corrupt := append([]byte(nil), page...)
		corrupt[i] ^= 1
		_, ok := decodeGeometry(corrupt)
		require.False(t, ok, "byte %d", i)
	}
	_, ok = decodeGeometry(make([]byte, superblockSize))
	require.False(t, ok, "an unformatted range has no superblock")
}
