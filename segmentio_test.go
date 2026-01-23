package blobcache

import (
	"math"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
)

func TestFooterEntriesToIndexItems_Basic(t *testing.T) {
	entries := []record.FooterEntry{
		{
			Key:          index.Key{Lo: 1, Hi: 2},
			Pos:          100,
			LogicalSize:  500,
			PhysicalSize: 400,
			SeqID:        42,
			Flags:        0,
			KeyLen:       16,
		},
		{
			Key:          index.Key{Lo: 3, Hi: 4},
			Pos:          600,
			LogicalSize:  1000,
			PhysicalSize: 800,
			SeqID:        43,
			Flags:        0,
			KeyLen:       32,
		},
	}

	items, err := footerEntriesToIndexItems(10, entries)
	require.NoError(t, err)
	require.Len(t, items, 2)

	// First item
	require.Equal(t, index.Key{Lo: 1, Hi: 2}, items[0].Key)
	require.Equal(t, uint32(10), items[0].SegmentID)
	require.Equal(t, uint32(100), items[0].Offset)
	// PhysicalLen = HeaderSize(42) + KeyLen(16) + PhysicalSize(400) = 458
	require.Equal(t, uint32(record.HeaderSize+16+400), items[0].PhysicalLen)

	// Second item
	require.Equal(t, index.Key{Lo: 3, Hi: 4}, items[1].Key)
	require.Equal(t, uint32(10), items[1].SegmentID)
	require.Equal(t, uint32(600), items[1].Offset)
	// PhysicalLen = HeaderSize(42) + KeyLen(32) + PhysicalSize(800) = 874
	require.Equal(t, uint32(record.HeaderSize+32+800), items[1].PhysicalLen)
}

func TestFooterEntriesToIndexItems_Empty(t *testing.T) {
	items, err := footerEntriesToIndexItems(5, nil)
	require.NoError(t, err)
	require.Empty(t, items)

	items, err = footerEntriesToIndexItems(5, []record.FooterEntry{})
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestFooterEntriesToIndexItems_PhysicalLenOverflow(t *testing.T) {
	// PhysicalSize that would overflow uint32 when combined with HeaderSize + KeyLen
	entries := []record.FooterEntry{
		{
			Key:          index.Key{Lo: 1, Hi: 2},
			Pos:          100,
			PhysicalSize: math.MaxUint32, // This alone exceeds uint32 when added to HeaderSize
			KeyLen:       16,
		},
	}

	_, err := footerEntriesToIndexItems(10, entries)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntryTooLarge)
}

func TestFooterEntriesToIndexItems_PosOverflow(t *testing.T) {
	// Pos that exceeds uint32
	entries := []record.FooterEntry{
		{
			Key:          index.Key{Lo: 1, Hi: 2},
			Pos:          math.MaxUint32 + 1,
			PhysicalSize: 100,
			KeyLen:       16,
		},
	}

	_, err := footerEntriesToIndexItems(10, entries)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntryTooLarge)
}

func TestFooterEntriesToIndexItems_Compression(t *testing.T) {
	// Entry with compression flag set
	entries := []record.FooterEntry{
		{
			Key:          index.Key{Lo: 1, Hi: 2},
			Pos:          100,
			PhysicalSize: 400,
			KeyLen:       16,
			Flags:        uint64(1) << record.FlagCompressionShift, // LZ4 compression
		},
	}

	items, err := footerEntriesToIndexItems(10, entries)
	require.NoError(t, err)
	require.Len(t, items, 1)

	// Verify compression is preserved
	require.True(t, items[0].IsCompressed())
}

func TestFooterEntriesToIndexItems_MaxValidSize(t *testing.T) {
	// Test boundary: maximum valid size that fits in uint32
	// PhysicalLen = HeaderSize(42) + KeyLen + PhysicalSize
	// Max valid: PhysicalSize = MaxUint32 - 42 - KeyLen
	maxPhysicalSize := int64(math.MaxUint32) - int64(record.HeaderSize) - 16

	entries := []record.FooterEntry{
		{
			Key:          index.Key{Lo: 1, Hi: 2},
			Pos:          0,
			PhysicalSize: maxPhysicalSize,
			KeyLen:       16,
		},
	}

	items, err := footerEntriesToIndexItems(10, entries)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, uint32(math.MaxUint32), items[0].PhysicalLen)

	// One byte more should fail
	entries[0].PhysicalSize = maxPhysicalSize + 1
	_, err = footerEntriesToIndexItems(10, entries)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEntryTooLarge)
}
