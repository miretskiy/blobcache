package blobstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSegment = 256 << 10

// recorder remembers the records ReadIndex reported: the last reported per
// key, which is the latest.
type recorder map[KeyHash]Location

func (r recorder) load(h KeyHash, loc Location) { r[h] = loc }

// openStore opens a store with small segments and reads its index into rec.
func openStore(t *testing.T, dir string, rec recorder, opts ...Option) *Store {
	t.Helper()
	st, err := Open(dir, append([]Option{WithSegmentSize(testSegment)}, opts...)...)
	require.NoError(t, err)
	require.NoError(t, st.ReadIndex(rec.load))
	return st
}

// buffered are the options of a store that writes and reads through the page
// cache.
var buffered = []Option{WithDirectWrites(false), WithDirectReads(false)}

// alignedMemory returns n bytes of page-aligned memory, freed when the test
// ends. Tests close their stores first (defers run before cleanups).
func alignedMemory(t *testing.T, n int) []byte {
	t.Helper()
	mem := align.AllocAligned(max(n, 1))
	t.Cleanup(func() { align.FreeAligned(mem) })
	return mem[:n]
}

func randomBytes(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		var w [8]byte
		binary.LittleEndian.PutUint64(w[:], rng.Uint64())
		copy(b[i:], w[:])
	}
	return b
}

// write stores value under key and waits for the write.
func write(t *testing.T, st *Store, key string, value []byte) Location {
	t.Helper()
	buf := align.AllocAligned(max(st.RecordSize(len(key), len(value)), 1))
	defer align.FreeAligned(buf)
	copy(buf, value)
	ticket, err := st.Write([]byte(key), buf, len(value))
	require.NoError(t, err)
	loc, err := ticket.Wait()
	require.NoError(t, err)
	return loc
}

func read(t *testing.T, st *Store, loc Location, key string) ([]byte, error) {
	t.Helper()
	return st.Read(loc, []byte(key), alignedMemory(t, loc.Size()))
}

func TestWriteReadRoundTrip(t *testing.T) {
	for name, opts := range map[string][]Option{"direct": nil, "buffered": buffered} {
		t.Run(name, func(t *testing.T) {
			st := openStore(t, t.TempDir(), recorder{}, append([]Option{WithSegmentSize(4 << 20)}, opts...)...)
			defer func() { require.NoError(t, st.Close()) }()
			var prev Location
			for i, size := range []int{0, 1, 100, 4048, 4049, 4096, 4097, 64 << 10, 3*64<<10 + 17, 1 << 20} {
				key := fmt.Sprintf("key-%d", i)
				value := randomBytes(uint64(i), size)
				loc := write(t, st, key, value)
				require.Equal(t, st.RecordSize(len(key), size), loc.Size())
				if name == "direct" {
					require.Zero(t, loc.offset%align.BlockSize, "direct records start on a page")
				} else if i > 0 {
					require.Equal(t, prev.offset+prev.size, loc.offset, "buffered records are packed")
				}
				prev = loc
				got, err := read(t, st, loc, key)
				require.NoError(t, err)
				require.Equal(t, value, got)
				_, err = read(t, st, loc, "other")
				require.ErrorIs(t, err, ErrCorrupt, "the record is not another key's")
			}
		})
	}
}

func TestDirectReadsNeedDirectWrites(t *testing.T) {
	_, err := Open(t.TempDir(), WithDirectWrites(false))
	require.Error(t, err)
}

// TestWriteAndReadReallocate checks that memory too short, or unaligned for
// direct I/O, still works, like append: the store uses memory of its own.
func TestWriteAndReadReallocate(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{})
	defer func() { require.NoError(t, st.Close()) }()
	value := randomBytes(1, 10000)

	short := slices.Clone(value) // exactly the value: no room for the framing
	ticket, err := st.Write([]byte("short"), short, len(value))
	require.NoError(t, err)
	shortLoc, err := ticket.Wait()
	require.NoError(t, err)
	require.Equal(t, value, short, "the caller's value is untouched")

	mem := alignedMemory(t, st.RecordSize(1, len(value))+1)[1:]
	copy(mem, value)
	ticket, err = st.Write([]byte("u"), mem, len(value))
	require.NoError(t, err)
	unalignedLoc, err := ticket.Wait()
	require.NoError(t, err)

	for key, loc := range map[string]Location{"short": shortLoc, "u": unalignedLoc} {
		got, err := st.Read(loc, []byte(key), make([]byte, 10)) // too short
		require.NoError(t, err)
		require.Equal(t, value, got)
		got, err = st.Read(loc, []byte(key), alignedMemory(t, loc.Size()+1)[1:]) // unaligned
		require.NoError(t, err)
		require.Equal(t, value, got)
	}
}

func TestWriteRejects(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{})
	mem := alignedMemory(t, testSegment)
	_, err := st.Write([]byte("k"), mem[:10], 11)
	require.Error(t, err, "value longer than its buffer")
	_, err = st.Write(nil, mem, 1)
	require.ErrorIs(t, err, ErrEmptyKey)
	_, err = st.Write(make([]byte, MaxKeyLen+1), mem, 1)
	require.ErrorIs(t, err, ErrKeyTooLarge)
	_, err = st.Write([]byte("huge"), mem, testSegment)
	require.ErrorIs(t, err, ErrValueTooLarge)

	long := make([]byte, MaxKeyLen)
	ticket, err := st.Write(long, mem, 100)
	require.NoError(t, err)
	loc, err := ticket.Wait()
	require.NoError(t, err)
	_, err = read(t, st, loc, string(long))
	require.NoError(t, err, "the longest key")

	require.NoError(t, st.Close())
	require.NoError(t, st.Close(), "Close is idempotent")
	_, err = st.Write([]byte("k"), mem, 1)
	require.ErrorIs(t, err, ErrClosed)
	_, err = read(t, st, loc, string(long))
	require.ErrorIs(t, err, ErrClosed)
}

// TestSegmentFiles checks where segment files go, and that a store closed
// with nothing written leaves none behind.
func TestSegmentFiles(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	require.NoError(t, st.Close())
	require.Empty(t, segmentFiles(t, dir), "a segment's file is created by its first write")

	st = openStore(t, dir, recorder{})
	for i := range 10 {
		write(t, st, fmt.Sprint(i), randomBytes(uint64(i), 100<<10))
	}
	require.NoError(t, st.Close())
	for id := range uint64(3) {
		require.FileExists(t, segmentPath(dir, id))
	}
	info, err := os.Stat(segmentPath(dir, 0))
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(testSegment), "the last record and the footer run past the segment size")
	require.FileExists(t, dir+"/01/0000000000000001.seg")
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	for shard := range shardCount {
		dirents, err := os.ReadDir(dir + "/" + shardName(shard))
		require.NoError(t, err)
		for _, de := range dirents {
			files = append(files, de.Name())
		}
	}
	return files
}

// TestReopenReportsRecordsInWriteOrder checks that ReadIndex reports every
// sealed record, the later of two records of a key last, and nothing written
// since Open.
func TestReopenReportsRecordsInWriteOrder(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	want := recorder{}
	for i := range 40 {
		key := fmt.Sprintf("key-%d", i%30) // ten keys written twice
		want[HashKey([]byte(key))] = write(t, st, key, randomBytes(uint64(i), 20<<10+i*997))
	}
	require.NoError(t, st.Close())

	st, err := Open(dir, WithSegmentSize(testSegment))
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	write(t, st, "after-open", randomBytes(1, 100))
	rec := recorder{}
	require.NoError(t, st.ReadIndex(rec.load))
	require.Equal(t, want, rec)
	require.Greater(t, st.Stats().Segments, 3, "test must span several segments")
}

// TestConcurrentWritesAndReads churns a small handle cache while writers
// rotate segments and readers submit concurrent reads. Run with -race.
func TestConcurrentWritesAndReads(t *testing.T) {
	st := openStore(t, t.TempDir(), recorder{}, WithMaxReadHandles(2))
	defer func() { require.NoError(t, st.Close()) }()
	var (
		mu      sync.Mutex
		landed  []Location
		keys    []int
		writers sync.WaitGroup
		readers sync.WaitGroup
		stop    atomic.Bool
	)
	for w := range 4 {
		writers.Go(func() {
			for i := w; i < 200; i += 4 {
				loc := write(t, st, fmt.Sprint(i), randomBytes(uint64(i), 30<<10))
				mu.Lock()
				landed, keys = append(landed, loc), append(keys, i)
				mu.Unlock()
			}
		})
	}
	for r := range 4 {
		readers.Go(func() {
			buf := alignedMemory(t, 64<<10)
			for j := r; !stop.Load(); j++ {
				mu.Lock()
				if len(landed) == 0 {
					mu.Unlock()
					continue
				}
				loc, i := landed[j%len(landed)], keys[j%len(keys)]
				mu.Unlock()
				got, err := st.Read(loc, []byte(fmt.Sprint(i)), buf)
				if errors.Is(err, ErrBusy) {
					continue // every open slot was about to be read
				}
				if assert.NoError(t, err) {
					assert.Equal(t, randomBytes(uint64(i), 30<<10), got)
				}
			}
		})
	}
	writers.Wait()
	stop.Store(true)
	readers.Wait()
	require.Greater(t, st.Stats().Segments, 10)
}

// TestCrashBeforeSeal simulates a crash while a segment was active by erasing
// its footer: its records are discarded, and sealed segments survive.
func TestCrashBeforeSeal(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	old := write(t, st, "old", randomBytes(1, 100<<10))
	// Fill past the first segment so "old" is sealed.
	for i := range 4 {
		write(t, st, fmt.Sprintf("filler-%d", i), randomBytes(uint64(i), 100<<10))
	}
	recent := write(t, st, "recent", []byte("lost in the crash"))
	activeID := st.writers[0].active.seg.id
	require.Equal(t, activeID, recent.segment)
	require.NoError(t, st.Close())

	active := segmentPath(dir, activeID)
	eraseFooter(t, active)

	rec := recorder{}
	st = openStore(t, dir, rec)
	defer func() { require.NoError(t, st.Close()) }()
	require.Equal(t, old, rec[HashKey([]byte("old"))])
	require.NotContains(t, rec, HashKey([]byte("recent")))
	require.NoFileExists(t, active, "unsealed data is discarded")
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestCorruptionIsDetected(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	valueLoc := write(t, st, "value-corrupt", randomBytes(1, 10000))
	trailerLoc := write(t, st, "trailer-corrupt", randomBytes(2, 10000))
	require.NoError(t, st.Close())

	flipByte(t, segmentPath(dir, valueLoc.segment), int64(valueLoc.offset)+100)
	flipByte(t, segmentPath(dir, trailerLoc.segment), int64(trailerLoc.offset+trailerLoc.size)-10)

	st = openStore(t, dir, recorder{})
	defer func() { require.NoError(t, st.Close()) }()
	_, err := read(t, st, valueLoc, "value-corrupt")
	require.ErrorIs(t, err, ErrCorrupt)
	var ce *base.ChecksumError
	require.ErrorAs(t, err, &ce)
	_, err = read(t, st, trailerLoc, "trailer-corrupt")
	require.ErrorIs(t, err, ErrCorrupt)
	require.NotErrorAs(t, err, &ce)
}

// failSubmission returns a hook that replaces the submission numbered at
// (from 1) with a write to a slot no segment uses, which fails with EBADF, as
// a write the device rejected would fail.
func failSubmission(at int64) Option {
	var submits atomic.Int64
	return TestingWithPreSubmit(func(op iosched.Op) iosched.Op {
		if submits.Add(1) == at {
			return iosched.VWriteOp(writeSlots-1, make([]byte, align.BlockSize), 0)
		}
		return op
	})
}

// A failed record does not invalidate successful peers or their footer.
func TestFailedWriteDoesNotPoisonSegment(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{}, failSubmission(2))
	landed := write(t, st, "landed", randomBytes(1, 10000))
	ticket, err := st.Write([]byte("failed"), alignedMemory(t, st.RecordSize(6, 10000)), 10000)
	require.NoError(t, err)
	_, err = ticket.Wait()
	require.Error(t, err)
	after := write(t, st, "after", randomBytes(3, 10000))
	require.Equal(t, landed.segment, after.segment)
	require.NoError(t, st.Close())
	rec := recorder{}
	st = openStore(t, dir, rec)
	defer func() { require.NoError(t, st.Close()) }()
	got, err := read(t, st, rec[HashKey([]byte("landed"))], "landed")
	require.NoError(t, err)
	require.Equal(t, randomBytes(1, 10000), got)
	_, err = read(t, st, rec[HashKey([]byte("failed"))], "failed")
	require.ErrorIs(t, err, ErrCorrupt)
	require.Equal(t, after, rec[HashKey([]byte("after"))])
}

func TestOpenRequiresDirectory(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "missing"))
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// TestOpenInitializesOnce checks that the first Open creates the segment
// directories and the marker, and that later Opens create nothing: with the
// marker present, a missing directory is an error, not recreated.
func TestOpenInitializesOnce(t *testing.T) {
	dir := t.TempDir()
	st := openStore(t, dir, recorder{})
	require.NoError(t, st.Close())
	require.FileExists(t, filepath.Join(dir, markerName))
	require.DirExists(t, filepath.Join(dir, "ff"))

	require.NoError(t, os.Remove(filepath.Join(dir, "ff")))
	_, err := Open(dir)
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(dir, "ff"))

	require.NoError(t, os.WriteFile(filepath.Join(dir, markerName), []byte("blobstore format 999\n"), 0o644))
	_, err = Open(dir)
	require.ErrorContains(t, err, "unsupported store")
}

func TestRecordFraming(t *testing.T) {
	key := []byte("k")
	for _, direct := range []bool{true, false} {
		for _, n := range []int{0, 1, 4048, 4049, 4096, 10000} {
			value := randomBytes(uint64(n), n)
			size := n + len(key) + trailerSize
			if direct {
				size = int(align.PageAlign(int64(size)))
			}
			rec := make([]byte, size)
			copy(rec, value)
			frameRecord(rec, key, recordTrailer{valueLen: uint32(n), keyLen: uint16(len(key))})
			got, err := verifyRecord(rec, key)
			require.NoError(t, err, "value of %d bytes", n)
			require.Equal(t, value, got)
			_, err = verifyRecord(rec, []byte("other"))
			require.ErrorIs(t, err, ErrCorrupt, "another key")
			rec[len(rec)-trailerSize] ^= 1
			_, err = verifyRecord(rec, key)
			require.ErrorIs(t, err, ErrCorrupt, "a damaged trailer")
		}
	}
	_, err := verifyRecord(make([]byte, 4096), key)
	require.ErrorIs(t, err, ErrCorrupt, "zeros, as an unwritten record reads")
}

// eraseFooter zeroes the last page of a segment file, as if the segment had
// never been sealed.
func eraseFooter(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	info, err := f.Stat()
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, 4096), info.Size()-4096)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestFooterRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 170, 171, 3000} {
		entries := make([]footerEntry, n)
		for i := range entries {
			entries[i] = footerEntry{hash: HashKey([]byte(fmt.Sprint(i))), off: uint32(i) * 4096, size: 4096}
		}
		footer := make([]byte, footerSize(n))
		encodeSegmentFooter(footer, 7, entries)
		got, err := decodeSegmentFooter(7, footer)
		require.NoError(t, err)
		require.Equal(t, entries, got, "%d entries", n)

		_, err = decodeSegmentFooter(8, footer)
		require.Error(t, err, "footer names a different segment")
		if n > 0 {
			footer[0] ^= 1
			_, err = decodeSegmentFooter(7, footer)
			require.Error(t, err, "corrupt entry")
		}
	}
	_, err := footerEntryCount(7, make([]byte, 4096))
	require.Error(t, err, "a zeroed tail is not a footer")
}

// TestFooterLargerThanFirstRead covers a footer that does not fit the first
// read of the file's tail.
func TestFooterLargerThanFirstRead(t *testing.T) {
	entries := make([]footerEntry, 4000) // 96 KB of entries
	for i := range entries {
		entries[i] = footerEntry{hash: HashKey([]byte(fmt.Sprint(i))), off: uint32(i) * 4096, size: 4096}
	}
	size := int64(len(entries))*4096 + footerSize(len(entries))
	require.Greater(t, footerSize(len(entries)), int64(footerReadSize))
	footer := make([]byte, footerSize(len(entries)))
	encodeSegmentFooter(footer, 3, entries)
	path := t.TempDir() + "/00000003.seg"
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o644))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	_, err = f.WriteAt(footer, size-int64(len(footer)))
	require.NoError(t, err)

	got, err := readSegmentFooter(f, 3)
	require.NoError(t, err)
	require.Equal(t, entries, got)
}
