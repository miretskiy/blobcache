package blobstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireRings(t *testing.T, n int) {
	t.Helper()
	cfg := defaultConfig()
	cfg.rings = n
	if _, err := queueCPUs(cfg); err != nil {
		t.Skipf("need %d coordinator CPUs: %v", n, err)
	}
}

func writerKey(writer, writers, n int) string {
	for i := n; ; i++ {
		key := fmt.Sprintf("writer-%d-key-%d", writer, i)
		if HashKey([]byte(key)).Lo%uint64(writers) == uint64(writer) {
			return key
		}
	}
}

func TestMultiRingRoutingAndRecovery(t *testing.T) {
	requireRings(t, 3)
	for _, tc := range []struct {
		name             string
		rings, dedicated int
		budget           time.Duration
	}{
		{"shared", 3, 0, 1500 * time.Microsecond},
		{"one-writer", 3, 1, 1500 * time.Microsecond},
		{"two-writers", 3, 2, 1500 * time.Microsecond},
		{"unbudgeted", 3, 1, 0},
		{"larger-budget", 3, 2, 6 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			st := openStore(t, dir, recorder{}, WithRings(tc.rings), WithDedicatedWriteRings(tc.dedicated),
				WithMaxReadHandles(7), WithSegmentSize(64<<10), WithIOBudget(tc.budget))
			require.Len(t, st.queues, tc.rings)
			defer func() { require.NoError(t, st.Close()) }()
			var handles int
			for _, q := range st.readers {
				handles += len(q.reads.slots)
			}
			require.Equal(t, 7, handles, "handle budget is total, not per ring")
			if tc.dedicated != 0 {
				for _, q := range st.writers {
					require.Nil(t, q.reads)
				}
				for _, q := range st.readers {
					require.Nil(t, q.writeSlots)
				}
			}
			latest := make(map[string]Location)
			for round := 0; round < 12; round++ {
				for writer, q := range st.writers {
					key := writerKey(writer, len(st.writers), 0)
					value := bytes.Repeat([]byte{byte(round + writer)}, 20<<10)
					loc := writeRetry(t, st, key, value)
					st.segments.Lock()
					for _, s := range st.segments.fifo {
						if s.id == loc.segment {
							require.Same(t, q, s.owner)
						}
					}
					st.segments.Unlock()
					got, err := read(t, st, loc, key)
					require.NoError(t, err)
					require.Equal(t, value, got)
					r := st.readers[loc.segment%uint64(len(st.readers))].reads
					r.mu.Lock()
					require.Contains(t, r.byID, loc.segment)
					r.mu.Unlock()
					latest[key] = loc
				}
			}
			require.NoError(t, st.Close())
			// Reopen with a different queue count and role assignment. Neither
			// Location nor the disk format contains runtime scheduler ownership.
			recovered := recorder{}
			st = openStore(t, dir, recovered, WithRings(2), WithDedicatedWriteRings(1), WithSegmentSize(64<<10))
			for key, loc := range latest {
				require.Equal(t, loc, recovered[HashKey([]byte(key))], "latest overwrite survives")
				_, err := read(t, st, loc, key)
				require.NoError(t, err)
			}
			for key := range latest {
				loc := writeRetry(t, st, key, []byte("after restart"))
				require.Greater(t, loc.segment, latest[key].segment)
			}
		})
	}
}

func TestMultiRingPressureSealsQuietProducer(t *testing.T) {
	requireRings(t, 2)
	for _, limit := range []struct {
		name   string
		option Option
	}{
		{"segments", WithMaxSegments(4)}, {"bytes", WithMaxSize(4 * 64 << 10)},
	} {
		t.Run(limit.name, func(t *testing.T) {
			var events atomic.Int64
			var st *Store
			st = openStore(t, t.TempDir(), recorder{}, WithRings(2), WithSegmentSize(64<<10), limit.option,
				WithEvictionCallback(func(ids Eviction) {
					// Notifications run with neither common nor producer locks held.
					st.segments.Lock()
					if st.segments.nextID <= slices.Max(ids) {
						t.Error("retired an unallocated ID")
					}
					st.segments.Unlock()
					for _, q := range st.writers {
						q.active.Lock()
						if s := q.active.seg; s != nil {
							if _, retired := slices.BinarySearch(ids, s.id); retired {
								t.Error("retired an active segment")
							}
						}
						q.active.Unlock()
					}
					if !slices.IsSorted(ids) {
						t.Error("unordered eviction batch")
					}
					events.Add(1)
				}))
			defer func() { require.NoError(t, st.Close()) }()
			cold := writerKey(0, 2, 0)
			loc := writeRetry(t, st, cold, []byte("quiet producer"))
			hot := writerKey(1, 2, 0)
			for i := 0; i < 24; i++ {
				writeRetry(t, st, hot, bytes.Repeat([]byte{byte(i)}, 20<<10))
				if limit.name == "segments" {
					require.LessOrEqual(t, st.Stats().Segments, 4)
				} else {
					require.LessOrEqual(t, st.Stats().DiskBytes, int64(4*64<<10))
				}
			}
			require.Positive(t, events.Load())
			_, err := read(t, st, loc, cold)
			require.ErrorIs(t, err, ErrNotFound)
			require.Zero(t, st.Stats().FailedSegments)
			// A pressure-sealed producer can start a new segment normally.
			next := writeRetry(t, st, cold, []byte("awake"))
			require.Greater(t, next.segment, loc.segment)
		})
	}
}

func TestMultiRingConcurrentCapacity(t *testing.T) {
	requireRings(t, 3)
	for _, dedicated := range []int{0, 2} {
		t.Run(fmt.Sprintf("dedicated-%d", dedicated), func(t *testing.T) {
			st := openStore(t, t.TempDir(), recorder{}, WithRings(3), WithDedicatedWriteRings(dedicated),
				WithSegmentSize(64<<10), WithMaxSegments(8), WithMaxReadHandles(5))
			defer func() { require.NoError(t, st.Close()) }()
			var wg sync.WaitGroup
			for worker := 0; worker < 8; worker++ {
				wg.Go(func() {
					key := []byte(fmt.Sprintf("worker-%d", worker))
					buf := allocRecord(st.RecordSize(len(key), 20<<10))
					for i := 0; i < 30; i++ {
						value := bytes.Repeat([]byte{byte(worker*30 + i)}, 20<<10)
						copy(buf, value)
						deadline := time.Now().Add(10 * time.Second)
						var ticket Ticket
						var err error
						for {
							ticket, err = st.Write(key, buf, len(value))
							if !errors.Is(err, ErrBusy) || time.Now().After(deadline) {
								break
							}
							time.Sleep(time.Millisecond)
						}
						if err != nil {
							t.Error(err)
							return
						}
						loc, err := ticket.Wait()
						if err != nil {
							t.Error(err)
							return
						}
						got, err := st.Read(loc, key, allocRecord(loc.Size()))
						if err == nil {
							if !bytes.Equal(got, value) {
								t.Error("read another producer's data")
								return
							}
						} else if !errors.Is(err, ErrBusy) && !errors.Is(err, ErrNotFound) {
							t.Error(err)
							return
						}
						if st.Stats().Segments > 8 {
							t.Error("global segment budget exceeded")
							return
						}
					}
				})
			}
			wg.Wait()
			require.Positive(t, st.Stats().EvictedSegments)
			require.Zero(t, st.Stats().FailedSegments)
		})
	}
}

func TestMultiRingConfiguration(t *testing.T) {
	for _, opts := range [][]Option{
		{WithRings(0)}, {WithRings(2), WithDedicatedWriteRings(2)},
		{WithDedicatedWriteRings(-1)}, {WithRings(2), WithMaxReadHandles(1)},
		{WithRings(2), WithMaxSegments(3)}, {WithRings(2), WithMaxSize(3 * testSegment)},
		{WithMaxSegments(2), WithMaxSize(2 * testSegment)},
		{WithRings(2), WithCoordinatorCPUs(0)}, {WithRings(2), WithCoordinatorCPUs(0, 0)},
		{WithCoordinatorCPUs(-1)},
		{WithIOBudget(-time.Millisecond)},
	} {
		_, err := Open(t.TempDir(), append([]Option{WithSegmentSize(testSegment)}, opts...)...)
		require.Error(t, err)
	}
}
