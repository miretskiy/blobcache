package blobstore

import (
	"errors"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestSchedulerConfiguration(t *testing.T) {
	for _, opts := range [][]Option{
		{WithSchedulerCPUs()},
		{WithSchedulerCPUs(-2)},
		{WithWriteSchedulers(-1)},
		{WithWriteSchedulers(1)},
		{WithSchedulerCPUs(0, 1), WithWriteSchedulers(2)},
	} {
		cfg := defaultConfig()
		for _, opt := range opts {
			opt.apply(&cfg)
		}
		require.Error(t, cfg.validate())
	}
	cpus := []int{2, 3, -1}
	option := WithSchedulerCPUs(cpus...)
	cpus[0] = -2
	cfg := defaultConfig()
	option.apply(&cfg)
	WithWriteSchedulers(2).apply(&cfg)
	require.NoError(t, cfg.validate())
	require.Equal(t, []int{2, 3, -1}, cfg.schedulerCPUs, "option owns the CPU list")
	for _, opt := range []Option{WithIOBudget(-1), WithWriteBacklogLimit(-1),
		WithWriteBacklogRecovery(-5), WithReadBacklogLimit(-1)} {
		opt.apply(&cfg)
	}
	require.NoError(t, cfg.validate())
	require.Zero(t, cfg.ioBudget)
	require.Zero(t, cfg.writeBacklog)
	require.Zero(t, cfg.readBacklog)
	require.EqualValues(t, -1, cfg.writeBacklogRecovery)
}

func TestSchedulerWriteBudget(t *testing.T) {
	cfg := defaultConfig()
	WithSchedulerCPUs(-1, -1, -1, -1).apply(&cfg)
	WithWriteSchedulers(1).apply(&cfg)
	WithIOBudget(time.Millisecond).apply(&cfg)
	require.NoError(t, cfg.validate())
	require.Equal(t, time.Millisecond, cfg.schedulerWriteBudget(0))
	for i := 1; i < 4; i++ {
		require.Zero(t, cfg.schedulerWriteBudget(i))
	}
	WithIOBudget(0).apply(&cfg)
	require.Zero(t, cfg.schedulerWriteBudget(0))
}

type closeTrackingScheduler struct {
	iosched.Scheduler
	closes int
	err    error
}

func (s *closeTrackingScheduler) Close() error {
	s.closes++
	return s.err
}

func TestCloseAllSchedulersDespiteError(t *testing.T) {
	failure := errors.New("close failed")
	first := &closeTrackingScheduler{err: failure}
	second := &closeTrackingScheduler{}
	st := &Store{schedulers: []iosched.Scheduler{first, second}}
	require.ErrorIs(t, st.Close(), failure)
	require.Equal(t, 1, first.closes)
	require.Equal(t, 1, second.closes)
	require.NoError(t, st.Close())
	require.Equal(t, 1, first.closes)
	require.Equal(t, 1, second.closes)
}
