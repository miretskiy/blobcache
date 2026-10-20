package blobstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetSizeNormalization(t *testing.T) {
	for _, tc := range []struct{ requested, target int64 }{
		{-1, 64 << 10}, {0, 64 << 10}, {1, 64 << 10},
		{64 << 10, 64 << 10}, {64<<10 + 1, 68 << 10},
	} {
		t.Run(fmt.Sprint(tc.requested), func(t *testing.T) {
			cfg := defaultConfig()
			WithTargetSize(tc.requested).apply(&cfg)
			require.NoError(t, cfg.validate())
			require.Equal(t, tc.target, cfg.targetSize)
		})
	}
}
