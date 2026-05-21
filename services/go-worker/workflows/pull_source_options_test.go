package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSourceDownloadActivityOptionsDoNotRequireHeartbeats(t *testing.T) {
	options := sourceDownloadActivityOptions()

	require.Equal(t, 20*time.Minute, options.StartToCloseTimeout)
	require.Zero(t, options.HeartbeatTimeout)
	require.NotNil(t, options.RetryPolicy)
}

func TestSourceFilePageCountReturnsZeroForNoFiles(t *testing.T) {
	require.Zero(t, sourceFilePageCount(nil))
}
