package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSourceDownloadActivityOptionsDoNotRequireHeartbeats(t *testing.T) {
	options := sourceDownloadActivityOptions()

	require.Equal(t, "corpscout-pipelines-python", options.TaskQueue)
	require.Equal(t, 20*time.Minute, options.StartToCloseTimeout)
	require.Zero(t, options.HeartbeatTimeout)
	require.NotNil(t, options.RetryPolicy)
	require.Equal(t, int32(3), options.RetryPolicy.MaximumAttempts)
	require.Equal(t, 15*time.Second, options.RetryPolicy.InitialInterval)
	require.Equal(t, 2*time.Minute, options.RetryPolicy.MaximumInterval)
	require.Equal(t, 2.0, options.RetryPolicy.BackoffCoefficient)
}

func TestSourceImportActivityOptionsAllowLargeFiles(t *testing.T) {
	options := sourceImportActivityOptions()

	require.Equal(t, "corpscout-pipelines", options.TaskQueue)
	require.Equal(t, time.Hour, options.StartToCloseTimeout)
	require.Equal(t, 2*time.Minute, options.HeartbeatTimeout)
	require.NotNil(t, options.RetryPolicy)
	require.Equal(t, int32(3), options.RetryPolicy.MaximumAttempts)
	require.Equal(t, 10*time.Second, options.RetryPolicy.InitialInterval)
}

func TestMarkCompleteActivityOptionsAreBounded(t *testing.T) {
	options := markCompleteActivityOptions()

	require.Equal(t, "corpscout-pipelines", options.TaskQueue)
	require.Equal(t, 2*time.Minute, options.StartToCloseTimeout)
	require.NotNil(t, options.RetryPolicy)
	require.Equal(t, int32(5), options.RetryPolicy.MaximumAttempts)
}

func TestSourceFilePageCountReturnsZeroForNoFiles(t *testing.T) {
	require.Zero(t, sourceFilePageCount(nil))
}
