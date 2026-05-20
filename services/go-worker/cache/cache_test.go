package cache_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/data-pipelines/cache"
)

func newTempCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.New(filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestFilterUnfetched_NeverSeen(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001", "AA002", "AA003"}
	out, err := c.FilterUnfetched("companies_house", ids, 0)
	require.NoError(t, err)
	require.ElementsMatch(t, ids, out)
}

func TestFilterUnfetched_AlreadyFresh(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001", "AA002"}
	require.NoError(t, c.MarkFetched("companies_house", ids))

	out, err := c.FilterUnfetched("companies_house", ids, 0)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestFilterUnfetched_PartiallyFresh(t *testing.T) {
	c := newTempCache(t)
	require.NoError(t, c.MarkFetched("companies_house", []string{"AA001"}))

	out, err := c.FilterUnfetched("companies_house", []string{"AA001", "AA002"}, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"AA002"}, out)
}

func TestFilterUnfetched_StaleExceedsCustomTTL(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001"}
	require.NoError(t, c.MarkFetched("companies_house", ids))

	// Use a 1-nanosecond TTL — the entry was just written so it should be "stale"
	// from the perspective of an extremely short TTL.
	out, err := c.FilterUnfetched("companies_house", ids, 1*time.Nanosecond)
	require.NoError(t, err)
	require.Equal(t, ids, out)
}

func TestFilterUnfetched_SourceIsolation(t *testing.T) {
	c := newTempCache(t)
	require.NoError(t, c.MarkFetched("companies_house", []string{"AA001"}))

	// Same ID under a different source should still need fetching.
	out, err := c.FilterUnfetched("brreg", []string{"AA001"}, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"AA001"}, out)
}

func TestMarkFetched_Idempotent(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001"}
	require.NoError(t, c.MarkFetched("companies_house", ids))
	require.NoError(t, c.MarkFetched("companies_house", ids))

	out, err := c.FilterUnfetched("companies_house", ids, 0)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestFilterUnfetched_Empty(t *testing.T) {
	c := newTempCache(t)
	out, err := c.FilterUnfetched("companies_house", nil, 0)
	require.NoError(t, err)
	require.Nil(t, out)
}

func TestNew_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	c1, err := cache.New(path)
	require.NoError(t, err)
	require.NoError(t, c1.MarkFetched("companies_house", []string{"AA001"}))
	require.NoError(t, c1.Close())

	// Reopen and verify entry survives.
	c2, err := cache.New(path)
	require.NoError(t, err)
	defer c2.Close()
	out, err := c2.FilterUnfetched("companies_house", []string{"AA001"}, 0)
	require.NoError(t, err)
	require.Empty(t, out)
}

func init() {
	// Ensure test binary can find the sqlite library (CGO_ENABLED=0 path).
	_ = os.Getenv("TMPDIR")
}
