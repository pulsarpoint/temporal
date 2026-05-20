package cache_test

import (
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

// ── enrichment_cache ──────────────────────────────────────────────────────────

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

func TestFilterUnfetched_StaleExceedsCustomTTL(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001"}
	require.NoError(t, c.MarkFetched("companies_house", ids))

	// 1ns TTL — entry was just written so it's already "stale" at this resolution.
	out, err := c.FilterUnfetched("companies_house", ids, 1*time.Nanosecond)
	require.NoError(t, err)
	require.Equal(t, ids, out)
}

func TestFilterUnfetched_SourceIsolation(t *testing.T) {
	c := newTempCache(t)
	require.NoError(t, c.MarkFetched("companies_house", []string{"AA001"}))

	out, err := c.FilterUnfetched("brreg", []string{"AA001"}, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"AA001"}, out)
}

// ── domain_cache ──────────────────────────────────────────────────────────────

func TestFilterNeedsDiscovery_NeverSearched(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001", "AA002"}
	out, err := c.FilterNeedsDiscovery("companies_house", ids, false)
	require.NoError(t, err)
	require.ElementsMatch(t, ids, out)
}

func TestFilterNeedsDiscovery_AlreadySearched(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001", "AA002"}
	require.NoError(t, c.MarkDomainsSearched("companies_house", ids))

	out, err := c.FilterNeedsDiscovery("companies_house", ids, false)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestFilterNeedsDiscovery_PartiallySearched(t *testing.T) {
	c := newTempCache(t)
	require.NoError(t, c.MarkDomainsSearched("companies_house", []string{"AA001"}))

	out, err := c.FilterNeedsDiscovery("companies_house", []string{"AA001", "AA002"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"AA002"}, out)
}

func TestFilterNeedsDiscovery_ForceBypassesCache(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001", "AA002"}
	require.NoError(t, c.MarkDomainsSearched("companies_house", ids))

	// force=true returns all IDs regardless.
	out, err := c.FilterNeedsDiscovery("companies_house", ids, true)
	require.NoError(t, err)
	require.ElementsMatch(t, ids, out)
}

func TestFilterNeedsDiscovery_SourceIsolation(t *testing.T) {
	c := newTempCache(t)
	require.NoError(t, c.MarkDomainsSearched("companies_house", []string{"AA001"}))

	out, err := c.FilterNeedsDiscovery("brreg", []string{"AA001"}, false)
	require.NoError(t, err)
	require.Equal(t, []string{"AA001"}, out)
}

func TestMarkDomainsSearched_Idempotent(t *testing.T) {
	c := newTempCache(t)
	ids := []string{"AA001"}
	require.NoError(t, c.MarkDomainsSearched("companies_house", ids))
	require.NoError(t, c.MarkDomainsSearched("companies_house", ids))

	out, err := c.FilterNeedsDiscovery("companies_house", ids, false)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestCache_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	c1, err := cache.New(path)
	require.NoError(t, err)
	require.NoError(t, c1.MarkDomainsSearched("companies_house", []string{"AA001"}))
	require.NoError(t, c1.Close())

	c2, err := cache.New(path)
	require.NoError(t, err)
	defer c2.Close()

	out, err := c2.FilterNeedsDiscovery("companies_house", []string{"AA001"}, false)
	require.NoError(t, err)
	require.Empty(t, out)
}
