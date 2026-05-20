package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/cache"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

func main() {
	temporalHost := getEnv("TEMPORAL_HOST", "localhost:7233")
	outputDir := getEnv("OUTPUT_DIR", "/var/lib/data-pipelines/results")
	corpscoutDB := os.Getenv("CORPSCOUT_DB_URL") // optional — omit to run in file-only mode

	ctx := context.Background()

	var pool *pgxpool.Pool
	if corpscoutDB != "" {
		var err error
		pool, err = pgxpool.New(ctx, corpscoutDB)
		if err != nil {
			slog.Error("connect to corpscout db", "error", err)
			os.Exit(1)
		}
		if err := pool.Ping(ctx); err != nil {
			slog.Error("ping corpscout db", "error", err)
			os.Exit(1)
		}
		defer pool.Close()
		slog.Info("database mode: writing records to corpscout DB")
	} else {
		slog.Info("file-only mode: writing records to output directory", "output_dir", outputDir)
	}

	enrichCache, err := cache.New(filepath.Join(outputDir, "cache.db"))
	if err != nil {
		slog.Error("open enrichment cache", "error", err)
		os.Exit(1)
	}
	defer enrichCache.Close()
	slog.Info("enrichment cache opened", "path", filepath.Join(outputDir, "cache.db"))

	c, err := client.Dial(client.Options{
		HostPort:  temporalHost,
		Namespace: "corpscout",
	})
	if err != nil {
		slog.Error("connect to temporal", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	goActs := activities.NewGoActivities(pool, outputDir, enrichCache)

	w := worker.New(c, "corpscout-pipelines", worker.Options{})

	// Register one workflow per source.
	w.RegisterWorkflow(workflows.PullCompaniesHouse)
	w.RegisterWorkflow(workflows.PullBrreg)

	// Shared Go activities used by all source workflows.
	w.RegisterActivity(goActs.WriteRawInputs)
	w.RegisterActivity(goActs.FilterForEnrichment)
	w.RegisterActivity(goActs.MarkEnriched)
	w.RegisterActivity(goActs.MarkExecutionComplete)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	if err := w.Start(); err != nil {
		slog.Error("start temporal worker", "error", err)
		os.Exit(1)
	}
	slog.Info("temporal Go worker started", "task_queue", "corpscout-pipelines", "host", temporalHost)

	<-stop
	slog.Info("shutting down Go worker")
	w.Stop()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
