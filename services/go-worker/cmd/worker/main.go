package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/data-pipelines/activities"
	"github.com/pulsarpoint/data-pipelines/workflows"
)

func main() {
	temporalHost := getEnv("TEMPORAL_HOST", "localhost:7233")
	corpscoutDB := mustEnv("CORPSCOUT_DB_URL")
	outputDir := getEnv("OUTPUT_DIR", "/var/lib/data-pipelines/results")

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, corpscoutDB)
	if err != nil {
		slog.Error("connect to corpscout db", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping corpscout db", "error", err)
		os.Exit(1)
	}

	c, err := client.Dial(client.Options{
		HostPort:  temporalHost,
		Namespace: "corpscout",
	})
	if err != nil {
		slog.Error("connect to temporal", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	goActs := activities.NewGoActivities(pool, outputDir)

	w := worker.New(c, "corpscout-pipelines", worker.Options{})
	w.RegisterWorkflow(workflows.PullCompanies)
	w.RegisterActivity(goActs.WriteRawInputs)
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

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var not set: %s", key))
	}
	return v
}
