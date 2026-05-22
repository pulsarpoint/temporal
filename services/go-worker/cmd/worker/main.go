package main

import (
	"context"
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
	corpscoutDB := os.Getenv("CORPSCOUT_DB_URL")
	if corpscoutDB == "" {
		slog.Error("CORPSCOUT_DB_URL is required")
		os.Exit(1)
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, corpscoutDB)
	if err != nil {
		slog.Error("connect to corpscout db", "error", err)
		os.Exit(1)
	}
	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping corpscout db", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("connected to corpscout DB")

	c, err := client.Dial(client.Options{
		HostPort:  temporalHost,
		Namespace: "corpscout",
	})
	if err != nil {
		slog.Error("connect to temporal", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	goActs := activities.NewGoActivities(pool)

	w := worker.New(c, "corpscout-pipelines", worker.Options{})

	w.RegisterWorkflow(workflows.PullCompaniesHouse)
	w.RegisterWorkflow(workflows.PullBrreg)
	w.RegisterWorkflow(workflows.PullGLEIF)
	w.RegisterWorkflow(workflows.PullAriregister)
	w.RegisterWorkflow(workflows.PullCVR)
	w.RegisterWorkflow(workflows.SyncCompaniesHouseSICCodes)
	w.RegisterWorkflow(workflows.EnrichCompanyDomains)
	w.RegisterWorkflow(workflows.TranslateSourceRawInputs)
	w.RegisterWorkflow(workflows.TranslateBrregRawInputs)

	w.RegisterActivity(goActs.WriteRawInputs)
	w.RegisterActivity(goActs.ImportBrregBulk)
	w.RegisterActivity(goActs.ImportGLEIFGoldenCopy)
	w.RegisterActivity(goActs.ImportAriregisterBulk)
	w.RegisterActivity(goActs.ImportCVRBulk)
	w.RegisterActivity(goActs.ImportCompaniesHouseSICCodes)
	w.RegisterActivity(goActs.MarkExecutionComplete)
	w.RegisterActivity(goActs.SaveSyncCheckpoint)
	w.RegisterActivity(goActs.FilterForDomainDiscovery)
	w.RegisterActivity(goActs.WriteDiscoveredDomains)
	w.RegisterActivity(goActs.MarkDomainsSearched)
	w.RegisterActivity(goActs.PrepareBrregTranslationBatch)
	w.RegisterActivity(goActs.WriteBrregTranslationBatch)
	w.RegisterActivity(goActs.PrepareSourceTranslationBatch)
	w.RegisterActivity(goActs.WriteSourceTranslationBatch)

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
