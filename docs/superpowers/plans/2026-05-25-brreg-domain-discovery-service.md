# BRREG Domain Discovery Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone BRREG domain discovery service and wire Dagster toward a single `domain_results` business artifact.

**Architecture:** The service owns search, crawl, LLM verification, scoring, and structured per-company errors. Dagster keeps queueing/retries/DB writes and stores one result payload per company rather than exposing search/crawl/verify as product states.

**Tech Stack:** Python 3.12, uv, FastAPI, httpx, json-repair, optional crawl4ai/Playwright runtime, PostgreSQL via existing Dagster store patterns.

---

### Task 1: Standalone Service

- [ ] Create `services/crawl-service` with uv.
- [ ] Add tests for existing website short-circuit, fake DuckDuckGo search, fake crawler, fake LLM verification, API response, real BRREG fixture loading, and optional real service stress.
- [ ] Implement focused modules: models, domain normalization, search, crawl, LLM verification, scoring/service orchestration, API.
- [ ] Add Dockerfile, Makefile, README, `.env`, `.dockerignore`, and GHCR workflow.

### Task 2: Dagster Wiring

- [ ] Add `dagster_brreg.domain_results` migration and tests.
- [ ] Add store methods for inserting and reading latest domain result artifacts.
- [ ] Add a crawl service client and `brreg_domain_results` asset.
- [ ] Keep existing old search/crawl tables for compatibility during transition; final enhanced payload can be switched in a follow-up once the new result table is populated.

### Task 3: Verification

- [ ] Run crawl service tests.
- [ ] Run Dagster targeted tests.
- [ ] Validate compose.
- [ ] Build crawl service Docker image.
