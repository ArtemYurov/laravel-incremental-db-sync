# AGENTS.md

> Structural map of this repository for AI agents and new contributors.
> Keep it factual and update it when the project layout changes significantly.

## Project Overview

A Laravel package (public Composer library) that synchronizes a PostgreSQL database from a
remote server into a local or staging database over an SSH tunnel — incrementally or via a
full DROP + CREATE refresh, with backups and foreign-key-aware ordering.

## Tech Stack

- **Programming language:** PHP 8.2+ (`declare(strict_types=1)`)
- **Framework:** Laravel 10 / 11 / 12 / 13 (`illuminate/*` components)
- **Database:** PostgreSQL (`pg_dump` / `psql` CLI required)
- **ORM:** none — Illuminate query builder only, no Eloquent models
- **Testing:** PHPUnit + Orchestra Testbench

Full stack and feature detail: `.ai-factory/DESCRIPTION.md`.

## Project Structure

```
.
├── config/
│   └── db-sync.php               # Publishable package config (connections, batch size, backup)
├── src/
│   ├── DbSyncServiceProvider.php # Registers config + Artisan commands
│   ├── Console/                  # Artisan commands — orchestration only, no vendor SQL
│   │   └── Traits/               # ManagesMemoryLimit, ManagesSignals
│   ├── Contracts/                # DatabaseAdapterInterface — the database-vendor seam
│   ├── Adapters/                 # PgsqlAdapter — all PostgreSQL catalog queries and DDL
│   ├── Services/                 # SchemaManager, DataSyncer, StructureDiff,
│   │                             # DependencyGraph, BackupManager
│   ├── DTO/                      # final readonly value objects (SyncConfig, SyncPlan, TableDiff)
│   ├── Enums/                    # SyncMode: refresh | incremental
│   └── Exceptions/               # DbSyncException → SyncException, AdapterException
├── tests/
│   ├── TestCase.php              # Testbench base: registers provider, defines 'test' connection
│   ├── Unit/                     # Pure-logic tests (diff, graph, config, DDL builders)
│   └── fixtures/                 # SQL schema fixtures and local dumps
├── docs/                         # Detailed documentation and research notes
└── .ai-factory/                  # AI Factory context artifacts
```

Dependency direction: `Console → Services → Contracts ← Adapters`.
Services depend on `DatabaseAdapterInterface`, never on `PgsqlAdapter` directly.

## Key Entry Points

| File | Purpose |
|------|---------|
| `src/DbSyncServiceProvider.php` | Package bootstrap: merges config, publishes it, registers commands |
| `src/Console/BaseDbSyncCommand.php` | Shared command setup: config resolution, service wiring, tunnel, query-log pause |
| `src/Console/PullCommand.php` | `db-sync:pull` — incremental sync |
| `src/Console/CloneCommand.php` | `db-sync:clone` — full refresh (DROP + CREATE + bulk insert) |
| `src/Console/RestoreCommand.php` | `db-sync:restore` — restore from a backup file |
| `src/Contracts/DatabaseAdapterInterface.php` | The contract any new database vendor must implement |
| `src/Adapters/PgsqlAdapter.php` | PostgreSQL implementation — catalog queries, DDL, dump/restore |
| `config/db-sync.php` | Connection definitions, batch size, backup path and retention |
| `phpunit.xml.dist` | Test suite configuration (single `Unit` suite) |

## Documentation

| Document | Path | Description |
|----------|------|-------------|
| README | `README.md` | Installation, configuration, and command usage for package consumers |
| Roadmap | `ROADMAP.md` | Open work item: UNIQUE-index lifecycle during `db-sync:pull` |
| Architecture (rendered) | `docs/architecture.html` | Visual architecture overview |
| Refactoring plan | `docs/refactoring-plan.md` | Planned structural changes |
| Research: FK loss | `docs/research/foreign-key-loss.md` | Investigation into foreign keys lost during sync |
| Test fixtures | `tests/fixtures/README.md` | What the SQL fixtures represent and how to regenerate them |

## AI Context Files

| File | Purpose |
|------|---------|
| `AGENTS.md` | This file — structural map of the repository |
| `.ai-factory/config.yaml` | AI Factory settings: languages, paths, git behavior |
| `.ai-factory/DESCRIPTION.md` | Project specification: features, stack, architecture and non-functional notes |
| `.ai-factory/ARCHITECTURE.md` | Architecture pattern, layer boundaries, and dependency rules |
| `.ai-factory/rules/base.md` | Detected project conventions: naming, structure, error handling, testing |

## Agent Rules

- **Language split:** all files in this repository — code, comments, PHPDoc, docs, commit
  messages — are written in **English**. Conversation with the maintainer is in **Russian**.
- **No auto-push:** `/aif-commit` stops after the local commit; pushing is an explicit,
  separate request (`git.skip_push_after_commit: true`).
- **Vendor SQL stays in the adapter:** never put PostgreSQL catalog queries or DDL strings
  into `Services/` or `Console/`.
- **Compatibility floor:** PHP 8.2 and Laravel 10 — do not use newer APIs without a guard.
- **Companion package:** this package depends on `artemyurov/laravel-autossh-tunnel`; when
  both change, release autossh-tunnel first.
- **Decompose shell commands** instead of chaining them:
  - Incorrect: `git checkout main && git pull`
  - Correct: first `git checkout main`, then `git pull origin main`
