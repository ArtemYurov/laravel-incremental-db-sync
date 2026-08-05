# Laravel Incremental DB Sync

## Overview

A Laravel package that synchronizes a PostgreSQL database from a remote server into a
local (or staging) database over an SSH tunnel. It is distributed as a public Composer
library (`artemyurov/laravel-incremental-db-sync`) and installed as a dev dependency in
consuming applications.

The package is a **library, not an application**: it ships no HTTP layer, no migrations,
and no UI — only Artisan commands, services, and a publishable config file.

## Core Features

- **Incremental sync** (`db-sync:pull`) — compares source and target structure, reconciles
  schema differences, and upserts changed rows without dropping the target database.
- **Full refresh** (`db-sync:clone`) — DROP + CREATE of tables followed by bulk insert;
  faster than incremental when the target is disposable.
- **Backup / restore** (`db-sync:restore`) — `pg_dump`-based backups with retention
  (`keep_last`) and restore from a named backup file.
- **Foreign-key dependency resolution** — tables are ordered parents-first via a dependency
  graph, so FK constraints hold during the data phase.
- **Self-referencing tables** — rows are inserted roots-first so an intra-table FK
  (e.g. `parent_id`) never points at a not-yet-inserted row.
- **Index and constraint lifecycle** — secondary unique indexes are dropped during the data
  phase and recreated afterwards, allowing value permutation mid-sync.
- **Views** — view definitions are dumped, diffed, and recreated alongside tables.
- **Keyset pagination + multi-row INSERT** — batches are sized against PostgreSQL's
  65535 bind-parameter limit; `ORDER BY pk, WHERE pk > last` avoids the duplicate/gap
  hazards of `OFFSET` on a live table.
- **Performance guards** — query log and Laravel Telescope recording are paused for the
  duration of a sync; memory limit and POSIX signals are managed explicitly.

## Tech Stack

- **Programming language:** PHP 8.2+ (`declare(strict_types=1)` everywhere)
- **Framework:** Laravel 10 / 11 / 12 / 13 (via `illuminate/*` components, not full framework)
- **Database:** PostgreSQL (the only adapter implemented; `pg_dump` / `psql` CLI required)
- **ORM / Query layer:** Illuminate Database query builder (no Eloquent models)
- **Process layer:** `symfony/process` for `pg_dump` / `psql` invocation
- **SSH tunnel:** `artemyurov/laravel-autossh-tunnel` (^0.5 | ^0.6) — required companion package
- **Testing:** PHPUnit 10/11/12 + Orchestra Testbench 9/10/11
- **Autoload:** PSR-4 — `ArtemYurov\DbSync\` → `src/`, `ArtemYurov\DbSync\Tests\` → `tests/`

## Architecture Notes

Layered by responsibility, with the database vendor isolated behind one interface:

- `Console/` — Artisan commands (`db-sync:clone`, `db-sync:pull`, `db-sync:restore`) sharing
  `BaseDbSyncCommand` plus traits for memory limit and signal handling. Commands orchestrate;
  they hold no vendor-specific SQL.
- `Contracts/DatabaseAdapterInterface` — the single seam for database vendors (~30 methods:
  structure introspection, DDL generation, dump/restore, upsert).
- `Adapters/PgsqlAdapter` — the only implementation; all PostgreSQL catalog queries
  (`pg_constraint`, `pg_indexes`, …) and `pg_dump`/`psql` shelling live here.
- `Services/` — stateless-ish orchestration: `SchemaManager` (structure reconciliation),
  `DataSyncer` (batching, keyset pagination, upsert), `StructureDiff` (pure diff functions),
  `DependencyGraph` (FK topological ordering), `BackupManager` (backup lifecycle).
- `DTO/` — `final readonly` value objects (`SyncConfig`, `SyncPlan`, `TableDiff`) built from
  the config array via named static factories.
- `Enums/SyncMode` — `refresh` | `incremental`.
- `Exceptions/` — `DbSyncException extends RuntimeException`, with `SyncException` and
  `AdapterException` narrowing it.

Sync runs in phases: **structure → data → post-data (indexes, constraints, sequences)**.
Several known issues (UNIQUE index creation, FK loss) stem from ordering within the post-data
phase — see `ROADMAP.md` and `docs/research/`.

## Architecture

See `.ai-factory/ARCHITECTURE.md` for detailed architecture guidelines: layer boundaries,
dependency rules, code examples, and anti-patterns.

Pattern: **Layered Architecture with Ports & Adapters**

## Non-Functional Requirements

- **Compatibility:** must keep working across Laravel 10–13 and PHP 8.2+; avoid APIs absent
  from the oldest supported line.
- **Data safety:** a sync must never leave the target in a silently partial state — failures
  surface as counted errors with messages, and destructive commands require `--force` or an
  interactive confirmation.
- **Backups:** `db-sync:clone` and `db-sync:pull` are destructive by nature; backup creation
  and retention are first-class, not optional afterthoughts.
- **Performance:** bulk paths must stay within PostgreSQL's bind-parameter limit and avoid
  per-row round trips where a batch will do; query recorders stay off during sync.
- **Security:** credentials come from env via `config/db-sync.php` and are never logged;
  database access is expected to run through the SSH tunnel, not a public port.
- **Observability:** progress bars with per-batch read/write timings; no logging framework
  dependency beyond Artisan console output.
