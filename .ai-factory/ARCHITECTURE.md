# Architecture: Layered Architecture with Ports & Adapters

## Overview

This package is organized as a **Layered Architecture** with a single **port** at the
database-vendor boundary. Console commands sit at the top and orchestrate a sync run;
services below them implement the sync algorithms; and every vendor-specific detail —
PostgreSQL catalog queries, DDL strings, `pg_dump`/`psql` process calls — lives behind
`DatabaseAdapterInterface` and is supplied by an adapter.

The package has no business domain in the DDD sense: there are no entities with
invariants, no aggregates, no domain events. What it has is an *algorithm* (synchronize
one database into another safely) and one volatile dependency (the database vendor).
Layered structure keeps the algorithm readable; the port keeps the vendor swappable and
the algorithm unit-testable without a live database.

## Decision Rationale

- **Project type:** public Composer library (Laravel package), CLI-only, no HTTP or UI
- **Tech stack:** PHP 8.2+, `illuminate/*` (Laravel 10–13), PostgreSQL, `symfony/process`
- **Team size:** 1 maintainer
- **Domain complexity:** low — no business rules, one technical workflow
- **Key factor:** exactly one axis of variation (the database vendor) justifies exactly one
  port. Full Explicit Architecture would add `Domain/`, `Application/`, and `Presentation/`
  folders with nothing meaningful to put in them.

## Folder Structure

Documented as it exists today — the layout is library-shaped, not application-shaped, so
there is no `Controllers/`, `Models/`, `Repositories/`, or `Routes/`.

```
src/
├── DbSyncServiceProvider.php        # ── COMPOSITION ROOT ──
│                                    # Merges/publishes config, registers Artisan commands
│
├── Console/                         # ── PRESENTATION (inbound adapter: CLI) ──
│   ├── BaseDbSyncCommand.php        # Shared setup: config → SyncConfig, service wiring,
│   │                                # tunnel handling, query-log pause
│   ├── CloneCommand.php             # db-sync:clone   — full refresh
│   ├── PullCommand.php              # db-sync:pull    — incremental sync
│   ├── RestoreCommand.php           # db-sync:restore — restore from backup
│   └── Traits/                      # Cross-cutting command behavior
│       ├── ManagesMemoryLimit.php
│       └── ManagesSignals.php
│
├── Services/                        # ── APPLICATION (sync algorithms) ──
│   ├── SchemaManager.php            # Structure reconciliation across phases
│   ├── DataSyncer.php               # Batching, keyset pagination, insert/upsert
│   ├── StructureDiff.php            # Pure diff functions (static, no I/O)
│   ├── DependencyGraph.php          # FK topological ordering
│   └── BackupManager.php            # Backup creation, retention, restore orchestration
│
├── Contracts/                       # ── PORT ──
│   └── DatabaseAdapterInterface.php # The single database-vendor seam
│
├── Adapters/                        # ── INFRASTRUCTURE (outbound adapter) ──
│   └── PgsqlAdapter.php             # PostgreSQL catalogs, DDL, pg_dump/psql via Process
│
├── DTO/                             # ── SHARED VALUE OBJECTS ──
│   ├── SyncConfig.php               # final readonly, built via fromArray()
│   ├── SyncPlan.php
│   └── TableDiff.php
│
├── Enums/
│   └── SyncMode.php                 # refresh | incremental
│
└── Exceptions/
    ├── DbSyncException.php          # extends RuntimeException — the package root
    ├── SyncException.php
    └── AdapterException.php

config/
└── db-sync.php                      # Publishable config: connections, batch size, backup

tests/
├── TestCase.php                     # Testbench base — registers provider, 'test' connection
├── Unit/                            # Pure-logic tests, no live database
└── fixtures/                        # SQL schema fixtures and dumps
```

## Dependency Rules

```
DbSyncServiceProvider  (composition root — wires everything)
        │
        ▼
    Console/  ──────────────►  Services/  ──────────────►  Contracts/
        │                          │                            ▲
        │                          │                            │
        └──────────────────────────┴──────────► DTO/ ───────────┤
                                                Enums/          │
                                             Exceptions/        │
                                                                │
                                              Adapters/  ───────┘
                                              (implements the port)
```

- ✅ `Console/` → `Services/`, `Contracts/`, `DTO/`, `Enums/`, `Exceptions/`
- ✅ `Services/` → `Contracts/`, `DTO/`, `Enums/`, `Exceptions/`
- ✅ `Adapters/` → `Contracts/`, `DTO/`, `Exceptions/`, and vendor libraries (`illuminate/database`, `symfony/process`)
- ✅ `DTO/`, `Enums/`, `Exceptions/` → nothing inside the package (leaf modules)
- ❌ `Services/` → `Adapters/` — services must type-hint `DatabaseAdapterInterface`, never `PgsqlAdapter`
- ❌ `Adapters/` → `Services/` or `Console/` — the adapter never calls back upward
- ❌ `Services/` or `Console/` containing raw PostgreSQL SQL, `pg_*` catalog queries, or DDL strings
- ❌ `Console/` reaching into vendor process calls directly (`pg_dump`, `psql`) — go through the adapter
- ❌ Any layer instantiating `PgsqlAdapter` except the composition root (`BaseDbSyncCommand::resolveAdapter()`)

## Layer/Module Communication

- **Constructor injection everywhere.** Services receive the adapter (and each other) as
  constructor arguments; nothing resolves dependencies from the container mid-flight.
- **The command is the composition root in practice.** `BaseDbSyncCommand::initializeSync()`
  resolves the adapter and builds `DependencyGraph`, `DataSyncer`, `SchemaManager`, and
  `BackupManager` once per run.
- **DTOs cross layers, arrays do not.** Configuration enters as an array and is immediately
  normalized into `SyncConfig` via `SyncConfig::fromArray()`. Downstream layers consume the
  DTO.
- **Structured arrays for tabular results.** Per-table results travel as documented array
  shapes (`array{inserted: int, updated: int, errors: int, error_messages: array}`) rather
  than exceptions, so a long sync can report a full tally at the end.
- **Progress reporting is passed down, not reached for.** Services accept an optional
  `OutputInterface` / `ProgressBar`; they never touch a global console facade.
- **Failures escalate as exceptions only when fatal.** Row- and batch-level failures are
  counted; configuration and connectivity failures throw `DbSyncException`.

## Key Principles

1. **One port, one reason.** `DatabaseAdapterInterface` exists because the database vendor
   is the only thing that could plausibly change. Do not introduce ports for things with a
   single stable implementation (filesystem, console output).
2. **Vendor knowledge is quarantined.** If a change requires knowing that the database is
   PostgreSQL, it belongs in `Adapters/PgsqlAdapter.php`. This is the rule that keeps a
   future MySQL adapter feasible.
3. **Commands orchestrate, services compute.** A command decides *what happens in what
   order* and talks to the user; it does not implement batching, diffing, or ordering.
4. **Pure logic stays pure and static.** `StructureDiff` and the DDL-string builders take
   arrays in and return arrays/strings out with no connection dependency — that is what
   makes the unit suite viable without a live database.
5. **Phases are explicit.** A sync run is structure → data → post-data (indexes,
   constraints, sequences). Ordering bugs in this package are almost always phase-ordering
   bugs; keep phase boundaries visible in `SchemaManager` rather than implicit in call order.
6. **DTOs are `final readonly`.** Configuration and plans are immutable once built; no
   setters, no mutation mid-sync.
7. **Compatibility floor is architectural.** PHP 8.2 and Laravel 10 constrain what APIs the
   layers may use; guard optional integrations (Telescope) with `class_exists()`.

## Code Organization Note

- **New Features:** All new code should follow the architecture defined in this document
  where practical.
- **Existing Code:** The structure above documents the current layout as-is. When modifying
  existing code, prefer following these conventions, but do not force a rewrite of unrelated
  code.
- **Interoperability:** When new code must call existing code, prefer clean interfaces but do
  not refactor purely for structural alignment.

## Code Examples

### Port and adapter — services depend on the interface, never the implementation

```php
// ── PORT: src/Contracts/DatabaseAdapterInterface.php ──
interface DatabaseAdapterInterface
{
    public function getIndexMap(Connection $connection): array;

    public function createIndexOrConstraintSql(
        string $table,
        string $name,
        string $type,
        string $def,
    ): string;
}

// ── ADAPTER: src/Adapters/PgsqlAdapter.php — all vendor knowledge lives here ──
class PgsqlAdapter implements DatabaseAdapterInterface
{
    public function getIndexMap(Connection $connection): array
    {
        // PostgreSQL catalog query — correct here, forbidden in Services/ or Console/
        return $connection->select('SELECT ... FROM pg_constraint ...');
    }

    public function createIndexOrConstraintSql(
        string $table,
        string $name,
        string $type,
        string $def,
    ): string {
        return $type === 'index'
            ? $def
            : "ALTER TABLE \"{$table}\" ADD CONSTRAINT \"{$name}\" {$def}";
    }
}

// ── SERVICE: src/Services/SchemaManager.php — depends on the port only ──
class SchemaManager
{
    public function __construct(
        protected DatabaseAdapterInterface $adapter,   // ✅ interface
        protected DependencyGraph $dependencyGraph,
    ) {}
}
```

### Composition root — the only place the concrete adapter is named

```php
// src/Console/BaseDbSyncCommand.php
protected function initializeSync(): void
{
    $connectionName = $this->option('sync-connection')
        ?? config('db-sync.default', 'production');

    $connectionConfig = config("db-sync.connections.{$connectionName}");

    if (!$connectionConfig) {
        throw new DbSyncException(
            "db-sync connection configuration '{$connectionName}' not found"
        );
    }

    // Array → DTO at the boundary; downstream layers never see the raw array
    $this->syncConfig = SyncConfig::fromArray($connectionName, $connectionConfig);

    $this->adapter = $this->resolveAdapter();          // ← the single `new PgsqlAdapter`
    $this->dependencyGraph = new DependencyGraph($this->adapter);
    $this->dataSyncer = new DataSyncer($this->adapter, $this->output);
    $this->schemaManager = new SchemaManager($this->adapter, $this->dependencyGraph);
    $this->backupManager = new BackupManager($this->adapter);
}
```

### Pure logic — testable without a database

```php
// src/Services/StructureDiff.php — static, no Connection, no adapter
final class StructureDiff
{
    /**
     * Tables present in target but absent from source.
     *
     * @return list<string>
     */
    public static function localOnlyTables(array $targetTables, array $sourceTables): array
    {
        return array_values(array_diff($targetTables, $sourceTables));
    }
}

// tests/Unit/StructureDiffTest.php — plain arrays in, plain arrays out
public function test_local_only_tables_returns_target_minus_source(): void
{
    $result = StructureDiff::localOnlyTables(
        ['users', 'orders', 'registration_request_logs'],
        ['users', 'orders', 'verification_request_logs'],
    );

    $this->assertSame(['registration_request_logs'], $result);
}
```

### Failure handling — count per-row errors, throw only on fatal conditions

```php
// Batch-level: accumulate, keep going, report at the end
$stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

try {
    $target->table($table)->insert($rows);
    $stats['inserted'] += count($rows);
} catch (\Throwable $e) {
    $stats['errors'] += count($rows);
    $stats['error_messages'][] = $e->getMessage();      // ✅ counted, not thrown
}

// Fatal: configuration or connectivity — stop the run
throw new SyncException("target connection '{$name}' is not configured");
```

## Anti-Patterns

- ❌ **Vendor SQL outside the adapter** — a `pg_constraint` query or a `CREATE INDEX` string
  built inside `Services/` or `Console/`. This is the single most damaging violation here:
  it silently makes the port a lie.
- ❌ **Type-hinting `PgsqlAdapter`** in a service or command instead of
  `DatabaseAdapterInterface`.
- ❌ **Fat command** — a `Command` that batches rows, walks the FK graph, or builds DDL
  itself instead of delegating to a service.
- ❌ **Throwing on every row error** — aborting a multi-hour sync on one bad row instead of
  counting it into `$stats['error_messages']`.
- ❌ **`OFFSET` pagination on a live table** — re-reads and gaps produce duplicate-key errors
  or silent data loss. Use keyset pagination (`ORDER BY pk`, `WHERE pk > $lastId`).
- ❌ **Ignoring the bind-parameter cap** — multi-row `INSERT` must stay under PostgreSQL's
  65535 placeholder limit; compute the effective batch size from the column count.
- ❌ **Mutable configuration** — adding setters to `SyncConfig` or mutating a DTO mid-run.
- ❌ **Container lookups inside services** — calling `app()` or a facade instead of taking
  the dependency in the constructor. It breaks unit testing and hides the wiring.
- ❌ **Speculative ports** — inventing `FilesystemInterface` or `OutputInterface` wrappers
  with exactly one implementation and no swap in sight.
