# Laravel Incremental DB Sync

Laravel package for incremental PostgreSQL database synchronization from remote servers via SSH tunnel.

Supports incremental sync, full refresh (DROP + CREATE), automatic backups, foreign key dependency resolution, and self-referencing table handling.

## Requirements

- PHP 8.2+
- Laravel 10, 11, 12, or 13
- PostgreSQL
- `pg_dump` and `psql` CLI tools available on the local machine
- [artemyurov/laravel-autossh-tunnel](https://github.com/artemyurov/laravel-autossh-tunnel) for SSH tunnel management

## Installation

```bash
composer require artemyurov/laravel-incremental-db-sync --dev
```

Publish the configuration file:

```bash
php artisan vendor:publish --tag=db-sync-config
```

## Configuration

The configuration file `config/db-sync.php` defines sync connections:

```php
return [
    'default' => env('DB_SYNC_CONNECTION', 'production'),
    'batch_size' => env('DB_SYNC_BATCH_SIZE', 10000),

    'backup' => [
        'path' => env('DB_SYNC_BACKUP_PATH', storage_path('app/db-sync/backups')),
        'keep_last' => env('DB_SYNC_BACKUP_KEEP_LAST', 5),
    ],

    'connections' => [
        'production' => [
            'tunnel' => env('DB_SYNC_TUNNEL', 'remote_db'),
            'source' => [
                'driver' => 'pgsql',
                'database' => env('DB_SYNC_REMOTE_DATABASE'),
                'username' => env('DB_SYNC_REMOTE_USERNAME'),
                'password' => env('DB_SYNC_REMOTE_PASSWORD'),
            ],
            'target' => env('DB_SYNC_TARGET_CONNECTION', 'pgsql'),
            'excluded_tables' => [
                'telescope_entries',
                'sessions',
                'cache',
                'jobs',
            ],
        ],
    ],
];
```

Each connection defines:

| Key | Description |
|-----|-------------|
| `tunnel` | SSH tunnel name from `config/tunnel.php` ([laravel-autossh-tunnel](https://github.com/artemyurov/laravel-autossh-tunnel)) |
| `source` | Remote database credentials (driver, database, username, password) |
| `target` | Local database connection name from `config/database.php` |
| `excluded_tables` | Tables to skip during synchronization |

You can define multiple connections (e.g. `production`, `staging`) and switch between them using the `--sync-connection` option.

## Commands

### `db-sync:pull` — Incremental Synchronization

Analyzes differences between remote and local databases, then applies only the changes (DELETE + UPSERT).

```bash
php artisan db-sync:pull
```

Features:
- Compares record counts and `updated_at` timestamps to detect changes
- Automatically rebuilds tables with changed structure
- Reconciles indexes and constraints to match remote (see *Index/constraint reconciliation* below)
- Resolves foreign key dependencies for correct sync order
- CASCADE RECHECK: re-syncs child tables after parent deletions
- Syncs views and resets sequences

> **Index/constraint reconciliation.** `pull`'s structure detection compares columns
> only, so index/constraint changes (including renames) would otherwise be invisible.
> Each run performs a dedicated reconciliation pass: it compares indexes and
> constraints of every table (by name) between remote and local and brings local in
> line — constraint-aware (PRIMARY KEY / UNIQUE / EXCLUSION via `ALTER TABLE`, plain
> indexes via `CREATE`/`DROP INDEX`), applying all drops before all creates so renamed
> objects don't collide. Excluded tables are skipped. Removed objects are reported
> afterwards. Note: `pull` never drops local-only **tables** (it preserves work in
> progress), but it **does** drop indexes/constraints that exist locally and not on
> remote — including a locally-added index from a not-yet-pushed migration. DDL is
> owned by migrations; this pass only keeps index/constraint metadata in sync.

Options:

| Option | Description |
|--------|-------------|
| `--sync-connection=` | Connection name from config (default: `production`) |
| `--tables=` | Sync only specified tables (comma-separated) |
| `--exclude=` | Exclude specified tables (comma-separated) |
| `--views=` | Sync only specified views (comma-separated) |
| `--include-excluded` | Include normally excluded tables |
| `--analyze-only` | Only show analysis, don't sync |
| `--dry-run` | Show plan without executing |
| `--skip-backup` | Skip automatic backup |
| `--skip-sequences` | Skip sequence reset |
| `--batch-size=` | Records per batch (overrides config `db-sync.batch_size` / `DB_SYNC_BATCH_SIZE`, default 10000) |
| `--memory-limit=-1` | Memory limit in MB |
| `--force` | Skip confirmation prompt |

### `db-sync:clone` — Full Clone

Drops all tables and recreates them from the remote server. Use this for a clean start.

```bash
php artisan db-sync:clone
```

> **Local-only tables.** `clone` mirrors the remote schema, so tables that exist
> locally but **not** on remote (e.g. leftovers from an interrupted run, or a renamed
> table whose old index name lingers) must be removed first — otherwise their index
> and constraint names collide with recreated tables (`SQLSTATE[42P07] ... already
> exists`). Such local-only tables are listed in the plan (with row counts) alongside
> the tables to refresh, and are dropped as part of the **single** confirmation prompt
> (no separate question). Pass `--keep-local-tables` to keep them (only safe when their
> names don't clash with remote objects).

Options:

| Option | Description |
|--------|-------------|
| `--sync-connection=` | Connection name from config |
| `--tables=` | Refresh only specified tables (comma-separated) |
| `--exclude=` | Exclude specified tables (comma-separated) |
| `--views=` | Refresh only specified views (comma-separated) |
| `--include-excluded` | Include normally excluded tables |
| `--dry-run` | Show plan without executing |
| `--skip-views` | Skip view synchronization |
| `--skip-backup` | Skip automatic backup |
| `--skip-sync-data` | Refresh structure only, no data |
| `--keep-local-tables` | Do not drop local-only tables (tables not present on remote) |
| `--batch-size=` | Records per batch (overrides config `db-sync.batch_size` / `DB_SYNC_BATCH_SIZE`, default 10000) |
| `--memory-limit=-1` | Memory limit in MB |
| `--force` | Skip confirmation prompt (and drop local-only tables without asking) |

### `db-sync:restore` — Restore from Backup

Restore local database from a previously created backup.

```bash
# List available backups
php artisan db-sync:restore --list

# Interactive backup selection
php artisan db-sync:restore

# Restore specific backup file
php artisan db-sync:restore backup_2025-01-15_120000.sql.gz
```

Options:

| Option | Description |
|--------|-------------|
| `--sync-connection=` | Connection name from config |
| `--list` | Only show available backups |
| `--force` | Skip confirmation prompt |

## How It Works

### Incremental Sync (`db-sync:pull`)

1. Opens SSH tunnel to the remote server
2. Creates a local backup
3. Analyzes each table: compares row counts, max IDs, and `updated_at` timestamps
4. Detects tables with changed structure (columns added/removed/modified)
5. Rebuilds changed tables (DROP + CREATE + import data)
6. Reconciles indexes/constraints to match remote (constraint-aware; all drops then all creates)
7. For unchanged structure: runs DELETE phase (removes records missing from remote), then UPSERT phase (inserts new / updates modified records)
8. CASCADE RECHECK: if parent table had deletions, re-checks child tables
9. Syncs views and resets auto-increment sequences

### Full Clone (`db-sync:clone`)

1. Opens SSH tunnel to the remote server
2. Shows the refresh plan, including local-only tables (not on remote) to be dropped, and asks for a single confirmation
3. Creates a local backup
4. Drops local-only tables (skipped with `--keep-local-tables`)
5. Dumps schema from remote using `pg_dump`
6. Drops all local tables and recreates from dump
7. Copies all data from remote in batches
8. Resets sequences

### Foreign Key Handling

The package builds a dependency graph from foreign key constraints and uses topological sorting to determine the correct order for:
- **Inserts**: parent tables first (parents-first order)
- **Deletes**: child tables first (children-first order)

Self-referencing tables (e.g. categories with `parent_id`) are handled via recursive CTEs.

## Architecture

The package uses an adapter pattern for database operations:

```
DatabaseAdapterInterface
└── PgsqlAdapter          # PostgreSQL implementation
```

Key services:

| Service | Responsibility |
|---------|---------------|
| `DependencyGraph` | FK dependency analysis, topological sorting |
| `DataSyncer` | Batch INSERT/UPSERT/DELETE operations |
| `SchemaManager` | Schema dump/restore, structure comparison |
| `BackupManager` | Backup creation, restore, cleanup |

## Docker & DDEV

This package requires SSH tunnels to work. For Docker and DDEV setup (SSH agent forwarding, autossh installation), see the [Docker & DDEV section](https://github.com/ArtemYurov/laravel-autossh-tunnel#docker--ddev) in laravel-autossh-tunnel documentation.

Add `postgresql-client` to your Dockerfile for schema operations (`pg_dump`, `psql`).

**Important:** The `pg_dump` version must be **>= the PostgreSQL server version**. Debian base images ship with older versions (e.g. Bookworm includes PG 15), so you may need the official PostgreSQL APT repository:

```dockerfile
# If the Debian pg_dump version matches your server — this is enough:
RUN apt-get update && apt-get install -y postgresql-client

# If the server is newer (e.g. PG 18 on Bookworm) — add the pgdg repository:
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/keyrings/pgdg.asc \
    && echo "deb [signed-by=/usr/share/keyrings/pgdg.asc] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
       > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends postgresql-client-18
```

## Adding Database Drivers

To support a new database (e.g. MySQL), implement `DatabaseAdapterInterface` and register it in `BaseDbSyncCommand::resolveAdapter()`.

## License

MIT
