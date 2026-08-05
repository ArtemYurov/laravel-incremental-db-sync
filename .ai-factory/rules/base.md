# Project Base Rules

> Auto-detected conventions from codebase analysis. Edit as needed.

## Language

- All source code, comments, PHPDoc, commit messages, README, and generated
  `.ai-factory/` artifacts are written in **English** — this is a public Composer package.
- Conversation with the maintainer happens in Russian; that never leaks into files.

## Naming Conventions

- Files: `PascalCase.php`, one class per file, filename equals class name (PSR-4).
- Namespaces: `ArtemYurov\DbSync\<Layer>` mirroring the directory under `src/`.
- Classes: `PascalCase`. Suffix by role — `*Command`, `*Adapter`, `*Exception`,
  `*Manager`, `*Syncer`, `*Graph`, `*Diff`.
- Interfaces: `*Interface` suffix (`DatabaseAdapterInterface`).
- Traits: `Manages*` for behavior mixins (`ManagesSignals`, `ManagesMemoryLimit`).
- Methods and variables: `camelCase`. Constants: `UPPER_SNAKE_CASE`.
- Test methods: `test_snake_case_describing_the_expectation()`.
- Config keys and Artisan option names: `kebab-case` / `snake_case`
  (`--sync-connection`, `excluded_tables`).
- Artisan commands: `db-sync:<verb>`.

## Module Structure

- `src/Console/` — Artisan commands; orchestration only, no vendor SQL. Shared behavior in
  `BaseDbSyncCommand` and `Console/Traits/`.
- `src/Contracts/` — interfaces defining seams; `DatabaseAdapterInterface` is the single
  boundary for database-vendor specifics.
- `src/Adapters/` — vendor implementations. **All** PostgreSQL catalog queries, DDL string
  building, and `pg_dump`/`psql` process calls belong here and nowhere else.
- `src/Services/` — sync orchestration (`SchemaManager`, `DataSyncer`, `DependencyGraph`,
  `BackupManager`) and pure logic (`StructureDiff`).
- `src/DTO/` — `final readonly` value objects with promoted public properties and a
  `fromArray()` / named static factory.
- `src/Enums/` — backed string enums.
- `src/Exceptions/` — exception hierarchy rooted at `DbSyncException`.
- Dependency direction: `Console → Services → Contracts ← Adapters`. Services must not
  reference `PgsqlAdapter` directly — always the interface.

## PHP Conventions

- Every file starts with `<?php`, a blank line, then `declare(strict_types=1);`.
- Constructor property promotion for dependencies; `protected` for service state,
  `public` for DTO fields.
- Explicit return types on every method, including `: void`.
- Array shapes documented in PHPDoc when a method returns a structured array:
  `@return array{inserted: int, updated: int, errors: int, error_messages: array}`.
- No Eloquent models — use the Illuminate query builder against explicit connections.
- `use` imports sorted; no inline fully-qualified names except optional third-party
  integrations guarded by `class_exists()` (e.g. Telescope).

## Error Handling

- Throw `DbSyncException` (or a narrower subclass: `SyncException`, `AdapterException`)
  for fatal conditions; never a bare `RuntimeException`.
- Per-row and per-batch failures are **counted, not thrown** — accumulate into
  `$stats['errors']` and `$stats['error_messages']` so a long sync can report at the end.
- Destructive operations require `--force` or an interactive confirmation.
- Message style: lowercase-after-prefix, quoting the offending identifier —
  `"db-sync connection configuration '{$name}' not found"`.

## Control Flow

- Prefer flat, readable control flow over deeply nested conditionals. Use guard clauses,
  early `return`/`continue`, small named helper methods, or explicit classification logic
  when they make the code easier to follow. Handle edge cases and irrelevant branches early
  so the main path stays visible.
- Batch loops use `while (true)` with an explicit `break` on an empty result set, and
  keyset pagination (`ORDER BY pk`, `WHERE pk > $lastId`) — never `OFFSET` on a live table.

## Logging

- No logging framework. Output goes through Artisan console helpers: `$this->line()`,
  `$this->warn()`, `$this->error()`, `$this->newLine()`.
- Status lines are prefixed with an emoji marker already in use: `ℹ` info, `⚠` warning,
  `🔧` structural work.
- Long operations report through `Symfony\Component\Console\Helper\ProgressBar` with a
  per-batch timing message (`[r: 0.42s w: 1.13s]`).
- Never log credentials or full row payloads.

## Comments

- Non-obvious decisions get a block comment explaining **why**, not what — e.g. why keyset
  pagination instead of `OFFSET`, why the bind-parameter cap is 65000 and not 65535.
- Class- and method-level PHPDoc describes intent and stated constraints (what the method
  does *not* handle) so callers know their responsibility.

## Testing

- PHPUnit + Orchestra Testbench. Base class: `ArtemYurov\DbSync\Tests\TestCase`, which
  registers `DbSyncServiceProvider` and defines a `test` sync connection.
- Suite lives in `tests/Unit/`; `phpunit.xml.dist` declares a single `Unit` testsuite.
- Prefer testing pure logic (`StructureDiff`, `DependencyGraph`, `SyncConfig`, DDL string
  builders) with plain array fixtures — no live database in unit tests.
- SQL fixtures live in `tests/fixtures/`.
- One assertion focus per test; `assertSame()` over `assertEquals()` for arrays and scalars.

## Compatibility

- Support matrix is PHP `^8.2` and Laravel `^10 | ^11 | ^12 | ^13`. Do not use APIs missing
  from the oldest supported line without a guard.
- The companion package `artemyurov/laravel-autossh-tunnel` is a hard dependency; when both
  are changed, release autossh-tunnel first.
