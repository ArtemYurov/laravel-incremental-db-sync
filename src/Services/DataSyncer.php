<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Services;

use ArtemYurov\DbSync\Contracts\DatabaseAdapterInterface;
use Illuminate\Database\Connection;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\OutputInterface;

class DataSyncer
{
    /**
     * Safe upper bound for bind parameters in a single multi-row INSERT.
     * PostgreSQL's hard limit is 65535; we stay below it with a margin.
     */
    protected const MAX_BIND_PARAMS = 65000;

    /** UNIQUE constraints cache per table */
    protected array $constraintsCache = [];

    public function __construct(
        protected DatabaseAdapterInterface $adapter,
        protected ?OutputInterface $output = null,
    ) {}

    /**
     * Effective batch size for a table: the requested size, capped so that one
     * multi-row INSERT (columns × rows) stays under PostgreSQL's bind-parameter limit.
     * Used for BOTH reading and inserting so batches are uniform (no read-then-resplit).
     */
    public function effectiveBatchSize(Connection $source, string $table, int $requested): int
    {
        $columns = count($source->getSchemaBuilder()->getColumnListing($table));

        if ($columns < 1) {
            return max(1, $requested);
        }

        return max(1, min($requested, intdiv(self::MAX_BIND_PARAMS, $columns)));
    }

    /**
     * Bulk-insert a regular table's data into an empty target table using keyset
     * pagination by primary key (ORDER BY pk, WHERE pk > last) + multi-row INSERT.
     * Used by `clone` after DROP+CREATE; no conflict handling.
     *
     * Keyset (not OFFSET) pagination is required: OFFSET without a stable order re-reads
     * or skips rows on a live table, producing duplicate-key errors or silent gaps.
     *
     * Does NOT handle self-referencing tables — use insertTableFromRemote for that.
     *
     * @return array{inserted: int, updated: int, errors: int, error_messages: array}
     */
    public function insertTableData(
        Connection $source,
        Connection $target,
        string $table,
        string $primaryKey,
        int $batchSize,
        callable $retryCallback,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        $lastId = null;
        while (true) {
            // Per-batch timing (last batch only) — shows the real read/write cost per batch.
            $startRead = microtime(true);
            $records = $retryCallback(function () use ($source, $table, $primaryKey, $batchSize, $lastId) {
                $query = $source->table($table)->orderBy($primaryKey)->limit($batchSize);
                if ($lastId !== null) {
                    $query->where($primaryKey, '>', $lastId);
                }

                return $query->get();
            });
            $readSeconds = microtime(true) - $startRead;

            if ($records->isEmpty()) {
                break;
            }

            $rows = array_map(fn ($r) => (array) $r, $records->toArray());

            $startWrite = microtime(true);
            $this->insertBatch($target, $table, $rows, $stats, $progressBar);
            $writeSeconds = microtime(true) - $startWrite;

            $lastId = end($rows)[$primaryKey];

            $progressBar?->setMessage(sprintf('[r: %.2fs w: %.2fs]', $readSeconds, $writeSeconds));
        }

        return $stats;
    }

    /**
     * Bulk-insert a table's data from remote into a freshly recreated (empty) target
     * table. Used by `clone`: since the table was just DROP+CREATE'd, there are no
     * conflicts, so a multi-row INSERT is correct and far faster than per-row upsert.
     *
     * Tables WITHOUT a primary key are skipped (stats['skipped'] = true): without a key
     * we can neither paginate by keyset nor de-duplicate, so a reliable copy cannot be
     * guaranteed.
     *
     * Foreign keys: cross-table ordering is the caller's responsibility — `clone` syncs
     * tables parents-first via the dependency graph. Self-referencing tables are inserted
     * in roots-first order so an intra-table FK (e.g. parent_id) never references a
     * not-yet-inserted row.
     *
     * @return array{inserted: int, updated: int, errors: int, error_messages: array, skipped?: bool}
     */
    public function insertTableFromRemote(
        Connection $source,
        Connection $target,
        string $table,
        int $batchSize,
        callable $retryCallback,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        $primaryKey = $this->adapter->getPrimaryKeyColumn($source, $table);

        if (! $primaryKey) {
            // No primary key — cannot keyset-paginate or de-duplicate reliably. Skip.
            $stats['skipped'] = true;

            return $stats;
        }

        $selfRefColumn = $this->adapter->getSelfReferencingColumn($source, $table);

        if (! $selfRefColumn) {
            // Regular table — keyset pagination by primary key + bulk insert.
            return $this->insertTableData($source, $target, $table, $primaryKey, $batchSize, $retryCallback, $progressBar);
        }

        // Self-referencing: fetch in dependency order (roots first) and insert in chunks.
        $allRecords = $retryCallback(fn () => $this->adapter->getSelfReferencingRecords($source, $table, $primaryKey, $selfRefColumn));

        foreach (array_chunk($allRecords, $batchSize) as $batch) {
            $rows = array_map(function ($record) {
                $record = (array) $record;
                unset($record['depth']);

                return $record;
            }, $batch);

            $this->insertBatch($target, $table, $rows, $stats, $progressBar);
        }

        return $stats;
    }

    /**
     * Insert a batch of rows as a single multi-row INSERT, updating stats and progress.
     *
     * The batch is already sized within PostgreSQL's bind-parameter limit by the caller
     * (see effectiveBatchSize), so no further splitting is needed here.
     *
     * A multi-row INSERT is atomic: a single bad row (e.g. an orphaned FK) fails the
     * whole batch, and the exception would carry its full SQL. So on failure we retry the
     * batch row by row — only the genuinely bad rows are skipped (with short messages),
     * the rest are inserted, and the log stays small.
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @param  array{inserted:int,updated:int,errors:int,error_messages:array}  $stats
     */
    protected function insertBatch(Connection $target, string $table, array $rows, array &$stats, ?ProgressBar $progressBar): void
    {
        if (empty($rows)) {
            return;
        }

        try {
            $target->table($table)->insert($rows);
            $stats['inserted'] += count($rows);
            $progressBar?->advance(count($rows));
        } catch (\Exception $e) {
            // Fallback advances the bar per row, so it visibly crawls one-by-one here.
            $this->insertRowsIndividually($target, $table, $rows, $stats, $progressBar);
        }
    }

    /**
     * Fallback for a failed bulk batch: insert rows one by one so only the offending
     * rows are reported as errors (with a short message, not the whole batch SQL).
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @param  array{inserted:int,updated:int,errors:int,error_messages:array}  $stats
     */
    protected function insertRowsIndividually(Connection $target, string $table, array $rows, array &$stats, ?ProgressBar $progressBar = null): void
    {
        foreach ($rows as $row) {
            try {
                $target->table($table)->insert($row);
                $stats['inserted']++;
            } catch (\Exception $e) {
                $stats['errors']++;
                $stats['error_messages'][] = [
                    'id' => $row['id'] ?? null,
                    'message' => $this->shortError($e),
                ];
            }

            $progressBar?->advance();
        }
    }

    /**
     * Shorten a DB exception message: keep the SQLSTATE/ERROR part, drop the appended
     * SQL/bindings dump so logs and console output stay readable.
     */
    protected function shortError(\Throwable $e): string
    {
        $message = $e->getMessage();

        // Laravel's QueryException appends " (Connection: ..., SQL: <full query>)".
        $cut = strpos($message, ' (Connection:');
        if ($cut !== false) {
            $message = substr($message, 0, $cut);
        }

        return trim(preg_replace('/\s+/', ' ', $message));
    }

    /**
     * Get IDs of records that don't exist on remote.
     */
    public function getIdsToDelete(
        Connection $source,
        Connection $target,
        string $table,
        string $primaryKey,
        int $batchSize,
        callable $retryCallback,
    ): array {
        // Get all IDs from remote
        $remoteIds = [];
        $offset = 0;
        while (true) {
            $batch = $retryCallback(fn () => $source
                ->table($table)
                ->select($primaryKey)
                ->limit($batchSize)
                ->offset($offset)
                ->pluck($primaryKey)
                ->toArray());

            if (empty($batch)) {
                break;
            }

            $remoteIds = array_merge($remoteIds, $batch);
            $offset += $batchSize;
        }

        // Determine which IDs will be deleted
        $localIds = $target->table($table)->pluck($primaryKey)->toArray();

        return !empty($remoteIds)
            ? array_values(array_diff($localIds, $remoteIds))
            : $localIds;
    }

    /**
     * Delete records that don't exist on remote.
     * First cleans orphaned records from child tables.
     *
     * @return array{deleted: int, errors: int}
     */
    public function deleteFromTable(
        Connection $target,
        string $table,
        string $primaryKey,
        array $idsToDelete,
        int $batchSize,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['deleted' => 0, 'errors' => 0, 'error_messages' => []];

        if (empty($idsToDelete)) {
            return $stats;
        }

        // First delete orphaned records from ALL child tables
        $childTables = $this->adapter->getChildTables($target, $table);
        foreach ($childTables as $childTable => $fkColumn) {
            foreach (array_chunk($idsToDelete, $batchSize) as $chunk) {
                try {
                    $target->table($childTable)->whereIn($fkColumn, $chunk)->delete();
                } catch (\Exception $e) {
                    // Ignore errors in child tables
                }
            }
        }

        // Now delete records from the main table
        foreach (array_chunk($idsToDelete, $batchSize) as $chunk) {
            try {
                $deleted = $target->table($table)->whereIn($primaryKey, $chunk)->delete();
                $stats['deleted'] += $deleted;
            } catch (\Exception $e) {
                $stats['errors'] += count($chunk);
                $stats['error_messages'][] = ['id' => null, 'message' => $e->getMessage()];
            }
            $progressBar?->advance(count($chunk));
        }

        return $stats;
    }

    /**
     * Upsert records from remote.
     *
     * @return array{inserted: int, updated: int, errors: int}
     */
    public function syncTableFromRemote(
        Connection $source,
        Connection $target,
        string $table,
        int $batchSize,
        callable $retryCallback,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        $primaryKey = $this->adapter->getPrimaryKeyColumn($source, $table);
        if (!$primaryKey) {
            return $stats;
        }

        // Check for self-reference
        $selfRefColumn = $this->adapter->getSelfReferencingColumn($source, $table);

        if ($selfRefColumn) {
            return $this->upsertSelfReferencingTable(
                $source, $target, $table, $primaryKey, $selfRefColumn, $batchSize, $retryCallback, $progressBar
            );
        }

        // Regular table — keyset pagination (stable on live tables) + bulk upsert.
        $lastId = null;
        while (true) {
            $startRead = microtime(true);
            $records = $retryCallback(function () use ($source, $table, $primaryKey, $batchSize, $lastId) {
                $query = $source->table($table)->orderBy($primaryKey)->limit($batchSize);
                if ($lastId !== null) {
                    $query->where($primaryKey, '>', $lastId);
                }

                return $query->get();
            });
            $readSeconds = microtime(true) - $startRead;

            if ($records->isEmpty()) {
                break;
            }

            $startWrite = microtime(true);
            $result = $this->upsertRecords($target, $table, $records->toArray(), $primaryKey, $progressBar);
            $writeSeconds = microtime(true) - $startWrite;

            $stats['inserted'] += $result['inserted'];
            $stats['updated'] += $result['updated'];
            $stats['errors'] += $result['errors'];
            $stats['error_messages'] = array_merge($stats['error_messages'], $result['error_messages']);

            $lastId = $records->last()->{$primaryKey};

            $progressBar?->setMessage(sprintf('[r: %.2fs w: %.2fs]', $readSeconds, $writeSeconds));
        }

        return $stats;
    }

    /**
     * Upsert for a self-referencing table.
     * Root records (FK = NULL) first, then children recursively.
     *
     * @return array{inserted: int, updated: int, errors: int}
     */
    public function upsertSelfReferencingTable(
        Connection $source,
        Connection $target,
        string $table,
        string $primaryKey,
        string $selfRefColumn,
        int $batchSize,
        callable $retryCallback,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        $allRecords = $retryCallback(fn () => $this->adapter->getSelfReferencingRecords($source, $table, $primaryKey, $selfRefColumn));

        if (empty($allRecords)) {
            return $stats;
        }

        // Remove the auxiliary depth field and upsert in batches
        foreach (array_chunk($allRecords, $batchSize) as $batch) {
            $cleanBatch = array_map(function ($record) {
                $record = (array) $record;
                unset($record['depth']);
                return (object) $record;
            }, $batch);

            $result = $this->upsertRecords($target, $table, $cleanBatch, $primaryKey, $progressBar);
            $stats['inserted'] += $result['inserted'];
            $stats['updated'] += $result['updated'];
            $stats['errors'] += $result['errors'];
            $stats['error_messages'] = array_merge($stats['error_messages'], $result['error_messages']);
        }

        return $stats;
    }

    /**
     * Upsert an array of records into a table.
     *
     * @return array{inserted: int, updated: int, errors: int}
     */
    public function upsertRecords(
        Connection $target,
        string $table,
        array $records,
        ?string $primaryKey = null,
        ?ProgressBar $progressBar = null,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        if (empty($records)) {
            return $stats;
        }

        if (!$primaryKey) {
            $primaryKey = $this->adapter->getPrimaryKeyColumn($target, $table);
        }

        if (!$primaryKey) {
            // Without PK — just INSERT
            try {
                $recordsArray = array_map(fn ($r) => (array) $r, $records);
                $target->table($table)->insert($recordsArray);
                $stats['inserted'] = count($records);
                $progressBar?->advance(count($records));
            } catch (\Exception $e) {
                $stats['errors'] = count($records);
                $stats['error_messages'][] = ['id' => null, 'message' => $e->getMessage()];
            }
            return $stats;
        }

        $columns = array_keys((array) $records[0]);
        $updateColumns = array_values(array_filter($columns, fn ($c) => $c !== $primaryKey));

        // Delete local records conflicting by UNIQUE constraints
        $this->deleteConflictingRecords($target, $table, $records, $primaryKey);

        // Bulk upsert in chunks within the bind-parameter limit; fall back to per-row on error.
        $rowsPerChunk = max(1, intdiv(self::MAX_BIND_PARAMS, count($columns)));

        foreach (array_chunk($records, $rowsPerChunk) as $chunk) {
            $rows = array_map(fn ($r) => (array) $r, $chunk);

            try {
                $target->table($table)->upsert($rows, [$primaryKey], $updateColumns);
                $stats['updated'] += count($rows);
                $progressBar?->advance(count($rows));
            } catch (\Exception $e) {
                $this->upsertRowsIndividually($target, $table, $rows, $primaryKey, $columns, $stats, $progressBar);
            }
        }

        return $stats;
    }

    /**
     * Fallback for a failed bulk upsert: upsert rows one by one so only the offending
     * rows are reported as errors and the rest are applied.
     *
     * @param  array<int, array<string, mixed>>  $rows
     * @param  string[]  $columns
     * @param  array{inserted:int,updated:int,errors:int,error_messages:array}  $stats
     */
    protected function upsertRowsIndividually(Connection $target, string $table, array $rows, string $primaryKey, array $columns, array &$stats, ?ProgressBar $progressBar = null): void
    {
        foreach ($rows as $record) {
            $result = $this->adapter->upsertRecord($target, $table, (array) $record, $primaryKey, $columns);
            $stats['inserted'] += $result['inserted'];
            $stats['updated'] += $result['updated'];
            $stats['errors'] += $result['errors'];
            $stats['error_messages'] = array_merge($stats['error_messages'], $result['error_messages']);

            $progressBar?->advance();
        }
    }

    /**
     * Delete records conflicting by UNIQUE constraints.
     */
    protected function deleteConflictingRecords(
        Connection $target,
        string $table,
        array $records,
        string $primaryKey,
    ): int {
        if (!isset($this->constraintsCache[$table])) {
            $this->constraintsCache[$table] = $this->adapter->getUniqueConstraints($target, $table);
        }

        $uniqueConstraints = $this->constraintsCache[$table];

        if (empty($uniqueConstraints)) {
            return 0;
        }

        $deleted = 0;
        $childTables = $this->adapter->getChildTables($target, $table);

        foreach ($uniqueConstraints as $constraint) {
            $columns = $constraint['columns'];
            if (empty($columns)) {
                continue;
            }

            $remoteValues = [];
            $remoteIds = [];
            foreach ($records as $record) {
                $record = (array) $record;
                $values = [];
                foreach ($columns as $col) {
                    $values[$col] = $record[$col] ?? null;
                }
                $remoteValues[] = $values;
                $remoteIds[] = $record[$primaryKey];
            }

            foreach ($remoteValues as $idx => $values) {
                // Skip if ALL unique fields are NULL (NULL != NULL in PG)
                $allNull = true;
                foreach ($values as $val) {
                    if ($val !== null) {
                        $allNull = false;
                        break;
                    }
                }
                if ($allNull) {
                    continue;
                }

                $findQuery = $target->table($table)->select($primaryKey);

                foreach ($values as $col => $val) {
                    if ($val === null) {
                        $findQuery->whereNull($col);
                    } else {
                        $findQuery->where($col, $val);
                    }
                }

                $findQuery->where($primaryKey, '!=', $remoteIds[$idx]);
                $idsToDelete = $findQuery->pluck($primaryKey)->toArray();

                if (empty($idsToDelete)) {
                    continue;
                }

                // Delete dependent records from child tables
                foreach ($childTables as $childTable => $fkColumn) {
                    try {
                        $target->table($childTable)->whereIn($fkColumn, $idsToDelete)->delete();
                    } catch (\Exception $e) {
                        // Ignore
                    }
                }

                try {
                    $deleted += $target->table($table)->whereIn($primaryKey, $idsToDelete)->delete();
                } catch (\Exception $e) {
                    // Ignore
                }
            }
        }

        return $deleted;
    }

    /**
     * Reset the constraints cache.
     */
    public function resetConstraintsCache(): void
    {
        $this->constraintsCache = [];
    }
}
