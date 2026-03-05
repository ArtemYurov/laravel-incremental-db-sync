<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Services;

use ArtemYurov\DbSync\Contracts\DatabaseAdapterInterface;
use Illuminate\Database\Connection;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\OutputInterface;

class DataSyncer
{
    /** UNIQUE constraints cache per table */
    protected array $constraintsCache = [];

    public function __construct(
        protected DatabaseAdapterInterface $adapter,
        protected ?OutputInterface $output = null,
    ) {}

    /**
     * Insert data into an empty table (used in refresh after DROP+CREATE).
     *
     * @return array{inserted: int, errors: int}
     */
    public function insertTableData(
        Connection $source,
        Connection $target,
        string $table,
        int $remoteCount,
        int $batchSize,
        callable $retryCallback,
    ): array {
        $stats = ['inserted' => 0, 'errors' => 0, 'error_messages' => []];

        if ($remoteCount == 0) {
            return $stats;
        }

        $offset = 0;

        while (true) {
            $records = $retryCallback(fn () => $source->table($table)->limit($batchSize)->offset($offset)->get());

            if ($records->isEmpty()) {
                break;
            }

            try {
                $recordsArray = array_map(fn ($record) => (array) $record, $records->toArray());
                $target->table($table)->insert($recordsArray);
                $stats['inserted'] += $records->count();
            } catch (\Exception $e) {
                $stats['errors'] += $records->count();
                $stats['error_messages'][] = ['id' => null, 'message' => $e->getMessage()];
            }

            $offset += $batchSize;
        }

        return $stats;
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

        // Regular table — simple upsert
        $offset = 0;
        while (true) {
            $records = $retryCallback(fn () => $source->table($table)->limit($batchSize)->offset($offset)->get());

            if ($records->isEmpty()) {
                break;
            }

            $result = $this->upsertRecords($target, $table, $records->toArray(), $primaryKey, $progressBar);
            $stats['inserted'] += $result['inserted'];
            $stats['updated'] += $result['updated'];
            $stats['errors'] += $result['errors'];
            $stats['error_messages'] = array_merge($stats['error_messages'], $result['error_messages']);
            $offset += $batchSize;
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

        // Delete local records conflicting by UNIQUE constraints
        $this->deleteConflictingRecords($target, $table, $records, $primaryKey);

        foreach ($records as $record) {
            $record = (array) $record;

            $result = $this->adapter->upsertRecord($target, $table, $record, $primaryKey, $columns);
            $stats['inserted'] += $result['inserted'];
            $stats['updated'] += $result['updated'];
            $stats['errors'] += $result['errors'];
            $stats['error_messages'] = array_merge($stats['error_messages'], $result['error_messages']);

            $progressBar?->advance();
        }

        return $stats;
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
