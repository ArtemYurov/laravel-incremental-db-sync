<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Console;

use ArtemYurov\Autossh\Console\Traits\ManagesTunnel;
use ArtemYurov\DbSync\Adapters\PgsqlAdapter;
use ArtemYurov\DbSync\Console\Traits\ManagesMemoryLimit;
use ArtemYurov\DbSync\Console\Traits\ManagesSignals;
use ArtemYurov\DbSync\Contracts\DatabaseAdapterInterface;
use ArtemYurov\DbSync\DTO\SyncConfig;
use ArtemYurov\DbSync\Exceptions\DbSyncException;
use ArtemYurov\DbSync\Services\BackupManager;
use ArtemYurov\DbSync\Services\DataSyncer;
use ArtemYurov\DbSync\Services\DependencyGraph;
use ArtemYurov\DbSync\Services\SchemaManager;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

abstract class BaseDbSyncCommand extends Command
{
    use ManagesMemoryLimit;
    use ManagesSignals;
    use ManagesTunnel;

    /** Remote database connection name (dynamic) */
    protected string $remoteConnectionName = 'db_sync_remote';

    protected ?SyncConfig $syncConfig = null;
    protected ?DatabaseAdapterInterface $adapter = null;
    protected ?DependencyGraph $dependencyGraph = null;
    protected ?DataSyncer $dataSyncer = null;
    protected ?SchemaManager $schemaManager = null;
    protected ?BackupManager $backupManager = null;

    /** Table analysis indexed by name */
    protected array $tableAnalysis = [];

    /**
     * Initialize configuration and services
     */
    protected function initializeSync(): void
    {
        $connectionName = $this->option('sync-connection') ?? config('db-sync.default', 'production');
        $connectionConfig = config("db-sync.connections.{$connectionName}");

        if (!$connectionConfig) {
            throw new DbSyncException("db-sync connection configuration '{$connectionName}' not found");
        }

        $this->syncConfig = SyncConfig::fromArray($connectionName, $connectionConfig);
        $this->adapter = $this->resolveAdapter();
        $this->dependencyGraph = new DependencyGraph($this->adapter);
        $this->dataSyncer = new DataSyncer($this->adapter, $this->output);
        $this->schemaManager = new SchemaManager($this->adapter, $this->dependencyGraph);
        $this->backupManager = new BackupManager($this->adapter);

        $this->disableQueryRecording();
    }

    /**
     * Disable per-query recorders for the duration of the sync. They capture every
     * statement (full SQL + bindings) — for bulk inserts of thousands of rows this adds
     * huge overhead and memory use. Notably Laravel Telescope's query watcher.
     */
    protected function disableQueryRecording(): void
    {
        DB::connection($this->syncConfig->target)->disableQueryLog();
        $this->line('ℹ DB query log disabled for this sync');

        if (class_exists(\Laravel\Telescope\Telescope::class)) {
            \Laravel\Telescope\Telescope::stopRecording();
            $this->line('ℹ Telescope stop recording for this sync');
        }

        $this->newLine();
    }

    /**
     * Resolve adapter by database driver
     */
    protected function resolveAdapter(): DatabaseAdapterInterface
    {
        $driver = $this->syncConfig->getDriver();

        return match ($driver) {
            'pgsql' => new PgsqlAdapter(),
            default => throw new DbSyncException("Database driver '{$driver}' is not supported. Available: pgsql"),
        };
    }

    /**
     * Establish SSH tunnel and connect to remote database
     */
    protected function connectToRemote(): void
    {
        $this->setupTunnel(
            connectionName: $this->syncConfig->tunnel,
            dbConfig: array_merge(
                $this->syncConfig->getSourceDatabaseConfig(),
                ['connection_name' => $this->remoteConnectionName]
            ),
        );

        // Verify connection
        $this->info('Verifying connection to the remote database...');
        try {
            $remoteDb = DB::connection($this->remoteConnectionName)->select('SELECT current_database() as db');
            $this->info('   ✓ Connected to: ' . $remoteDb[0]->db . ' (tunnel: ' . $this->syncConfig->tunnel . ')');
        } catch (\Exception $e) {
            throw new DbSyncException("Remote database connection error: {$e->getMessage()}", 0, $e);
        }
        $this->newLine();
    }

    /**
     * Get source connection
     */
    protected function sourceConnection(): \Illuminate\Database\Connection
    {
        return DB::connection($this->remoteConnectionName);
    }

    /**
     * Get target connection
     */
    protected function targetConnection(): \Illuminate\Database\Connection
    {
        return DB::connection($this->syncConfig->target);
    }

    /**
     * Get source database configuration (for pg_dump, etc.)
     */
    protected function getSourceConnectionConfig(): array
    {
        return config("database.connections.{$this->remoteConnectionName}", []);
    }

    /**
     * Get target database configuration
     */
    protected function getTargetConnectionConfig(): array
    {
        return config("database.connections.{$this->syncConfig->target}", []);
    }

    /**
     * Get batch size from command option
     */
    /**
     * Resolve the batch size: the explicit --batch-size option when it is a positive
     * number, otherwise the configured default (config db-sync.batch_size / env
     * DB_SYNC_BATCH_SIZE).
     */
    protected function getBatchSize(): int
    {
        $batchSize = (int) $this->option('batch-size');

        if ($batchSize <= 0) {
            $batchSize = (int) ($this->syncConfig?->batchSize ?? config('db-sync.batch_size', 10000));
        }

        return $batchSize;
    }

    /**
     * Execute callback with retry through tunnel
     */
    protected function retryCallback(): callable
    {
        return fn (callable $operation) => $this->withTunnelRetry($operation);
    }

    /**
     * Create local database backup
     */
    protected function createLocalBackup(): bool
    {
        $config = $this->getTargetConnectionConfig();
        $backupPath = $this->syncConfig->backupPath;

        try {
            $backupFile = $this->backupManager->createBackup($config, $backupPath);
            $this->info("   File: {$backupFile}");
            $this->info("   Size: " . $this->formatBytes(filesize($backupFile)) . " (compressed)");

            // Clean up old backups
            $deleted = $this->backupManager->cleanupOldBackups($backupPath, $this->syncConfig->keepLastBackups);
            if ($deleted > 0) {
                $this->info("   Old backups deleted: {$deleted}");
            }

            return true;
        } catch (\Exception $e) {
            $this->error("   ✗ Backup creation error: {$e->getMessage()}");
            return false;
        }
    }

    /**
     * Get table metadata (with retry for remote)
     */
    protected function getTableMetadata(string $table, string $connectionType = 'remote'): array
    {
        $connection = $connectionType === 'remote'
            ? $this->sourceConnection()
            : $this->targetConnection();

        if ($connectionType === 'remote') {
            return $this->withTunnelRetry(fn () => $this->adapter->getTableMetadata($connection, $table));
        }

        return $this->adapter->getTableMetadata($connection, $table);
    }

    /**
     * Cleanup on termination (ManagesSignals)
     */
    protected function performCleanup(): void
    {
        $this->closeTunnel();
    }

    protected function formatBytes(int $bytes, int $precision = 2): string
    {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        for ($i = 0; $bytes > 1024 && $i < count($units) - 1; $i++) {
            $bytes /= 1024;
        }
        return round($bytes, $precision) . ' ' . $units[$i];
    }

    /**
     * Filter excluded tables, but keep ones explicitly requested via --tables
     *
     * @param  array  $tableNames  All tables
     * @param  array  $excludedTables  Tables from excluded_tables config
     * @param  array  $requestedTables  Tables explicitly requested via --tables
     * @return array
     */
    protected function filterExcludedTables(array $tableNames, array $excludedTables, array $requestedTables = []): array
    {
        return array_values(array_filter(
            $tableNames,
            fn ($table) => !in_array($table, $excludedTables) || in_array($table, $requestedTables)
        ));
    }

    /**
     * Tables in scope that are excluded and NOT explicitly requested via --tables
     * (for the "structure only, no data" listing).
     */
    protected function getExcludedInScope(array $allTableNames, array $excludedTables, array $syncTableNames): array
    {
        return array_values(array_filter(
            $allTableNames,
            fn ($table) => in_array($table, $excludedTables) && !in_array($table, $syncTableNames)
        ));
    }

    /**
     * Reset internal state
     */
    protected function resetState(): void
    {
        $this->dependencyGraph?->reset();
        $this->dataSyncer?->resetConstraintsCache();
        $this->tableAnalysis = [];
    }
}
