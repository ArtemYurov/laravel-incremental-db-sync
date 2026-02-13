<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Console;

use ArtemYurov\DbSync\DTO\TableDiff;
use Illuminate\Support\Facades\DB;

/**
 * Incremental database synchronization from remote server
 */
class PullCommand extends BaseDbSyncCommand
{
    protected $signature = 'db-sync:pull
                            {--sync-connection= : Connection name from config/db-sync.php}
                            {--force : Run without confirmation}
                            {--tables= : Synchronize only specified tables (comma-separated)}
                            {--views= : Synchronize only specified views (comma-separated)}
                            {--include-excluded : Include excluded tables}
                            {--analyze-only : Only analyze changes}
                            {--dry-run : Show what will be synchronized without executing}
                            {--batch-size=10000 : Batch size}
                            {--skip-backup : Skip backup creation}
                            {--skip-sequences : Skip sequence reset}
                            {--memory-limit=-1 : Memory limit in MB (-1 unlimited)}';

    protected $description = 'Incremental database synchronization from remote server via SSH';

    /** Sync results: ['table' => ['inserted'=>0, 'updated'=>0, 'deleted'=>0, 'errors'=>0]] */
    protected array $syncResults = [];

    public function handle(): int
    {
        $this->info('=== Database synchronization from remote server ===');
        $this->newLine();

        try {
            $this->initializeSync();
        } catch (\Exception $e) {
            $this->error($e->getMessage());
            return self::FAILURE;
        }

        $this->ensureMemoryLimit((int) $this->option('memory-limit'));
        $this->setupSignalHandlers();
        $this->resetState();
        $this->syncResults = [];

        try {
            $this->connectToRemote();

            // Build dependency graph
            $this->dependencyGraph->build($this->sourceConnection());

            // Analyze changes
            $analysis = $this->analyzeChanges();

            $checkTables = !$this->option('views') || $this->option('tables');
            $checkViews = !$this->option('tables') || $this->option('views');
            $allRemoteTables = $checkTables ? array_column($analysis, 'table') : [];
            $allRemoteViews = $checkViews ? $this->getRemoteViews() : [];

            // Find tables that need recreation
            $refreshInfo = $this->schemaManager->findTablesNeedingRefresh(
                $this->sourceConnection(),
                $this->targetConnection(),
                $allRemoteTables,
                $allRemoteViews,
            );
            $tablesToRefresh = array_merge($refreshInfo['missing_tables'], $refreshInfo['changed_tables']);
            $viewsToRefresh = array_merge($refreshInfo['missing_views'], $refreshInfo['changed_views']);

            // Sync plan
            $tablesToSync = $this->buildSyncPlan($analysis, $tablesToRefresh);

            // Structural changes info
            if (!empty($tablesToRefresh) || !empty($viewsToRefresh)) {
                if (!empty($refreshInfo['missing_tables'])) {
                    $this->info('Missing tables: ' . implode(', ', $refreshInfo['missing_tables']));
                }
                if (!empty($refreshInfo['changed_tables'])) {
                    $this->info('Tables with changed structure: ' . implode(', ', $refreshInfo['changed_tables']));
                }
                $this->newLine();
            }

            if (empty($tablesToSync)) {
                $this->info('✓ No changes to synchronize!');
                return self::SUCCESS;
            }

            // analyze-only
            if ($this->option('analyze-only')) {
                $this->displayAnalysisTable($tablesToSync);
                $this->info('Run the command without --analyze-only to perform synchronization');
                return self::SUCCESS;
            }

            // Filter tables with actual changes
            $tablesToProcess = $this->filterTablesWithChanges($tablesToSync);

            if (empty($tablesToProcess)) {
                $this->info('✓ No changes to synchronize!');
                return self::SUCCESS;
            }

            // dry-run
            if ($this->option('dry-run')) {
                $this->displayChangesTable($tablesToProcess);
                $this->info('--dry-run mode: no actual synchronization will be performed');
                return self::SUCCESS;
            }

            // Display and confirm
            $this->displayChangesTable($tablesToProcess);

            if (!$this->confirmSync()) {
                $this->info('Operation cancelled.');
                return self::SUCCESS;
            }

            // Backup (after confirmation, before modifications)
            if (!$this->option('skip-backup')) {
                $this->info('Creating local database backup...');
                if (!$this->createLocalBackup()) {
                    return self::FAILURE;
                }
                $this->info('   ✓ Backup created');
                $this->newLine();
            }

            // DROP+CREATE tables with changed structure
            if (!empty($tablesToRefresh)) {
                $totalRefresh = count($tablesToRefresh);
                $this->info('Recreating tables (DROP+CREATE)...');

                foreach ($tablesToRefresh as $idx => $table) {
                    $num = $idx + 1;
                    $this->line("   [{$num}/{$totalRefresh}] {$table}...");
                }

                $sourceConfig = $this->getSourceConnectionConfig();
                $schemaResult = $this->schemaManager->refreshTablesStructure(
                    $this->sourceConnection(),
                    $this->targetConnection(),
                    $sourceConfig,
                    $tablesToRefresh,
                    [],
                );

                $this->output->write("\033[{$totalRefresh}A");
                foreach ($tablesToRefresh as $idx => $table) {
                    $num = $idx + 1;
                    $this->output->write("\r\033[K");
                    $this->line("   [{$num}/{$totalRefresh}] {$table} ✓");
                }
                $this->newLine();
            }

            // MAIN SYNCHRONIZATION
            $this->syncTables($tablesToSync, 'main', true, false);

            // CASCADE RECHECK
            $this->executeCascadeRecheck();

            // VIEW
            if (!empty($allRemoteViews)) {
                $missingViews = [];
                foreach ($allRemoteViews as $view) {
                    if (!$this->adapter->viewExists($this->targetConnection(), $view)) {
                        $missingViews[] = $view;
                    }
                }

                if (!empty($missingViews)) {
                    $this->info('Updating views: ' . implode(', ', $missingViews));
                    $sourceConfig = $this->getSourceConnectionConfig();
                    $schemaResult = $this->schemaManager->refreshTablesStructure(
                        $this->sourceConnection(),
                        $this->targetConnection(),
                        $sourceConfig,
                        [],
                        $missingViews,
                    );
                    $this->info('   ✓ Views updated');
                    $this->newLine();
                }
            }

            // SEQUENCES
            if (!$this->option('skip-sequences')) {
                $this->info('Resetting auto-increment counters...');
                $resetCount = $this->adapter->resetSequences($this->targetConnection());
                $this->info("   Sequences reset: {$resetCount}");
                $this->newLine();
            }

            // RESULTS
            $this->displaySyncResults();
            $this->newLine();
            $this->info('✓ Synchronization completed successfully!');
            return self::SUCCESS;

        } catch (\Exception $e) {
            $this->error('Synchronization error: ' . $e->getMessage());
            return self::FAILURE;
        } finally {
            $this->closeTunnel();
        }
    }

    /**
     * Analyze changes across all tables
     */
    protected function analyzeChanges(): array
    {
        $this->info('Analyzing changes...');

        $tableNames = $this->withTunnelRetry(fn () => $this->adapter->getTablesList($this->sourceConnection()));

        if (!$this->option('include-excluded')) {
            $tableNames = array_filter($tableNames, fn ($table) => !in_array($table, $this->syncConfig->excludedTables));
        }
        if ($this->option('tables')) {
            $requestedTables = array_map('trim', explode(',', $this->option('tables')));
            $tableNames = array_intersect($tableNames, $requestedTables);
        }
        $tableNames = array_values($tableNames);

        $totalTables = count($tableNames);
        $analysis = [];
        $needsSyncCount = 0;

        $progressBar = $this->output->createProgressBar($totalTables);
        $progressBar->setFormat(' %current%/%max% [%bar%] %percent:3s%% %message%');
        $progressBar->setMessage('');

        foreach ($tableNames as $table) {
            $progressBar->setMessage($table);

            $diff = $this->compareTable($table);
            $analysis[] = $diff;

            if ($diff['needs_sync']) {
                $needsSyncCount++;
            }

            $progressBar->advance();
        }

        $this->newLine(2);

        $this->info('✓ Analysis complete:');
        $this->info('   Total tables: ' . count($tableNames));
        $this->info('   Need synchronization: ' . $needsSyncCount);
        $this->newLine();

        // Fill map for quick access
        $this->tableAnalysis = [];
        foreach ($analysis as $diff) {
            $this->tableAnalysis[$diff['table']] = $diff;
        }

        return $analysis;
    }

    /**
     * Compare a single table
     */
    protected function compareTable(string $table): array
    {
        $local = $this->getTableMetadata($table, 'local');
        $remote = $this->getTableMetadata($table, 'remote');

        $diff = [
            'table' => $table,
            'needs_sync' => false,
            'local_count' => $local['count'],
            'remote_count' => $remote['count'],
            'has_updates' => false,
            'ids_to_delete' => [],
        ];

        if (!empty($local['error']) || !empty($remote['error'])) {
            $diff['needs_sync'] = true;
            $diff['metadata_error'] = true;
            return $diff;
        }

        // Get IDs to delete
        $primaryKey = $this->adapter->getPrimaryKeyColumn($this->sourceConnection(), $table);
        if ($primaryKey && $local['count'] > 0) {
            $diff['ids_to_delete'] = $this->dataSyncer->getIdsToDelete(
                $this->sourceConnection(),
                $this->targetConnection(),
                $table,
                $primaryKey,
                $this->getBatchSize(),
                $this->retryCallback(),
            );
        }

        $needsSync = !empty($diff['ids_to_delete'])
            || $remote['count'] != $local['count']
            || $remote['max_id'] != $local['max_id'];

        $diff['needs_sync'] = $needsSync;

        if ($local['has_updated_at'] && $remote['has_updated_at']) {
            if ($remote['max_updated_at'] != $local['max_updated_at']) {
                $diff['needs_sync'] = true;
                $diff['has_updates'] = true;
            }
        }

        return $diff;
    }

    /**
     * Build synchronization plan
     */
    protected function buildSyncPlan(array $analysis, array $tablesToRefresh = []): array
    {
        $tablesToSync = array_filter($analysis, fn ($diff) => $diff['needs_sync']);

        foreach ($tablesToRefresh as $table) {
            foreach ($tablesToSync as &$diff) {
                if ($diff['table'] === $table) {
                    $diff['refreshed'] = true;
                    break;
                }
            }
            unset($diff);
        }

        return $this->addDependentTables($tablesToSync, $analysis);
    }

    /**
     * Add dependent (parent) tables
     */
    protected function addDependentTables(array $tablesToSync, array $allAnalysis): array
    {
        $graph = $this->dependencyGraph->getGraph() ?? [];
        $tableNames = array_column($tablesToSync, 'table');

        foreach ($tableNames as $table) {
            if (!isset($graph[$table])) {
                continue;
            }
            foreach ($graph[$table]['depends_on'] as $parentTable) {
                if (!in_array($parentTable, $tableNames)) {
                    $parentAnalysis = collect($allAnalysis)->firstWhere('table', $parentTable);
                    if ($parentAnalysis) {
                        $parentAnalysis['is_parent'] = true;
                        $tablesToSync[] = $parentAnalysis;
                        $tableNames[] = $parentTable;
                    }
                }
            }
        }

        return $tablesToSync;
    }

    /**
     * Filter tables with actual changes
     */
    protected function filterTablesWithChanges(array $tablesToSync): array
    {
        return array_filter($tablesToSync, fn ($diff) =>
            !empty($diff['refreshed'])
            || !empty($diff['ids_to_delete'])
            || $diff['local_count'] != $diff['remote_count']
            || !empty($diff['has_updates'])
            || !empty($diff['is_child'])
        );
    }

    /**
     * Synchronize a set of tables
     */
    protected function syncTables(array $tablesToSync, string $phase = 'main', bool $skipChildren = false, bool $showTable = true): bool
    {
        if (empty($tablesToSync)) {
            return false;
        }

        foreach ($tablesToSync as $diff) {
            $this->tableAnalysis[$diff['table']] = $diff;
        }

        $tablesToProcess = $this->filterTablesWithChanges($tablesToSync);

        if (empty($tablesToProcess)) {
            $label = $phase === 'cascade' ? 'CASCADE RECHECK — ' : '';
            $this->info("✓ {$label}No changes to synchronize!");
            $this->newLine();
            return false;
        }

        if ($showTable) {
            $title = $phase === 'cascade'
                ? 'CASCADE RECHECK — will be synchronized:'
                : 'Will be synchronized:';
            $this->displayChangesTable($tablesToProcess, $title);
        }

        // Initialize results
        foreach ($tablesToProcess as $diff) {
            $table = $diff['table'];
            if (!isset($this->syncResults[$table])) {
                $this->syncResults[$table] = ['inserted' => 0, 'updated' => 0, 'deleted' => 0, 'errors' => 0];
            }
        }

        // DELETE phase
        $this->executeSyncDeletePhase($tablesToProcess, $phase);

        // UPSERT phase
        $tablesToUpsert = $skipChildren
            ? array_filter($tablesToProcess, fn ($diff) => empty($diff['is_child']))
            : $tablesToProcess;

        $this->executeSyncUpsertPhase($tablesToUpsert, $phase);

        return true;
    }

    /**
     * DELETE phase
     */
    protected function executeSyncDeletePhase(array $tablesToProcess, string $phase = 'main'): void
    {
        $tablesToDelete = array_filter($tablesToProcess, fn ($diff) =>
            empty($diff['refreshed']) && !empty($diff['ids_to_delete'])
        );

        $prefix = $phase === 'cascade' ? 'CASCADE RECHECK — ' : '';

        if (empty($tablesToDelete)) {
            $this->info("🗑️  {$prefix}No records to delete");
            $this->newLine();
            return;
        }

        $deleteTableNames = array_column($tablesToDelete, 'table');
        $deleteOrder = $this->dependencyGraph->sortByDependencies($deleteTableNames, 'children_first');
        $totalDelete = count($deleteOrder);

        $this->info("🗑️  {$prefix}Deleting records...");

        $num = 0;
        foreach ($deleteOrder as $table) {
            $num++;
            $linePrefix = "   [{$num}/{$totalDelete}] {$table}";

            $idsToDelete = $this->tableAnalysis[$table]['ids_to_delete'] ?? [];
            $primaryKey = $this->adapter->getPrimaryKeyColumn($this->sourceConnection(), $table);

            if (empty($idsToDelete) || !$primaryKey) {
                continue;
            }

            $totalToDelete = count($idsToDelete);
            $deleteBar = $this->output->createProgressBar($totalToDelete);
            $deleteBar->setFormat("{$linePrefix} [%bar%] %percent:3s%%  %current%/%max%");
            $deleteBar->display();

            $deleteStats = $this->dataSyncer->deleteFromTable(
                $this->targetConnection(),
                $table,
                $primaryKey,
                $idsToDelete,
                $this->getBatchSize(),
                $deleteBar,
            );

            $deleteBar->clear();
            $this->line("{$linePrefix} ✓ -{$totalToDelete}");

            $this->syncResults[$table]['deleted'] = $deleteStats['deleted'];
            $this->syncResults[$table]['errors'] += $deleteStats['errors'];
        }

        $this->newLine();
    }

    /**
     * UPSERT phase
     */
    protected function executeSyncUpsertPhase(array $tablesToProcess, string $phase = 'main'): void
    {
        $prefix = $phase === 'cascade' ? 'CASCADE RECHECK — ' : '';

        if (empty($tablesToProcess)) {
            $this->info("📥 {$prefix}No tables to synchronize");
            $this->newLine();
            return;
        }

        $upsertTableNames = array_column($tablesToProcess, 'table');
        $upsertOrder = $this->dependencyGraph->sortByDependencies($upsertTableNames, 'parents_first');
        $totalUpsert = count($upsertOrder);

        $this->info("📥 {$prefix}Inserting and updating records from remote...");

        foreach ($upsertOrder as $idx => $table) {
            $num = $idx + 1;
            $linePrefix = "   [{$num}/{$totalUpsert}] {$table}";

            $totalCount = $this->tableAnalysis[$table]['remote_count'] ?? null;
            if ($totalCount === null) {
                $totalCount = $this->withTunnelRetry(fn () => $this->sourceConnection()->table($table)->count());
            }

            if ($totalCount == 0) {
                $this->line("{$linePrefix} ✓");
                continue;
            }

            $recordsBar = $this->output->createProgressBar($totalCount);
            $recordsBar->setFormat("{$linePrefix} [%bar%] %percent:3s%%  %current%/%max%");
            $recordsBar->display();

            $upsertStats = $this->dataSyncer->syncTableFromRemote(
                $this->sourceConnection(),
                $this->targetConnection(),
                $table,
                $this->getBatchSize(),
                $this->retryCallback(),
                $recordsBar,
            );

            $recordsBar->clear();
            $this->line("{$linePrefix} ✓ {$totalCount}");

            $this->syncResults[$table]['inserted'] = ($this->syncResults[$table]['inserted'] ?? 0) + $upsertStats['inserted'];
            $this->syncResults[$table]['updated'] = ($this->syncResults[$table]['updated'] ?? 0) + $upsertStats['updated'];
            $this->syncResults[$table]['errors'] = ($this->syncResults[$table]['errors'] ?? 0) + $upsertStats['errors'];
        }

        $this->newLine();
    }

    /**
     * CASCADE RECHECK for child tables after deletions
     */
    protected function executeCascadeRecheck(): void
    {
        $tablesWithDeletes = [];
        foreach ($this->syncResults as $table => $stats) {
            if ($stats['deleted'] > 0) {
                $tablesWithDeletes[] = $table;
            }
        }

        foreach ($this->tableAnalysis as $table => $diff) {
            if (!empty($diff['refreshed']) && !in_array($table, $tablesWithDeletes)) {
                $tablesWithDeletes[] = $table;
            }
        }

        if (empty($tablesWithDeletes)) {
            return;
        }

        $graph = $this->dependencyGraph->getGraph() ?? [];
        $alreadySyncedTables = array_keys($this->syncResults);
        $childTablesToRecheck = [];

        foreach ($tablesWithDeletes as $parentTable) {
            if (!isset($graph[$parentTable])) {
                continue;
            }
            foreach ($graph[$parentTable]['referenced_by'] as $childTable) {
                if (in_array($childTable, $childTablesToRecheck)) {
                    continue;
                }
                if (in_array($childTable, $alreadySyncedTables)) {
                    continue;
                }
                if (!$this->option('include-excluded') && in_array($childTable, $this->syncConfig->excludedTables)) {
                    continue;
                }
                $childTablesToRecheck[] = $childTable;
            }
        }

        if (empty($childTablesToRecheck)) {
            return;
        }

        $this->info('CASCADE RECHECK — analyzing ' . count($childTablesToRecheck) . ' child tables: ' . implode(', ', $childTablesToRecheck));

        $cascadeAnalysis = [];
        foreach ($childTablesToRecheck as $table) {
            $diff = $this->compareTable($table);
            if ($diff['needs_sync']) {
                $cascadeAnalysis[] = $diff;
            }
        }

        if (empty($cascadeAnalysis)) {
            $this->info('   ✓ Child tables do not require synchronization');
            $this->newLine();
            return;
        }

        $this->syncTables($cascadeAnalysis, 'cascade', false);
    }

    /**
     * Get remote view list
     */
    protected function getRemoteViews(): array
    {
        $viewNames = $this->withTunnelRetry(fn () => $this->adapter->getViewsList($this->sourceConnection()));

        if ($this->option('views')) {
            $requestedViews = array_map('trim', explode(',', $this->option('views')));
            $viewNames = array_intersect($viewNames, $requestedViews);
        }

        return array_values($viewNames);
    }

    protected function confirmSync(): bool
    {
        if ($this->option('force') || !$this->input->isInteractive()) {
            return true;
        }

        $localDb = config("database.connections.{$this->syncConfig->target}.database");
        $this->warn('⚠ WARNING: This operation will modify data in local database "' . $localDb . '"!');
        $this->warn('⚠ Records that do not exist on remote will be deleted.');

        return $this->confirm('Continue?', false);
    }

    /**
     * Display table with planned changes
     */
    protected function displayChangesTable(array $tablesToProcess, string $title = 'Will be synchronized:'): void
    {
        $this->info($title);
        $tableRows = [];

        foreach ($tablesToProcess as $diff) {
            $stats = $this->calculateChangeStats($diff);

            $toDeleteStr = $stats['to_delete'] > 0
                ? '-' . number_format($stats['to_delete'], 0, '', ' ')
                : '-';
            $toAddStr = $stats['to_add'] > 0
                ? '+' . number_format($stats['to_add'], 0, '', ' ')
                : '-';

            $tableRows[] = [
                $diff['table'],
                number_format($diff['local_count'], 0, '', ' '),
                number_format($diff['remote_count'], 0, '', ' '),
                $toDeleteStr,
                $toAddStr,
                $stats['note'],
            ];
        }

        $this->table(['Table', 'Local', 'Remote', 'Delete', 'Add', 'Note'], $tableRows);
        $this->newLine();
    }

    /**
     * Display analysis table
     */
    protected function displayAnalysisTable(array $tablesToSync): void
    {
        $this->info('Tables requiring synchronization:');
        $tableRows = [];
        foreach ($tablesToSync as $diff) {
            $tableRows[] = [
                $diff['table'],
                number_format($diff['local_count'], 0, '', ' '),
                number_format($diff['remote_count'], 0, '', ' '),
            ];
        }
        $this->table(['Table', 'Local', 'Remote'], $tableRows);
        $this->newLine();
    }

    /**
     * Calculate change statistics for a table
     */
    protected function calculateChangeStats(array $diff): array
    {
        $isRefresh = !empty($diff['refreshed']);
        $deleteCount = count($diff['ids_to_delete'] ?? []);
        $hasUpdates = !empty($diff['has_updates']);

        if ($isRefresh) {
            return [
                'to_delete' => $diff['local_count'],
                'to_add' => $diff['remote_count'],
                'note' => 'DROP+CREATE+INSERT',
            ];
        }

        $toDelete = $deleteCount;
        $toAdd = max(0, $diff['remote_count'] - ($diff['local_count'] - $deleteCount));

        if ($diff['local_count'] == 0) {
            $hasUpdates = false;
        }

        if ($toDelete > 0 && $hasUpdates) {
            $note = 'DELETE+UPDATE';
        } elseif ($toDelete > 0 && $toAdd > 0) {
            $note = 'DELETE+INSERT';
        } elseif ($toDelete > 0) {
            $note = 'DELETE';
        } elseif ($toAdd > 0 && $hasUpdates) {
            $note = 'INSERT+UPDATE';
        } elseif ($hasUpdates) {
            $note = 'UPDATE';
        } elseif ($toAdd > 0) {
            $note = 'INSERT';
        } else {
            $note = 'UPSERT';
        }

        return ['to_delete' => $toDelete, 'to_add' => $toAdd, 'note' => $note];
    }

    /**
     * Display final synchronization results
     */
    protected function displaySyncResults(): void
    {
        $this->newLine();
        $this->info('Synchronization results:');
        $this->newLine();

        $totalInserted = $totalUpdated = $totalDeleted = $totalErrors = $syncedTables = 0;
        $tableRows = [];

        foreach ($this->syncResults as $table => $stats) {
            $hasChanges = $stats['inserted'] > 0 || $stats['updated'] > 0 || ($stats['deleted'] ?? 0) > 0;

            if ($hasChanges) {
                $syncedTables++;
            }

            $totalInserted += $stats['inserted'];
            $totalUpdated += $stats['updated'];
            $totalDeleted += $stats['deleted'] ?? 0;
            $totalErrors += $stats['errors'];

            if ($hasChanges || $stats['errors'] > 0) {
                $tableRows[] = [
                    $table,
                    $stats['inserted'] > 0 ? "+{$stats['inserted']}" : '-',
                    $stats['updated'] > 0 ? "~{$stats['updated']}" : '-',
                    ($stats['deleted'] ?? 0) > 0 ? "-{$stats['deleted']}" : '-',
                    $stats['errors'] > 0 ? "⚠ {$stats['errors']}" : '-',
                ];
            }
        }

        if (!empty($tableRows)) {
            $this->table(['Table', 'Inserted', 'Updated', 'Deleted', 'Errors'], $tableRows);
        }

        $this->newLine();
        $this->info('Total:');
        $this->info('   Tables synchronized: ' . number_format($syncedTables, 0, ',', ' '));
        $this->info('   Records inserted: ' . number_format($totalInserted, 0, ',', ' '));
        $this->info('   Records updated: ' . number_format($totalUpdated, 0, ',', ' '));
        $this->info('   Records deleted: ' . number_format($totalDeleted, 0, ',', ' '));
        if ($totalErrors > 0) {
            $this->warn('   Errors: ' . number_format($totalErrors, 0, ',', ' '));
        }
    }
}
