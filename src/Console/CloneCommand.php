<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Console;

use Illuminate\Support\Facades\DB;

/**
 * Full database clone from remote server (DROP + CREATE + SYNC)
 */
class CloneCommand extends BaseDbSyncCommand
{
    protected $signature = 'db-sync:clone
                            {--sync-connection= : Connection name from config/db-sync.php}
                            {--force : Run without confirmation}
                            {--tables= : Refresh only specified tables (comma-separated)}
                            {--views= : Refresh only specified views (comma-separated)}
                            {--include-excluded : Include excluded tables}
                            {--dry-run : Show what will be refreshed without executing}
                            {--skip-views : Do not synchronize views}
                            {--skip-backup : Skip backup creation}
                            {--skip-sync-data : Structure only, no data}
                            {--batch-size=10000 : Batch size}
                            {--memory-limit=-1 : Memory limit in MB (-1 unlimited)}';

    protected $description = 'Full database refresh from remote server (DROP + CREATE + SYNC)';

    public function handle(): int
    {
        $this->info('=== Full database refresh from remote server (DROP + CREATE + SYNC) ===');
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

        try {
            $this->connectToRemote();

            // Build dependency graph
            $this->dependencyGraph->build($this->sourceConnection());

            // Get all tables from remote (for structure recreation)
            $this->info('Fetching table list...');
            $allTableNames = $this->withTunnelRetry(fn () => $this->adapter->getTablesList($this->sourceConnection()));

            // If specific tables are specified, limit to those
            if ($this->option('tables')) {
                $requestedTables = array_map('trim', explode(',', $this->option('tables')));
                $allTableNames = array_values(array_intersect($allTableNames, $requestedTables));
            }

            $this->info('   Tables found: ' . count($allTableNames));

            // Separate tables for data sync (exclude excluded tables)
            $excludedTables = $this->syncConfig->excludedTables;
            if ($this->option('include-excluded')) {
                $syncTableNames = $allTableNames;
            } else {
                $syncTableNames = array_values(array_filter(
                    $allTableNames,
                    fn ($table) => !in_array($table, $excludedTables)
                ));
                $excludedCount = count($allTableNames) - count($syncTableNames);
                if ($excludedCount > 0) {
                    $this->info("   Excluded from data sync: {$excludedCount} tables (structure will be created)");
                }
            }
            $this->newLine();

            // Get view list
            $viewNames = [];
            if (!$this->option('skip-views')) {
                $this->info('Fetching view list...');
                $viewNames = $this->withTunnelRetry(fn () => $this->adapter->getViewsList($this->sourceConnection()));

                if ($this->option('views')) {
                    $requestedViews = array_map('trim', explode(',', $this->option('views')));
                    $viewNames = array_values(array_intersect($viewNames, $requestedViews));
                }

                // If --tables is specified without --views, skip views
                if ($this->option('tables') && !$this->option('views')) {
                    $viewNames = [];
                }

                if (!empty($viewNames)) {
                    $this->info('   Views found: ' . count($viewNames));
                }
                $this->newLine();
            }

            // Show plan
            $this->info('Tables to refresh (structure + data):');
            foreach ($syncTableNames as $table) {
                $this->info("   • {$table}");
            }

            $excludedInScope = array_values(array_filter(
                $allTableNames,
                fn ($table) => in_array($table, $excludedTables)
            ));
            if (!empty($excludedInScope) && !$this->option('include-excluded')) {
                $this->newLine();
                $this->info('Tables to refresh (structure only, no data):');
                foreach ($excludedInScope as $table) {
                    $this->info("   • {$table}");
                }
            }
            $this->newLine();

            if (!empty($viewNames)) {
                $this->info('Views to refresh:');
                foreach ($viewNames as $view) {
                    $this->info("   • {$view}");
                }
                $this->newLine();
            }

            // dry-run
            if ($this->option('dry-run')) {
                $this->info('--dry-run mode: no actual refresh will be performed');
                return self::SUCCESS;
            }

            // Confirmation
            if (!$this->confirmRefresh()) {
                $this->info('Operation cancelled.');
                return self::SUCCESS;
            }

            // Backup
            if (!$this->option('skip-backup')) {
                $this->info('Creating local database backup...');
                if (!$this->createLocalBackup()) {
                    return self::FAILURE;
                }
                $this->info('   ✓ Backup created');
                $this->newLine();
            }

            // DROP + CREATE all tables (including excluded — structure only)
            $this->info('Refreshing table structure (DROP + CREATE)...');
            $sourceConfig = $this->getSourceConnectionConfig();
            $schemaResult = $this->schemaManager->refreshTablesStructure(
                $this->sourceConnection(),
                $this->targetConnection(),
                $sourceConfig,
                $allTableNames,
                $viewNames,
            );

            $this->info("   ✓ Created: {$schemaResult['created_tables']} tables, {$schemaResult['created_sequences']} sequences, {$schemaResult['created_constraints']} constraints");
            if ($schemaResult['skipped_fk'] > 0) {
                $this->warn("   ⚠ Skipped {$schemaResult['skipped_fk']} foreign key constraints");
            }
            foreach ($schemaResult['errors'] as $error) {
                $this->warn("   ⚠ {$error}");
            }
            $this->newLine();

            // Data synchronization
            if (!$this->option('skip-sync-data')) {
                $this->info('Synchronizing data...');
                $this->newLine();

                $syncOrder = $this->dependencyGraph->sortByDependencies($syncTableNames, 'parents_first');

                $totalInserted = 0;
                $totalUpdated = 0;
                $totalErrors = 0;
                $totalTables = count($syncOrder);

                $this->info('Inserting records from remote...');

                foreach ($syncOrder as $idx => $table) {
                    $num = $idx + 1;
                    $linePrefix = "   [{$num}/{$totalTables}] {$table}";

                    $remoteMeta = $this->getTableMetadata($table, 'remote');
                    $totalCount = $remoteMeta['count'];

                    if ($totalCount == 0) {
                        $this->line("{$linePrefix} ✓");
                        continue;
                    }

                    // Progress bar
                    $recordsBar = $this->output->createProgressBar($totalCount);
                    $recordsBar->setFormat("{$linePrefix} [%bar%] %percent:3s%%  %current%/%max%");
                    $recordsBar->display();

                    $stats = $this->dataSyncer->syncTableFromRemote(
                        $this->sourceConnection(),
                        $this->targetConnection(),
                        $table,
                        $this->getBatchSize(),
                        $this->retryCallback(),
                        $recordsBar,
                    );

                    $recordsBar->clear();
                    $this->line("{$linePrefix} ✓ {$totalCount}");

                    $totalInserted += $stats['inserted'];
                    $totalUpdated += $stats['updated'];
                    $totalErrors += $stats['errors'];
                }

                $this->newLine();
                $this->info("   ✓ Records inserted: " . number_format($totalInserted, 0, ',', ' '));
                if ($totalUpdated > 0) {
                    $this->info("   ✓ Records updated: " . number_format($totalUpdated, 0, ',', ' '));
                }
                if ($totalErrors > 0) {
                    $this->warn("   ⚠ Errors: {$totalErrors}");
                }

                // Reset sequences
                $this->newLine();
                $this->info('Resetting sequences...');
                $resetCount = $this->adapter->resetSequences($this->targetConnection());
                $this->info("   Sequences reset: {$resetCount}");
                $this->newLine();
            }

            $this->info('✓ Structure and data refresh completed successfully!');
            return self::SUCCESS;

        } catch (\Exception $e) {
            $this->error('Refresh error: ' . $e->getMessage());
            return self::FAILURE;
        } finally {
            $this->closeTunnel();
        }
    }

    protected function confirmRefresh(): bool
    {
        if ($this->option('force') || !$this->input->isInteractive()) {
            return true;
        }

        $localDb = config("database.connections.{$this->syncConfig->target}.database");
        $this->warn('⚠ WARNING: This operation will completely DROP and recreate all tables in database "' . $localDb . '"!');
        $this->warn('⚠ All data will be lost and replaced with data from the remote server!');

        return $this->confirm('Continue?', false);
    }
}
