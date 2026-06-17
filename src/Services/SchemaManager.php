<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Services;

use ArtemYurov\DbSync\Contracts\DatabaseAdapterInterface;
use Illuminate\Database\Connection;

class SchemaManager
{
    public function __construct(
        protected DatabaseAdapterInterface $adapter,
        protected DependencyGraph $dependencyGraph,
    ) {}

    /**
     * Recreate table structure (DROP + CREATE).
     *
     * @return array{created_tables: int, created_sequences: int, created_constraints: int, skipped_fk: int, errors: string[]}
     */
    public function refreshTablesStructure(
        Connection $source,
        Connection $target,
        array $sourceConfig,
        array $tables,
        array $views = [],
    ): array {
        $result = [
            'created_tables' => 0,
            'created_sequences' => 0,
            'created_constraints' => 0,
            'skipped_fk' => 0,
            'errors' => [],
        ];

        // Drop tables in correct order (children -> parents)
        if (!empty($tables)) {
            $dropOrder = $this->dependencyGraph->sortByDependencies($tables, 'children_first');
            foreach ($dropOrder as $table) {
                $this->adapter->dropTable($target, $table);
            }

            // Get creation order (parents -> children)
            $createOrder = $this->dependencyGraph->sortByDependencies($tables, 'parents_first');

            $schema = $this->adapter->dumpSchema($sourceConfig, $createOrder);
            if (!$schema) {
                $result['errors'][] = 'Failed to retrieve table schema';
                return $result;
            }

            $statements = $this->adapter->parseSqlStatements($schema);
            foreach ($statements as $statement) {
                $statement = trim($statement);
                if (empty($statement) || $statement === '--') {
                    continue;
                }

                try {
                    $target->unprepared($statement);

                    if (stripos($statement, 'CREATE TABLE') !== false) {
                        $result['created_tables']++;
                    } elseif (stripos($statement, 'CREATE SEQUENCE') !== false) {
                        $result['created_sequences']++;
                    } elseif (stripos($statement, 'ADD CONSTRAINT') !== false) {
                        $result['created_constraints']++;
                    }
                } catch (\Exception $e) {
                    if (stripos($statement, 'FOREIGN KEY') !== false && str_contains($e->getMessage(), 'does not exist')) {
                        $result['skipped_fk']++;
                    } else {
                        $result['errors'][] = $e->getMessage();
                    }
                }
            }
        }

        // Refresh views
        if (!empty($views)) {
            foreach ($views as $view) {
                $this->adapter->dropView($target, $view);
            }

            $viewsSchema = $this->adapter->dumpViewsSchema($sourceConfig, $views);
            if ($viewsSchema) {
                $viewStatements = $this->adapter->parseSqlStatements($viewsSchema);
                foreach ($viewStatements as $statement) {
                    $statement = trim($statement);
                    if (empty($statement) || $statement === '--') {
                        continue;
                    }

                    try {
                        $target->unprepared($statement);
                    } catch (\Exception $e) {
                        $result['errors'][] = "VIEW: {$e->getMessage()}";
                    }
                }
            }
        }

        return $result;
    }

    /**
     * Find tables and views that need recreation.
     *
     * @return array{missing_tables: string[], changed_tables: string[], missing_views: string[], changed_views: string[]}
     */
    public function findTablesNeedingRefresh(
        Connection $source,
        Connection $target,
        array $remoteTables,
        array $remoteViews = [],
    ): array {
        $result = [
            'missing_tables' => [],
            'changed_tables' => [],
            'missing_views' => [],
            'changed_views' => [],
        ];

        foreach ($remoteTables as $table) {
            if (!$this->adapter->tableExists($target, $table)) {
                $result['missing_tables'][] = $table;
            } elseif ($this->adapter->hasStructureChanged($source, $target, $table)) {
                $result['changed_tables'][] = $table;
            }
        }

        foreach ($remoteViews as $view) {
            if (!$this->adapter->viewExists($target, $view)) {
                $result['missing_views'][] = $view;
            } elseif ($this->adapter->hasViewStructureChanged($source, $target, $view)) {
                $result['changed_views'][] = $view;
            }
        }

        return $result;
    }

    /**
     * Build the index/constraint reconciliation plan: what to drop and what to create
     * in target to bring its structure in line with source. Read-only, changes nothing.
     *
     * Name-based comparison (see StructureDiff::indexDiff) catches an index rename as a
     * drop(old)+create(new) pair.
     *
     * @param  string[]  $excludedTables  tables excluded from the comparison
     * @return array{drop: array<int, array{table:string,name:string,type:string,def:string}>, create: array<int, array{table:string,name:string,type:string,def:string}>}
     */
    public function planIndexReconcile(
        Connection $source,
        Connection $target,
        array $excludedTables = [],
    ): array {
        return StructureDiff::indexDiff(
            $this->adapter->getIndexMap($source),
            $this->adapter->getIndexMap($target),
            $excludedTables,
        );
    }

    /**
     * Apply the index/constraint reconciliation plan to target.
     *
     * Constraint-aware: PK/UNIQUE/EXCLUSION are dropped/created via ALTER TABLE, plain
     * indexes via DROP/CREATE INDEX. Strict order: ALL DROPs first, then all CREATEs —
     * to free occupied names before recreating.
     *
     * @param  array{drop: array<int, array{table:string,name:string,type:string,def:string}>, create: array<int, array{table:string,name:string,type:string,def:string}>}  $plan
     * @return array{dropped: array<int, array{table:string,name:string,type:string}>, created: array<int, array{table:string,name:string,type:string}>, errors: string[]}
     */
    public function applyIndexReconcile(Connection $target, array $plan): array
    {
        $report = ['dropped' => [], 'created' => [], 'errors' => []];

        // 1) All drops first — free occupied names.
        foreach ($plan['drop'] as $obj) {
            $sql = $this->adapter->dropIndexOrConstraintSql($obj['table'], $obj['name'], $obj['type']);
            try {
                $target->unprepared($sql);
                $report['dropped'][] = ['table' => $obj['table'], 'name' => $obj['name'], 'type' => $obj['type']];
            } catch (\Exception $e) {
                $report['errors'][] = "DROP {$obj['name']} ({$obj['table']}): {$e->getMessage()}";
            }
        }

        // 2) Then all creates.
        foreach ($plan['create'] as $obj) {
            $sql = $this->adapter->createIndexOrConstraintSql($obj['table'], $obj['name'], $obj['type'], $obj['def']);
            try {
                $target->unprepared($sql);
                $report['created'][] = ['table' => $obj['table'], 'name' => $obj['name'], 'type' => $obj['type']];
            } catch (\Exception $e) {
                $report['errors'][] = "CREATE {$obj['name']} ({$obj['table']}): {$e->getMessage()}";
            }
        }

        return $report;
    }
}
