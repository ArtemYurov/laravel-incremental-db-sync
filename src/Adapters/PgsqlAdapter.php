<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Adapters;

use ArtemYurov\DbSync\Contracts\DatabaseAdapterInterface;
use ArtemYurov\DbSync\Exceptions\AdapterException;
use Illuminate\Database\Connection;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

class PgsqlAdapter implements DatabaseAdapterInterface
{
    public function getForeignKeyDependencies(Connection $connection): array
    {
        $query = "
            SELECT tc.table_name, ccu.table_name AS foreign_table_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'
            ORDER BY tc.table_name, ccu.table_name
        ";

        $dependencies = $connection->select($query);
        $graph = [];

        foreach ($dependencies as $dep) {
            $table = $dep->table_name;
            $foreignTable = $dep->foreign_table_name;

            if (!isset($graph[$table])) {
                $graph[$table] = ['depends_on' => [], 'referenced_by' => []];
            }
            if (!isset($graph[$foreignTable])) {
                $graph[$foreignTable] = ['depends_on' => [], 'referenced_by' => []];
            }

            if (!in_array($foreignTable, $graph[$table]['depends_on'])) {
                $graph[$table]['depends_on'][] = $foreignTable;
            }
            if (!in_array($table, $graph[$foreignTable]['referenced_by'])) {
                $graph[$foreignTable]['referenced_by'][] = $table;
            }
        }

        return $graph;
    }

    public function getChildTables(Connection $connection, string $table): array
    {
        $query = "
            SELECT
                tc.table_name as child_table,
                kcu.column_name as fk_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND ccu.table_name = ?
              AND tc.table_name != ?
        ";

        $results = $connection->select($query, [$table, $table]);

        $children = [];
        foreach ($results as $row) {
            $children[$row->child_table] = $row->fk_column;
        }

        return $children;
    }

    public function getSelfReferencingColumn(Connection $connection, string $table): ?string
    {
        $query = "
            SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name = ?
              AND ccu.table_name = ?
        ";

        $result = $connection->select($query, [$table, $table]);

        return $result[0]->column_name ?? null;
    }

    public function getPrimaryKeyColumn(Connection $connection, string $table): ?string
    {
        $result = $connection->select(
            "SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ?::regclass AND i.indisprimary",
            ["public.{$table}"]
        );

        return $result[0]->column_name ?? null;
    }

    public function getUniqueConstraints(Connection $connection, string $table): array
    {
        $query = "
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.table_name = ?
              AND tc.constraint_type = 'UNIQUE'
            GROUP BY tc.constraint_name
        ";

        $results = $connection->select($query, [$table]);

        $constraints = [];
        foreach ($results as $row) {
            // PostgreSQL returns arrays as strings like {col1,col2}
            $columns = trim($row->columns, '{}');
            $columns = $columns ? explode(',', $columns) : [];
            $constraints[] = [
                'name' => $row->constraint_name,
                'columns' => $columns,
            ];
        }

        return $constraints;
    }

    public function resetSequences(Connection $connection): int
    {
        $sequences = $connection->select("
            SELECT t.table_name, c.column_name, pg_get_serial_sequence(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name), c.column_name) as sequence_name
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE' AND c.column_default LIKE 'nextval%'
        ");

        $resetCount = 0;
        foreach ($sequences as $seq) {
            if (!$seq->sequence_name) {
                continue;
            }

            try {
                $connection->statement(
                    "SELECT setval('{$seq->sequence_name}', COALESCE((SELECT MAX({$seq->column_name}) FROM {$seq->table_name}), 1), COALESCE((SELECT MAX({$seq->column_name}) FROM {$seq->table_name}), 1) IS NOT NULL)"
                );
                $resetCount++;
            } catch (\Exception $e) {
                // Skip errors for individual sequences
            }
        }

        return $resetCount;
    }

    public function dumpSchema(array $connectionConfig, array $tables): ?string
    {
        if (empty($tables)) {
            return '';
        }

        $tableParams = '';
        foreach ($tables as $table) {
            $tableParams .= ' -t ' . escapeshellarg($table);
        }

        $command = sprintf(
            'PGPASSWORD=%s pg_dump %s --schema-only --no-owner --no-acl --no-privileges -h %s -p %s -U %s %s',
            escapeshellarg($connectionConfig['password']),
            $tableParams,
            escapeshellarg($connectionConfig['host']),
            escapeshellarg((string) $connectionConfig['port']),
            escapeshellarg($connectionConfig['username']),
            escapeshellarg($connectionConfig['database'])
        );

        return $this->runProcess($command, 300);
    }

    public function dumpViewsSchema(array $connectionConfig, array $views): ?string
    {
        if (empty($views)) {
            return '';
        }

        $viewParams = '';
        foreach ($views as $view) {
            $viewParams .= ' -t ' . escapeshellarg($view);
        }

        $command = sprintf(
            'PGPASSWORD=%s pg_dump %s --schema-only --no-owner --no-acl --no-privileges -h %s -p %s -U %s %s',
            escapeshellarg($connectionConfig['password']),
            $viewParams,
            escapeshellarg($connectionConfig['host']),
            escapeshellarg((string) $connectionConfig['port']),
            escapeshellarg($connectionConfig['username']),
            escapeshellarg($connectionConfig['database'])
        );

        return $this->runProcess($command, 300);
    }

    public function parseSqlStatements(string $sql): array
    {
        $lines = explode("\n", $sql);
        $statements = [];
        $currentStatement = '';

        foreach ($lines as $line) {
            $line = trim($line);

            if (empty($line) || str_starts_with($line, '--')) {
                continue;
            }

            // Skip psql meta-commands (\restrict, \unrestrict, etc.)
            if (str_starts_with($line, '\\')) {
                continue;
            }

            if (str_starts_with($line, 'SET ') || str_starts_with($line, 'SELECT pg_catalog.set_config')) {
                continue;
            }

            $currentStatement .= $line . "\n";

            if (str_ends_with($line, ';')) {
                $statements[] = trim($currentStatement);
                $currentStatement = '';
            }
        }

        if (!empty(trim($currentStatement))) {
            $statements[] = trim($currentStatement);
        }

        return $statements;
    }

    public function createBackup(array $connectionConfig, string $backupDir): string
    {
        if (!is_dir($backupDir)) {
            mkdir($backupDir, 0755, true);
        }

        $backupFile = $backupDir . '/db_backup_' . date('Y-m-d_H-i-s') . '.sql.gz';

        $command = sprintf(
            'PGPASSWORD=%s pg_dump -h %s -p %s -U %s -F p --no-owner --no-acl %s | gzip > %s',
            escapeshellarg($connectionConfig['password']),
            escapeshellarg($connectionConfig['host']),
            escapeshellarg((string) $connectionConfig['port']),
            escapeshellarg($connectionConfig['username']),
            escapeshellarg($connectionConfig['database']),
            escapeshellarg($backupFile)
        );

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(3600);

        try {
            $process->mustRun();
        } catch (ProcessFailedException $e) {
            throw new AdapterException("Failed to create backup: {$e->getMessage()}", 0, $e);
        }

        return $backupFile;
    }

    public function restoreBackup(array $connectionConfig, string $backupFile): void
    {
        $command = sprintf(
            'gunzip -c %s | PGPASSWORD=%s psql -h %s -p %s -U %s -d %s -q 2>&1',
            escapeshellarg($backupFile),
            escapeshellarg($connectionConfig['password']),
            escapeshellarg($connectionConfig['host']),
            escapeshellarg((string) $connectionConfig['port']),
            escapeshellarg($connectionConfig['username']),
            escapeshellarg($connectionConfig['database'])
        );

        $process = Process::fromShellCommandline($command);
        $process->setTimeout(3600);

        $process->run();

        // Check for real errors (not "already exists")
        if (!$process->isSuccessful()) {
            $output = $process->getOutput() . $process->getErrorOutput();
            foreach (explode("\n", $output) as $line) {
                if (str_contains($line, 'ERROR:') && !str_contains($line, 'already exists')) {
                    throw new AdapterException("Backup restore error: " . trim($line));
                }
            }
        }
    }

    public function getTablesList(Connection $connection): array
    {
        $tables = $connection->select("
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        ");

        return array_map(fn($t) => $t->table_name, $tables);
    }

    public function getViewsList(Connection $connection): array
    {
        $views = $connection->select("
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'VIEW'
            ORDER BY table_name
        ");

        return array_map(fn($v) => $v->table_name, $views);
    }

    public function dropTable(Connection $connection, string $table): bool
    {
        try {
            $connection->statement("DROP TABLE IF EXISTS \"{$table}\" CASCADE");
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    public function dropView(Connection $connection, string $view): void
    {
        try {
            $connection->statement("DROP VIEW IF EXISTS \"{$view}\" CASCADE");
        } catch (\Exception $e) {
            // Ignore VIEW drop errors
        }
    }

    public function dropSchema(Connection $connection): void
    {
        $config = $connection->getConfig();
        $username = $config['username'];

        $connection->statement('DROP SCHEMA public CASCADE');
        $connection->statement('CREATE SCHEMA public');
        $connection->statement("GRANT ALL ON SCHEMA public TO {$username}");
        $connection->statement('GRANT ALL ON SCHEMA public TO public');
    }

    public function upsertRecord(
        Connection $connection,
        string $table,
        array $record,
        string $primaryKey,
        array $columns,
    ): array {
        $stats = ['inserted' => 0, 'updated' => 0, 'errors' => 0, 'error_messages' => []];

        $updateSet = [];
        foreach ($columns as $column) {
            if ($column !== $primaryKey) {
                $updateSet[] = "\"{$column}\" = EXCLUDED.\"{$column}\"";
            }
        }

        $quotedColumns = array_map(fn($c) => "\"{$c}\"", $columns);
        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $sql = "INSERT INTO \"{$table}\" (" . implode(', ', $quotedColumns) . ") VALUES ({$placeholders}) ON CONFLICT (\"{$primaryKey}\") DO UPDATE SET " . implode(', ', $updateSet);

        try {
            $affected = $connection->affectingStatement($sql, array_values($record));
            if ($affected > 0) {
                $stats['updated']++;
            } else {
                $stats['inserted']++;
            }
        } catch (\Exception $e) {
            $stats['errors']++;
            $stats['error_messages'][] = ['id' => $record[$primaryKey] ?? null, 'message' => $e->getMessage()];
        }

        return $stats;
    }

    public function getTableMetadata(Connection $connection, string $table): array
    {
        $metadata = [
            'table' => $table,
            'count' => 0,
            'has_updated_at' => false,
            'max_updated_at' => null,
            'max_id' => null,
            'error' => false,
        ];

        try {
            $columnCheck = $connection->select(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = 'updated_at'",
                [$table]
            );
            $metadata['has_updated_at'] = count($columnCheck) > 0;

            $count = $connection->select("SELECT COUNT(*) as count FROM \"{$table}\"");
            $metadata['count'] = $count[0]->count ?? 0;

            try {
                $maxId = $connection->select("SELECT MAX(id) as max_id FROM \"{$table}\"");
                $metadata['max_id'] = $maxId[0]->max_id ?? null;
            } catch (\Exception $e) {
                // Table without id column is normal
            }

            if ($metadata['has_updated_at'] && $metadata['count'] > 0) {
                $maxUpdated = $connection->select("SELECT MAX(updated_at) as max_updated_at FROM \"{$table}\"");
                $metadata['max_updated_at'] = $maxUpdated[0]->max_updated_at ?? null;
            }
        } catch (\Exception $e) {
            $metadata['error'] = true;
        }

        return $metadata;
    }

    public function tableExists(Connection $connection, string $table): bool
    {
        $result = $connection->select(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?) as exists",
            [$table]
        );

        return (bool) ($result[0]->exists ?? false);
    }

    public function viewExists(Connection $connection, string $view): bool
    {
        $result = $connection->select(
            "SELECT EXISTS (SELECT FROM information_schema.views WHERE table_schema = 'public' AND table_name = ?) as exists",
            [$view]
        );

        return (bool) ($result[0]->exists ?? false);
    }

    public function hasStructureChanged(Connection $source, Connection $target, string $table): bool
    {
        $query = "SELECT column_name, data_type, udt_name, is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? ORDER BY ordinal_position";

        $sourceColumns = $source->select($query, [$table]);
        $targetColumns = $target->select($query, [$table]);

        if (count($sourceColumns) !== count($targetColumns)) {
            return true;
        }

        $targetByName = collect($targetColumns)->keyBy('column_name');
        foreach ($sourceColumns as $sourceCol) {
            $targetCol = $targetByName->get($sourceCol->column_name);
            if (!$targetCol
                || $sourceCol->data_type !== $targetCol->data_type
                || $sourceCol->udt_name !== $targetCol->udt_name
                || $sourceCol->is_nullable !== $targetCol->is_nullable
            ) {
                return true;
            }
        }

        return false;
    }

    public function hasViewStructureChanged(Connection $source, Connection $target, string $view): bool
    {
        try {
            $sourceView = $source->select("SELECT pg_get_viewdef(?, true) as definition", [$view]);
            $targetView = $target->select("SELECT pg_get_viewdef(?, true) as definition", [$view]);

            return ($sourceView[0]->definition ?? '') !== ($targetView[0]->definition ?? '');
        } catch (\Exception $e) {
            return true;
        }
    }

    public function getViewDefinition(Connection $connection, string $view): ?string
    {
        try {
            $result = $connection->select("SELECT pg_get_viewdef(?, true) as definition", [$view]);
            return $result[0]->definition ?? null;
        } catch (\Exception $e) {
            return null;
        }
    }

    public function getSelfReferencingRecords(
        Connection $connection,
        string $table,
        string $primaryKey,
        string $selfRefColumn,
    ): array {
        $query = "
            WITH RECURSIVE tree AS (
                SELECT *, 0 as depth
                FROM \"{$table}\"
                WHERE \"{$selfRefColumn}\" IS NULL

                UNION ALL

                SELECT t.*, tree.depth + 1
                FROM \"{$table}\" t
                INNER JOIN tree ON t.\"{$selfRefColumn}\" = tree.\"{$primaryKey}\"
            )
            SELECT * FROM tree ORDER BY depth, \"{$primaryKey}\"
        ";

        return $connection->select($query);
    }

    /**
     * Run a shell process and return its output.
     */
    protected function runProcess(string $command, int $timeout = 300): ?string
    {
        $process = Process::fromShellCommandline($command);
        $process->setTimeout($timeout);

        try {
            $process->mustRun();
            return $process->getOutput();
        } catch (ProcessFailedException $e) {
            throw new AdapterException("Process execution failed: {$e->getMessage()}", 0, $e);
        }
    }
}
