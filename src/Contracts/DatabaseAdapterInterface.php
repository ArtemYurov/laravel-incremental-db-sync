<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Contracts;

use Illuminate\Database\Connection;

interface DatabaseAdapterInterface
{
    /**
     * Get the FK dependency graph for tables.
     *
     * @return array<string, array{depends_on: string[], referenced_by: string[]}>
     */
    public function getForeignKeyDependencies(Connection $connection): array;

    /**
     * Get child tables referencing the given table.
     *
     * @return array<string, string> ['child_table' => 'fk_column', ...]
     */
    public function getChildTables(Connection $connection, string $table): array;

    /**
     * Get the self-referencing column of a table (if any).
     */
    public function getSelfReferencingColumn(Connection $connection, string $table): ?string;

    /**
     * Get the primary key column of a table.
     */
    public function getPrimaryKeyColumn(Connection $connection, string $table): ?string;

    /**
     * Get UNIQUE constraints of a table (excluding PK).
     *
     * @return array<int, array{name: string, columns: string[]}>
     */
    public function getUniqueConstraints(Connection $connection, string $table): array;

    /**
     * Reset auto-increment sequences for tables.
     *
     * @return int Number of sequences reset
     */
    public function resetSequences(Connection $connection): int;

    /**
     * Dump table schema via CLI tool (e.g. pg_dump).
     */
    public function dumpSchema(array $connectionConfig, array $tables): ?string;

    /**
     * Dump VIEW schema via CLI tool.
     */
    public function dumpViewsSchema(array $connectionConfig, array $views): ?string;

    /**
     * Parse an SQL dump into individual statements.
     *
     * @return string[]
     */
    public function parseSqlStatements(string $sql): array;

    /**
     * Create a database backup.
     *
     * @return string Path to the backup file
     */
    public function createBackup(array $connectionConfig, string $backupDir): string;

    /**
     * Restore a database from backup.
     */
    public function restoreBackup(array $connectionConfig, string $backupFile): void;

    /**
     * Get the list of tables.
     *
     * @return string[]
     */
    public function getTablesList(Connection $connection): array;

    /**
     * Get the list of views.
     *
     * @return string[]
     */
    public function getViewsList(Connection $connection): array;

    /**
     * Drop a table (CASCADE).
     */
    public function dropTable(Connection $connection, string $table): bool;

    /**
     * Drop a VIEW (CASCADE).
     */
    public function dropView(Connection $connection, string $view): void;

    /**
     * Drop the entire schema (DROP SCHEMA CASCADE + CREATE SCHEMA).
     */
    public function dropSchema(Connection $connection): void;

    /**
     * Map of indexes and constraints for every table in the schema.
     *
     * @return array<string, array<int, array{name:string,type:string,def:string}>>
     *   table => [ {name, type: 'index'|'p'|'u'|'x', def}, ... ]
     */
    public function getIndexMap(Connection $connection): array;

    /**
     * Build SQL to drop an index/constraint (constraint-aware).
     * For p/u/x — ALTER TABLE DROP CONSTRAINT; for a plain index — DROP INDEX.
     */
    public function dropIndexOrConstraintSql(string $table, string $name, string $type): string;

    /**
     * Build SQL to create an index/constraint (constraint-aware).
     * For p/u/x — ALTER TABLE ADD CONSTRAINT; for a plain index — the original CREATE INDEX.
     */
    public function createIndexOrConstraintSql(string $table, string $name, string $type, string $def): string;

    /**
     * Tables present in target (local) but missing in source (remote).
     *
     * @return string[]
     */
    public function getLocalOnlyTables(Connection $source, Connection $target): array;

    /**
     * Upsert a record via ON CONFLICT.
     *
     * @return array{inserted: int, updated: int, errors: int}
     */
    public function upsertRecord(
        Connection $connection,
        string $table,
        array $record,
        string $primaryKey,
        array $columns,
    ): array;

    /**
     * Get table metadata (count, max_id, has_updated_at, max_updated_at).
     */
    public function getTableMetadata(Connection $connection, string $table): array;

    /**
     * Check if a table exists.
     */
    public function tableExists(Connection $connection, string $table): bool;

    /**
     * Check if a VIEW exists.
     */
    public function viewExists(Connection $connection, string $view): bool;

    /**
     * Check if the table structure has changed between source and target.
     */
    public function hasStructureChanged(Connection $source, Connection $target, string $table): bool;

    /**
     * Check if the VIEW structure has changed between source and target.
     */
    public function hasViewStructureChanged(Connection $source, Connection $target, string $view): bool;

    /**
     * Get the VIEW definition.
     */
    public function getViewDefinition(Connection $connection, string $view): ?string;

    /**
     * Get self-referencing table records in correct order (recursive CTE).
     */
    public function getSelfReferencingRecords(
        Connection $connection,
        string $table,
        string $primaryKey,
        string $selfRefColumn,
    ): array;
}
