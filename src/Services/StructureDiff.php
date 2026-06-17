<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Services;

/**
 * Pure (DB-free) structure comparison logic between source and target:
 * index/constraint diff and local-only table computation.
 *
 * Kept as a separate class with static methods so it can be unit-tested
 * without a live database connection.
 *
 * Index/constraint object shape (item of a table map):
 *   ['name' => string, 'type' => 'index'|'p'|'u'|'x', 'def' => string]
 * where type:
 *   index — plain index,
 *   p — PRIMARY KEY, u — UNIQUE, x — EXCLUSION (constraint-backed).
 *
 * Map: ['table_name' => [ {name,type,def}, ... ], ...]
 */
class StructureDiff
{
    /**
     * Tables that exist locally (target) but are missing on remote (source) — orphans.
     *
     * Excluded tables are NOT treated as orphans, since they exist on source and
     * therefore never appear in the set difference.
     *
     * @param  string[]  $localTables   target table list
     * @param  string[]  $remoteTables  source table list
     * @return string[]
     */
    public static function localOnlyTables(array $localTables, array $remoteTables): array
    {
        return array_values(array_diff($localTables, $remoteTables));
    }

    /**
     * Index/constraint diff between source (reference) and target (local).
     *
     * Comparison is done BY NAME (table+name) — otherwise an index rename is not
     * detected (its definition stays the same). Definitions are compared after
     * normalization (see normalizeDefinition) to avoid false diffs from cosmetics.
     *
     * Returns two sets:
     *   - drop:   present in target but not in source (or changed) → drop;
     *   - create: present in source but not in target (or changed) → create.
     * The consumer MUST apply ALL drops first, then ALL creates
     * (to free occupied names before recreating).
     *
     * @param  array  $sourceMap        source map: table => [{name,type,def}]
     * @param  array  $targetMap        target map: table => [{name,type,def}]
     * @param  string[]  $excludedTables tables excluded from the comparison
     * @return array{drop: array<int, array{table:string,name:string,type:string,def:string}>, create: array<int, array{table:string,name:string,type:string,def:string}>}
     */
    public static function indexDiff(array $sourceMap, array $targetMap, array $excludedTables = []): array
    {
        $drop = [];
        $create = [];
        $excluded = array_flip($excludedTables);

        $tables = array_unique(array_merge(array_keys($sourceMap), array_keys($targetMap)));
        sort($tables);

        foreach ($tables as $table) {
            if (isset($excluded[$table])) {
                continue;
            }

            $sourceByName = self::indexByName($sourceMap[$table] ?? []);
            $targetByName = self::indexByName($targetMap[$table] ?? []);

            // target-only names and changed definitions → DROP (remove the old object from target)
            foreach ($targetByName as $name => $obj) {
                $source = $sourceByName[$name] ?? null;

                if ($source === null
                    || self::normalizeDefinition($obj['def']) !== self::normalizeDefinition($source['def'])
                ) {
                    $drop[] = self::entry($table, $obj);
                }
            }

            // source-only names and changed definitions → CREATE (create the new object from source)
            foreach ($sourceByName as $name => $obj) {
                $target = $targetByName[$name] ?? null;

                if ($target === null
                    || self::normalizeDefinition($obj['def']) !== self::normalizeDefinition($target['def'])
                ) {
                    $create[] = self::entry($table, $obj);
                }
            }
        }

        return ['drop' => $drop, 'create' => $create];
    }

    /**
     * Normalize an index/constraint definition for stable comparison.
     *
     * Collapses any run of whitespace into a single space and trims the edges —
     * guards against false diffs caused by formatting differences between
     * PostgreSQL versions.
     */
    public static function normalizeDefinition(string $def): string
    {
        return trim((string) preg_replace('/\s+/', ' ', $def));
    }

    /**
     * Index a list of objects by name.
     *
     * @param  array<int, array{name:string,type:string,def:string}>  $objects
     * @return array<string, array{name:string,type:string,def:string}>
     */
    protected static function indexByName(array $objects): array
    {
        $byName = [];
        foreach ($objects as $obj) {
            $byName[$obj['name']] = $obj;
        }

        return $byName;
    }

    /**
     * @param  array{name:string,type:string,def:string}  $obj
     * @return array{table:string,name:string,type:string,def:string}
     */
    protected static function entry(string $table, array $obj): array
    {
        return [
            'table' => $table,
            'name' => $obj['name'],
            'type' => $obj['type'],
            'def' => $obj['def'],
        ];
    }
}
