<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Tests\Unit;

use ArtemYurov\DbSync\Services\StructureDiff;
use ArtemYurov\DbSync\Tests\TestCase;

class StructureDiffTest extends TestCase
{
    /** Object names from a diff result. */
    private function names(array $entries): array
    {
        return array_map(fn ($e) => $e['name'], $entries);
    }

    public function test_local_only_tables_returns_target_minus_source(): void
    {
        $result = StructureDiff::localOnlyTables(
            ['users', 'orders', 'registration_request_logs'], // local (target)
            ['users', 'orders', 'verification_request_logs'],  // remote (source)
        );

        $this->assertSame(['registration_request_logs'], $result);
    }

    public function test_excluded_tables_are_never_local_only(): void
    {
        // Excluded tables exist on source, so they never appear in the set difference.
        $result = StructureDiff::localOnlyTables(
            ['users', 'sessions', 'cache'],
            ['users', 'sessions', 'cache', 'orders'],
        );

        $this->assertSame([], $result);
    }

    public function test_target_only_index_goes_to_drop(): void
    {
        $source = ['t' => [
            ['name' => 't_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
        ]];
        $target = ['t' => [
            ['name' => 't_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
            ['name' => 't_extra_idx', 'type' => 'index', 'def' => 'CREATE INDEX t_extra_idx ON public.t USING btree (name)'],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertSame(['t_extra_idx'], $this->names($diff['drop']));
        $this->assertSame([], $this->names($diff['create']));
    }

    public function test_source_only_index_goes_to_create(): void
    {
        $source = ['t' => [
            ['name' => 't_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
            ['name' => 't_name_idx', 'type' => 'index', 'def' => 'CREATE INDEX t_name_idx ON public.t USING btree (name)'],
        ]];
        $target = ['t' => [
            ['name' => 't_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertSame([], $this->names($diff['drop']));
        $this->assertSame(['t_name_idx'], $this->names($diff['create']));
    }

    public function test_index_rename_is_detected_as_drop_old_plus_create_new(): void
    {
        // Rename: same definition, different names. Name-based comparison must yield
        // drop(old) + create(new).
        $source = ['t' => [
            ['name' => 'new_idx', 'type' => 'index', 'def' => 'CREATE INDEX new_idx ON public.t USING btree (a)'],
        ]];
        $target = ['t' => [
            ['name' => 'old_idx', 'type' => 'index', 'def' => 'CREATE INDEX old_idx ON public.t USING btree (a)'],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertSame(['old_idx'], $this->names($diff['drop']));
        $this->assertSame(['new_idx'], $this->names($diff['create']));
    }

    public function test_changed_definition_same_name_goes_to_both_drop_and_create(): void
    {
        $source = ['t' => [
            ['name' => 't_idx', 'type' => 'index', 'def' => 'CREATE INDEX t_idx ON public.t USING btree (a, b)'],
        ]];
        $target = ['t' => [
            ['name' => 't_idx', 'type' => 'index', 'def' => 'CREATE INDEX t_idx ON public.t USING btree (a)'],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertSame(['t_idx'], $this->names($diff['drop']));
        $this->assertSame(['t_idx'], $this->names($diff['create']));
    }

    public function test_whitespace_only_difference_is_not_a_diff(): void
    {
        $source = ['t' => [
            ['name' => 't_idx', 'type' => 'index', 'def' => 'CREATE INDEX t_idx ON public.t USING btree (a)'],
        ]];
        $target = ['t' => [
            ['name' => 't_idx', 'type' => 'index', 'def' => "CREATE INDEX t_idx ON public.t\n  USING btree (a)"],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertSame([], $diff['drop']);
        $this->assertSame([], $diff['create']);
    }

    public function test_excluded_tables_are_skipped_entirely(): void
    {
        $source = ['telescope_entries' => [
            ['name' => 'telescope_entries_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (uuid)'],
        ]];
        $target = ['telescope_entries' => []]; // no PK on target, but the table is excluded

        $diff = StructureDiff::indexDiff($source, $target, ['telescope_entries']);

        $this->assertSame([], $diff['drop']);
        $this->assertSame([], $diff['create']);
    }

    public function test_carries_constraint_type_for_constraint_backed_objects(): void
    {
        // Real-bug scenario: orphan PK in target, missing PK on the intended table.
        $source = ['verification_request_logs' => [
            ['name' => 'registration_request_logs_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
        ]];
        $target = ['registration_request_logs' => [
            ['name' => 'registration_request_logs_pkey', 'type' => 'p', 'def' => 'PRIMARY KEY (id)'],
        ]];

        $diff = StructureDiff::indexDiff($source, $target);

        $this->assertCount(1, $diff['drop']);
        $this->assertSame('registration_request_logs', $diff['drop'][0]['table']);
        $this->assertSame('p', $diff['drop'][0]['type']);

        $this->assertCount(1, $diff['create']);
        $this->assertSame('verification_request_logs', $diff['create'][0]['table']);
        $this->assertSame('p', $diff['create'][0]['type']);
    }
}
