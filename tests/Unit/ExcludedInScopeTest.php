<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Tests\Unit;

use ArtemYurov\DbSync\Console\BaseDbSyncCommand;
use ArtemYurov\DbSync\Tests\TestCase;

class ExcludedInScopeTest extends TestCase
{
    private BaseDbSyncCommand $command;

    protected function setUp(): void
    {
        parent::setUp();

        $this->command = new class extends BaseDbSyncCommand {
            protected $signature = 'test:excluded-in-scope';
            protected $description = 'Test command';

            public function handle(): int
            {
                return self::SUCCESS;
            }

            public function callGetExcludedInScope(array $allTableNames, array $excludedTables, array $syncTableNames): array
            {
                return $this->getExcludedInScope($allTableNames, $excludedTables, $syncTableNames);
            }
        };
    }

    public function test_excluded_tables_appear_in_scope(): void
    {
        $result = $this->command->callGetExcludedInScope(
            ['users', 'sessions', 'cache', 'orders'],
            ['sessions', 'cache'],
            ['users', 'orders'], // syncTableNames — only non-excluded
        );

        $this->assertEquals(['sessions', 'cache'], $result);
    }

    public function test_explicitly_synced_excluded_table_not_in_scope(): void
    {
        // Table sessions is excluded but also in syncTableNames (via --tables)
        // It must not be duplicated in the "structure only" list
        $result = $this->command->callGetExcludedInScope(
            ['users', 'sessions', 'cache', 'orders'],
            ['sessions', 'cache'],
            ['users', 'sessions', 'orders'], // sessions explicitly requested
        );

        $this->assertEquals(['cache'], $result);
    }

    public function test_all_excluded_synced_returns_empty(): void
    {
        // All excluded tables are explicitly requested via --tables
        $result = $this->command->callGetExcludedInScope(
            ['users', 'sessions', 'cache', 'orders'],
            ['sessions', 'cache'],
            ['users', 'sessions', 'cache', 'orders'],
        );

        $this->assertEquals([], $result);
    }

    public function test_no_excluded_tables_returns_empty(): void
    {
        $result = $this->command->callGetExcludedInScope(
            ['users', 'sessions', 'orders'],
            [],
            ['users', 'sessions', 'orders'],
        );

        $this->assertEquals([], $result);
    }

    public function test_returns_reindexed_array(): void
    {
        $result = $this->command->callGetExcludedInScope(
            ['a', 'b', 'c', 'd'],
            ['b', 'c'],
            ['a', 'c', 'd'], // c — excluded, but in syncTableNames
        );

        $this->assertSame([0], array_keys($result));
        $this->assertEquals(['b'], $result);
    }
}
