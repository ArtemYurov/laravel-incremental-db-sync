<?php

declare(strict_types=1);

namespace ArtemYurov\DbSync\Tests\Unit;

use ArtemYurov\DbSync\Adapters\PgsqlAdapter;
use ArtemYurov\DbSync\Tests\TestCase;

/**
 * Tests constraint-aware DDL building. The methods are pure string builders — no DB needed.
 */
class IndexDdlSqlTest extends TestCase
{
    private PgsqlAdapter $adapter;

    protected function setUp(): void
    {
        parent::setUp();
        $this->adapter = new PgsqlAdapter();
    }

    public function test_drop_plain_index_uses_drop_index(): void
    {
        $sql = $this->adapter->dropIndexOrConstraintSql('t', 't_name_idx', 'index');

        $this->assertSame('DROP INDEX IF EXISTS "t_name_idx"', $sql);
    }

    public function test_drop_primary_key_uses_alter_table_drop_constraint(): void
    {
        // CRITICAL: a PK cannot be dropped via DROP INDEX — only via ALTER TABLE.
        $sql = $this->adapter->dropIndexOrConstraintSql('registration_request_logs', 'registration_request_logs_pkey', 'p');

        $this->assertSame(
            'ALTER TABLE "registration_request_logs" DROP CONSTRAINT IF EXISTS "registration_request_logs_pkey"',
            $sql,
        );
    }

    public function test_drop_unique_and_exclusion_use_alter_table(): void
    {
        $this->assertSame(
            'ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_email_unique"',
            $this->adapter->dropIndexOrConstraintSql('t', 't_email_unique', 'u'),
        );
        $this->assertSame(
            'ALTER TABLE "t" DROP CONSTRAINT IF EXISTS "t_excl"',
            $this->adapter->dropIndexOrConstraintSql('t', 't_excl', 'x'),
        );
    }

    public function test_create_plain_index_returns_definition_as_is(): void
    {
        $def = 'CREATE INDEX t_name_idx ON public.t USING btree (name)';
        $sql = $this->adapter->createIndexOrConstraintSql('t', 't_name_idx', 'index', $def);

        $this->assertSame($def, $sql);
    }

    public function test_create_primary_key_wraps_definition_in_alter_table(): void
    {
        $sql = $this->adapter->createIndexOrConstraintSql(
            'verification_request_logs',
            'verification_request_logs_pkey',
            'p',
            'PRIMARY KEY (id)',
        );

        $this->assertSame(
            'ALTER TABLE "verification_request_logs" ADD CONSTRAINT "verification_request_logs_pkey" PRIMARY KEY (id)',
            $sql,
        );
    }

    /**
     * Regression: DDL routing must key off the constraint contypes (p/u/x), not `=== 'index'`.
     * A mislabeled plain index (e.g. the truncated 'i' that Postgres produced before the
     * ::text fix in getIndexMap) must still be treated as a plain index, never wrapped in
     * ALTER TABLE ADD CONSTRAINT — otherwise it builds `ADD CONSTRAINT ... CREATE UNIQUE INDEX ...`
     * and dies with SQLSTATE 42601.
     */
    public function test_create_treats_non_constraint_type_as_plain_index(): void
    {
        $def = 'CREATE UNIQUE INDEX t_slug_idx ON public.t USING btree (brand_id, slug) WHERE (deleted_at IS NULL)';

        // 'i' — the historic truncated value; must return the CREATE INDEX def verbatim.
        $this->assertSame($def, $this->adapter->createIndexOrConstraintSql('t', 't_slug_idx', 'i', $def));
        // any other unexpected type is also treated as a plain index, not a constraint.
        $this->assertSame($def, $this->adapter->createIndexOrConstraintSql('t', 't_slug_idx', '', $def));
    }

    public function test_drop_treats_non_constraint_type_as_plain_index(): void
    {
        $this->assertSame(
            'DROP INDEX IF EXISTS "t_slug_idx"',
            $this->adapter->dropIndexOrConstraintSql('t', 't_slug_idx', 'i'),
        );
        $this->assertSame(
            'DROP INDEX IF EXISTS "t_slug_idx"',
            $this->adapter->dropIndexOrConstraintSql('t', 't_slug_idx', ''),
        );
    }
}
