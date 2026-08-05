# Project Rules

> Short, actionable rules and conventions for this project. Loaded automatically by /aif-implement.

## Rules

- Never write PostgreSQL catalog queries, DDL strings, or `pg_dump`/`psql` process calls outside `src/Adapters/`; services and commands depend on `DatabaseAdapterInterface` only.
- Always write code, comments, PHPDoc, documentation, and commit messages in English — this is a public Composer package.
- Always release `artemyurov/laravel-autossh-tunnel` before this package when both change in the same cycle.
