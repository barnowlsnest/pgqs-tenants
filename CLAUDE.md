# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Tasks are defined in `Taskfile.yml` (requires [Task](https://taskfile.dev); `go test` also works directly).

- `task build` — `go build ./...`
- `task test` — clears the test cache, then `go test -v -race -timeout 5m ./...`
- `task lint` — `go vet`, `go fmt`, `golangci-lint run --fix`
- `task update` — `go mod tidy`

Run a single test (the whole suite spins up one Postgres container in `SetupSuite`):

```bash
go test -v -race ./pkg/pgtenant -run TestTenantRepoTestSuite/TestCreate_Success
```

Tests use testcontainers-go and **require a running Docker daemon**. Requires Go 1.27+ and PostgreSQL 14+.

CI (`.github/workflows/`): `build.yml` runs `task build` + `task test`; `lint.yml` runs golangci-lint v2.13. Both take the Go version from `go.mod`.

## Architecture

This is a library (no `main`) for schema-per-tenant Postgres multi-tenancy in the pgqs ecosystem. Two packages:

### `pkg/database`
Thin wrapper over `pgqs-harness/v2/db`. Embeds `migrations/*.sql` (golang-migrate format, `NNNNNN_name.up/down.sql`) via `go:embed` and exposes `RollOut` / `RollDown`. Run `RollOut` once at startup before any tenant operations.

Migrations build the control plane in the `pgqs` schema:
1. `pgqs` schema
2. `pgqs.tenants` table — `status` CHECK constrained to `created` / `ready` / `disabled`; `name` unique
3. `schema_name` — a `GENERATED ALWAYS AS ('pgqs_tenant_' || id::text) STORED` column (immutable, never set from Go), plus `idx_tenants_schema_name`
4. `LISTEN/NOTIFY` triggers: AFTER INSERT, AFTER UPDATE (only when status changed), AFTER DELETE all `pg_notify` the `tenants` channel with `{"id","schema","event"}` where `event` is the new status or `"purged"` on delete

### `pkg/pgtenant`
`TenantRepo` (constructed with `NewRepo(*postgres.DBPool)` from `pgqs-harness/v2/postgres`; `DBPool` is a type alias for `pgxpool.Pool`) — all tenant lifecycle operations. SQL is built with goqu (`postgres.SQL()`), rows scanned with scany/pgxscan.

Key behaviors:
- **`Create`** runs in a transaction: upsert the `tenants` row, then `CREATE SCHEMA pgqs_tenant_<uuid>`. `ON CONFLICT (name)` only re-activates (sets status back to `created`) when the existing row is `disabled` — otherwise the conflict does nothing and the insert returns no row / an error. Schema creation and the row insert commit together.
- **`SoftDelete`** sets status `disabled` (guarded by `status != 'disabled'`); schema and row are kept.
- **`DeleteTenantSchema`** ("purge") — transactionally deletes the row then `DROP SCHEMA ... CASCADE`. The DELETE trigger fires the `purged` notification.
- **`GetAll`** orders active tenants before disabled ones.
- **`GetSchemaInfo`** — `Exists` from `information_schema.schemata`; `Migrated` is true when the schema has any table other than `schema_migrations`.

The `pgtenant` package does not run per-tenant migrations — it only creates the empty schema. Applying tenant-scoped migrations into that schema is the caller's responsibility.

## Conventions

- Module path is `.../pgqs-tenants/v3` (v3 in the import path); goimports local-prefix is set to it.
- goqu struct tags on `Tenant` control insert/update behavior: `defaultifempty` (id), `omitempty`, `skipinsert` (schema_name).
- Logging via `go-logslib/v2` `sharedlog` as `log`, using `log.Debug(msg, log.F(k, v))`; SQL is logged at debug.
- golangci-lint runs a large linter set (funlen 100 lines, gocyclo 15, lll 140); `_test.go` files are exempted from several.