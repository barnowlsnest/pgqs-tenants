# pgqs-tenants

A Go library for managing PostgreSQL-based multi-tenant infrastructure within the [pgqs](https://github.com/barnowlsnest) ecosystem. It provides tenant lifecycle management — create, update, soft-delete, and purge — backed by schema-per-tenant isolation and `LISTEN/NOTIFY` change events.

## Overview

Each tenant gets its own PostgreSQL schema (`pgqs_tenant_<uuid>`), created automatically on tenant creation. The `pgqs` controller schema holds the shared `tenants` table. Triggers fire `pg_notify` on the `tenants` channel whenever a tenant is inserted, changes status, or is deleted, enabling event-driven consumers to react to lifecycle changes.

## Packages

| Package       | Description                                                   |
|---------------|---------------------------------------------------------------|
| `pkg/tenants` | `TenantRepo` — CRUD operations and schema management          |
| `pkg/pgqsdb`  | `RollOut` / `RollDown` — applies embedded database migrations |

## Installation

```bash
go get github.com/barnowlsnest/pgqs-tenants/v3
```

Requires Go 1.26+ and PostgreSQL 14+.

## Usage

### Apply migrations

Run this once at application startup before creating any tenants. It creates the `pgqs` schema and the `tenants` table.

```go
import "github.com/barnowlsnest/pgqs-tenants/v3/pkg/pgqsdb"

if err := pgqsdb.RollOut(ctx, dbURL); err != nil {
    log.Fatal(err)
}
```

### Create a tenant

```go
import (
    "github.com/barnowlsnest/pgqs-tenants/v3/pkg/tenants"
    harnesspg "github.com/barnowlsnest/pgqs-harness/postgres"
)

pool, _ := harnesspg.NewPool(ctx, dbURL)
repo := tenants.NewRepo(pool)

tenant, err := repo.Create(ctx, &tenants.Tenant{
    Name:     "acme",
    Metadata: []byte(`{"engine": "standard"}`),
})
// tenant.SchemaName == "pgqs_tenant_<uuid>"
```

Creating a tenant with a name that already exists re-activates the tenant only if its current status is `disabled`. Otherwise an error is returned.

### Retrieve tenants

```go
// by ID
t, err := repo.Get(ctx, tenantID)

// all tenants (active tenants first, disabled last)
all, err := repo.GetAll(ctx)
```

### Update a tenant

```go
updated, err := repo.Update(ctx, tenantID, &tenants.UpdateTenantParams{
    Status:   "ready",
    Metadata: []byte(`{"engine": "premium"}`),
})
```

Valid status values: `created`, `ready`, `disabled`.

### Soft-delete a tenant

Sets the tenant's status to `disabled`. The record and schema are preserved.

```go
err := repo.SoftDelete(ctx, tenantID)
```

### Permanently delete a tenant

Drops the tenant's PostgreSQL schema and removes the record from the database.

```go
err := repo.DeleteTenantSchema(ctx, tenantID)
```

### Check schema state

```go
info, err := repo.GetSchemaInfo(ctx, tenantID)
// info.Exists   — schema exists in PostgreSQL
// info.Migrated — schema contains at least one non-migration table
```

### LISTEN/NOTIFY events

The database emits a notification on the `tenants` channel for every lifecycle event. The payload is a JSON object:

```json
{"id": "<uuid>", "schema": "pgqs_tenant_<uuid>", "event": "<status|purged>"}
```

`event` is the new status value (`created`, `ready`, `disabled`) or `purged` when the record is deleted.

## Tenant lifecycle

```
       Create
         │
         ▼
      created ──── Update ───► ready
         │                      │
         └──── SoftDelete ──────┘
                    │
                    ▼
                disabled
                    │
           Create (same name)
                    │
                    ▼
                 created   ← reactivated; schema preserved
                    │
          DeleteTenantSchema
                    │
                    ▼
                (purged)
```

## Schema naming

Tenant schemas follow the pattern `pgqs_tenant_<uuid>` and are generated as a stored computed column — the name is immutable and consistent.

```go
schemaName := tenants.TenantSchema(tenantID) // "pgqs_tenant_<uuid>"
```

## Testing

Tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) and require Docker.

```bash
task test
# or
go test -v -race -timeout 5m ./...
```

## License

See [LICENSE](LICENSE).
