# pgqs-tenants

[![Build](https://github.com/barnowlsnest/pgqs-tenants/actions/workflows/build.yml/badge.svg)](https://github.com/barnowlsnest/pgqs-tenants/actions/workflows/build.yml)
[![Lint](https://github.com/barnowlsnest/pgqs-tenants/actions/workflows/lint.yml/badge.svg)](https://github.com/barnowlsnest/pgqs-tenants/actions/workflows/lint.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/barnowlsnest/pgqs-tenants/v3.svg)](https://pkg.go.dev/github.com/barnowlsnest/pgqs-tenants/v3)
[![Go Report Card](https://goreportcard.com/badge/github.com/barnowlsnest/pgqs-tenants/v3)](https://goreportcard.com/report/github.com/barnowlsnest/pgqs-tenants/v3)
[![Release](https://img.shields.io/github/v/release/barnowlsnest/pgqs-tenants)](https://github.com/barnowlsnest/pgqs-tenants/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Go library for schema-per-tenant PostgreSQL multi-tenancy, built for the [pgqs](https://github.com/barnowlsnest) ecosystem but usable on its own. It provides tenant lifecycle management — create, update, soft-delete and purge — backed by schema isolation and `LISTEN/NOTIFY` change events.

## Features

- **Schema-per-tenant isolation** — every tenant gets its own `pgqs_tenant_<uuid>` schema, created in the same transaction as its row.
- **Immutable schema names** — derived in the database as a stored generated column, never written from Go.
- **Event-driven** — triggers `pg_notify` the `tenants` channel on insert, status change and delete, so consumers can react to lifecycle changes.
- **Embedded migrations** — the control-plane schema ships with the library via `go:embed` and golang-migrate.
- **Soft delete and reactivation** — disabled tenants keep their data and can be revived by name.

## Requirements

- Go 1.27+
- PostgreSQL 14+
- Docker (only to run the test suite, which uses testcontainers)

## Installation

```bash
go get github.com/barnowlsnest/pgqs-tenants/v3
```

## Packages

| Package        | Description                                                   |
|----------------|---------------------------------------------------------------|
| `pkg/pgtenant` | `TenantRepo` — CRUD operations and schema management          |
| `pkg/database` | `RollOut` / `RollDown` — applies embedded database migrations |

## Overview

Each tenant gets its own PostgreSQL schema (`pgqs_tenant_<uuid>`), created automatically on tenant creation. The `pgqs` controller schema holds the shared `tenants` table. Triggers fire `pg_notify` on the `tenants` channel whenever a tenant is inserted, changes status, or is deleted, enabling event-driven consumers to react to lifecycle changes.

## Quick start

```go
package main

import (
	"context"
	"log"

	harnesspg "github.com/barnowlsnest/pgqs-harness/v2/postgres"

	"github.com/barnowlsnest/pgqs-tenants/v3/pkg/database"
	"github.com/barnowlsnest/pgqs-tenants/v3/pkg/pgtenant"
)

func main() {
	ctx := context.Background()
	dbURL := "postgres://postgres:postgres@localhost:5432/pgqs?sslmode=disable"

	// Create the pgqs control-plane schema. Run once at startup.
	if err := database.RollOut(ctx, dbURL); err != nil {
		log.Fatal(err)
	}

	pool, err := harnesspg.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repo := pgtenant.NewRepo(pool)

	tenant, err := repo.Create(ctx, &pgtenant.Tenant{
		Name:     "acme",
		Metadata: []byte(`{"engine": "standard"}`),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println(tenant.ID, tenant.SchemaName) // <uuid> pgqs_tenant_<uuid>
}
```

## Usage

### Apply migrations

Run this once at application startup before creating any tenants. It creates the `pgqs` schema and the `tenants` table.

```go
if err := database.RollOut(ctx, dbURL); err != nil {
    log.Fatal(err)
}
```

`database.RollDown(ctx, dbURL)` reverses them.

### Create a tenant

```go
tenant, err := repo.Create(ctx, &pgtenant.Tenant{
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
updated, err := repo.Update(ctx, tenantID, &pgtenant.UpdateTenantParams{
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

### Per-tenant migrations

This library only creates the empty `pgqs_tenant_<uuid>` schema — it never runs tenant-scoped migrations. Applying those inside the schema is the caller's responsibility; `GetSchemaInfo` reports whether it has been done.

### LISTEN/NOTIFY events

The database emits a notification on the `tenants` channel for every lifecycle event. The payload is a JSON object:

```json
{"id": "<uuid>", "schema": "pgqs_tenant_<uuid>", "event": "<status|purged>"}
```

`event` is the new status value (`created`, `ready`, `disabled`) or `purged` when the record is deleted.

A listener can be built straight from the pool with the harness helper:

```go
listener, err := harnesspg.NewListenerFromPool(ctx, pool, "tenants", 16)
```

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
schemaName := pgtenant.TenantSchema(tenantID) // "pgqs_tenant_<uuid>"
```

## Development

Tasks are defined in `Taskfile.yml` and need [Task](https://taskfile.dev); plain `go` commands work just as well.

```bash
task build   # go build ./...
task lint    # go vet, go fmt, golangci-lint run --fix
task test    # go test -v -race -timeout 5m ./...
task update  # go mod tidy
```

Tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) and require a running Docker daemon — the suite starts a PostgreSQL container once per run.

```bash
go test -v -race -timeout 5m ./...
```

## Contributing

Issues and pull requests are welcome.

1. Fork the repository and branch off `main`.
2. Make your change, keeping it covered by tests.
3. Run `task lint` and `task test` before opening the PR — CI runs the same checks.
4. Describe the motivation for the change in the PR description.

## License

Released under the [MIT License](LICENSE).
