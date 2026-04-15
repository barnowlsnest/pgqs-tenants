package pgqs_tenants

import (
	"embed"
	"net/url"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrateUP migrates the tenant schema up.
func MigrateUP(dbURL string, tenantID uuid.UUID) error {
	m, err := newTenantMigration(dbURL, tenantID)
	if err != nil {
		return err
	}

	defer func() { _, _ = m.Close() }()

	return m.Up()
}

// MigrateDOWN migrates the tenant schema down.
func MigrateDOWN(dbURL string, tenantID uuid.UUID) error {
	m, err := newTenantMigration(dbURL, tenantID)
	if err != nil {
		return err
	}

	defer func() { _, _ = m.Close() }()

	return m.Down()
}

func newTenantMigration(dbURL string, tenantID uuid.UUID) (*migrate.Migrate, error) {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("search_path", TenantSchema(tenantID))
	u.RawQuery = q.Encode()

	return migrate.NewWithSourceInstance("iofs", src, u.String())
}
