package pgqsdb

import (
	"context"
	"embed"

	"github.com/barnowlsnest/pgqs-harness/mgr"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// RollOut applies pgqs database migrations.
func RollOut(ctx context.Context, dbURL string) error {
	driver, err := iofs.New(embeddedMigrations, "migrations")
	if err != nil {
		return err
	}

	return mgr.Up(ctx, &mgr.Config{
		DBURL:       dbURL,
		EmbeddedSRC: driver,
	})
}

// RollDown rolls down pgqs database migrations.
func RollDown(ctx context.Context, dbURL string) error {
	driver, err := iofs.New(embeddedMigrations, "migrations")
	if err != nil {
		return err
	}

	return mgr.Down(ctx, &mgr.Config{
		DBURL:       dbURL,
		EmbeddedSRC: driver,
	})
}
