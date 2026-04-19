package pgqstenant

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/barnowlsnest/pgqs-harness/postgres"
	"github.com/doug-martin/goqu/v9"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
)

var (
	ErrNilTenant = errors.New("nil tenant")
)

type TenantRepo struct {
	pool *postgres.DBPool
}

func NewRepo(pool *postgres.DBPool) *TenantRepo {
	return &TenantRepo{pool}
}

// Create inserts a new tenant record into the database and initializes its schema.
// Returns the created tenant or an error.
func (tr *TenantRepo) Create(ctx context.Context, tenant *Tenant) (*Tenant, error) {
	if tenant == nil {
		return nil, ErrNilTenant
	}

	tenantsTableName := TenantsTable()
	tenantSQL, args, errSQL := postgres.SQL().
		Insert(tenantsTableName).
		Rows(tenant).
		OnConflict(
			goqu.DoUpdate(
				"name",
				goqu.Record{
					"status":     "created",
					"updated_at": time.Now().UTC(),
				},
			).Where(goqu.T(TenantsTableName).Col("status").Eq("disabled")),
		).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return nil, errSQL
	}

	tx, errTx := tr.pool.Begin(ctx)
	if errTx != nil {
		return nil, errTx
	}

	defer func() { _ = tx.Rollback(ctx) }()

	var nTenant Tenant
	if errInsert := pgxscan.Get(ctx, tx, &nTenant, tenantSQL, args...); errInsert != nil {
		return nil, errInsert
	}

	if errSchema := tr.createTenantSchemaWithTX(ctx, nTenant.ID, tx); errSchema != nil {
		return nil, errSchema
	}

	if errTx := tx.Commit(ctx); errTx != nil {
		return nil, errTx
	}

	return &nTenant, nil
}

// Update modifies a tenant record in the database using the provided parameters and returns the updated tenant or an error.
func (tr *TenantRepo) Update(
	ctx context.Context, id uuid.UUID, params *UpdateTenantParams,
) (*Tenant, error) {
	record := goqu.Record{"updated_at": time.Now().UTC()}
	if status := params.Status; status != "" {
		record["status"] = status
	}
	if metadata := params.Metadata; metadata != nil {
		record["metadata"] = metadata
	}

	tenantsTableName := TenantsTable()
	sql, args, errSQL := postgres.SQL().Update(tenantsTableName).
		Set(record).
		Where(goqu.C("id").Eq(id.String())).
		Returning(goqu.Star()).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return nil, errSQL
	}

	log.Debug("TenantRepo.Update", log.F("sql", sql))

	var nTenant Tenant
	if errInsert := pgxscan.Get(ctx, tr.pool, &nTenant, sql, args...); errInsert != nil {
		return nil, errInsert
	}
	return &nTenant, nil
}

// Get retrieves a tenant record by its ID from the database.
// Returns the tenant or an error if the operation fails.
func (tr *TenantRepo) Get(ctx context.Context, id uuid.UUID) (*Tenant, error) {
	tenantsTableName := TenantsTable()
	sql, args, errSQL := postgres.SQL().
		From(tenantsTableName).
		Where(goqu.C("id").Eq(id.String())).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return nil, errSQL
	}

	log.Debug("TenantRepo.Get", log.F("sql", sql))

	var nTenant Tenant
	if errInsert := pgxscan.Get(ctx, tr.pool, &nTenant, sql, args...); errInsert != nil {
		return nil, errInsert
	}

	return &nTenant, nil
}

// GetAll retrieves all tenant records from the database, ordered by status with disabled tenants listed last.
func (tr *TenantRepo) GetAll(ctx context.Context) ([]*Tenant, error) {
	tenantsTableName := TenantsTable()
	sql, _, errSQL := postgres.SQL().
		From(tenantsTableName).
		Order(goqu.L("(status = 'disabled')").Desc()).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return nil, errSQL
	}

	log.Debug("TenantRepo.GetAll", log.F("sql", sql))

	var tenants []*Tenant
	if errQuery := pgxscan.Select(ctx, tr.pool, &tenants, sql); errQuery != nil {
		return nil, errQuery
	}

	return tenants, nil
}

// SoftDelete updates the tenant's status to "disabled" and updates the "updated_at" field for the given tenant ID.
func (tr *TenantRepo) SoftDelete(ctx context.Context, id uuid.UUID) error {
	tenantsTableName := TenantsTable()
	sql, args, errSQL := postgres.SQL().
		Update(tenantsTableName).
		Set(
			goqu.Record{
				"status":     "disabled",
				"updated_at": time.Now().UTC(),
			},
		).
		Where(
			goqu.And(
				goqu.C("id").Eq(id.String()),
				goqu.C("status").Neq("disabled"),
			),
		).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return errSQL
	}

	log.Debug("TenantRepo.SoftDelete", log.F("sql", sql))

	tx, errTx := tr.pool.Begin(ctx)
	if errTx != nil {
		return errTx
	}

	defer func() { _ = tx.Rollback(ctx) }()

	update, errDel := tx.Exec(ctx, sql, args...)
	if errDel != nil {
		return errDel
	}

	if update.RowsAffected() == 0 {
		return fmt.Errorf("not a single row updated")
	}

	if errCommit := tx.Commit(ctx); errCommit != nil {
		return errCommit
	}

	return nil
}

// GetSchemaInfo checks whether a tenant's schema exists and has been migrated.
func (tr *TenantRepo) GetSchemaInfo(ctx context.Context, tenantID uuid.UUID) (*SchemaInfo, error) {
	schemaName := TenantSchema(tenantID)

	tx, err := tr.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	var info SchemaInfo

	errExists := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schemaName,
	).Scan(&info.Exists)
	if errExists != nil {
		return nil, errExists
	}

	if !info.Exists {
		return &info, tx.Commit(ctx)
	}

	var tableCount int
	errCount := tx.QueryRow(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name != 'schema_migrations'`,
		schemaName,
	).Scan(&tableCount)
	if errCount != nil {
		return nil, errCount
	}

	info.Migrated = tableCount > 0

	return &info, tx.Commit(ctx)
}

// DeleteTenantSchema removes the database schema and tenant record associated with the given tenant ID.
func (tr *TenantRepo) DeleteTenantSchema(ctx context.Context, tenantID uuid.UUID) error {
	tx, errTx := tr.pool.Begin(ctx)
	if errTx != nil {
		return errTx
	}

	defer func() { _ = tx.Rollback(ctx) }()

	tenantsTableName := TenantsTable()
	sql, args, errSQL := postgres.SQL().Delete(tenantsTableName).
		Where(goqu.C("id").Eq(tenantID.String())).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return errSQL
	}

	log.Debug("TenantRepo.DeleteTenantSchema.TX", log.F("sql", sql))

	del, errDel := tx.Exec(ctx, sql, args...)
	if errDel != nil {
		return errDel
	}

	if del.RowsAffected() == 0 {
		return fmt.Errorf("tenant not found: %s", tenantID.String())
	}

	schemaName := TenantSchema(tenantID)
	schemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE;", schemaName)

	log.Debug("TenantRepo.DeleteTenantSchema.TX", log.F("sql", schemaSQL))

	if _, errExec := tx.Exec(ctx, schemaSQL); errExec != nil {
		return errExec
	}

	if errCommit := tx.Commit(ctx); errCommit != nil {
		return errCommit
	}

	return nil
}

func (tr *TenantRepo) createTenantSchemaWithTX(ctx context.Context, tenantID uuid.UUID, tx pgx.Tx) error {
	schemaName := TenantSchema(tenantID)
	schemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q AUTHORIZATION CURRENT_USER;", schemaName)

	log.Debug("TenantRepo.createTenantSchemaWithTX", log.F("sql", schemaSQL))

	if _, errExec := tx.Exec(ctx, schemaSQL); errExec != nil {
		return errExec
	}

	return nil
}
