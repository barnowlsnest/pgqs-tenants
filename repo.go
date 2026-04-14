package pgqstenants

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
	ErrSchemaNotFound = errors.New("tenant schema not found")
	ErrNilTenant      = errors.New("nil tenant")
)

type TenantRepo struct {
	pool *postgres.DBPool
}

func NewTenantRepo(pool *postgres.DBPool) *TenantRepo {
	return &TenantRepo{pool}
}

func (tr *TenantRepo) notifyTenantStatusWithTX(
	ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, status string,
) error {
	if tx == nil {
		return fmt.Errorf("nil transaction")
	}

	notifySQL, err := NotifyTenantStatusSQL(tenantID, status)
	if err != nil {
		return err
	}

	log.Debug("TenantRepo.notifyTenantStatusWithTX", log.F("sql", notifySQL))
	if _, err := tx.Exec(ctx, notifySQL); err != nil {
		return err
	}

	return nil
}

func (tr *TenantRepo) Create(ctx context.Context, tenant *Tenant) (*Tenant, error) {
	if tenant == nil {
		return nil, ErrNilTenant
	}

	tenant.State = "created"
	tenantsTableName := PGQSTenantsTable()
	tenantSQL, args, errSQL := postgres.SQL().
		Insert(tenantsTableName).
		Rows(tenant).
		OnConflict(
			goqu.DoUpdate(
				"name",
				goqu.Record{
					"status":     "created",
					"state":      "created",
					"updated_at": time.Now().UTC(),
				},
			).Where(goqu.T(TenantsTable).Col("status").Eq("disabled")),
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
	if errNotify := tr.notifyTenantStatusWithTX(ctx, tx, nTenant.ID, nTenant.Status); errNotify != nil {
		return nil, errNotify
	}

	if errTx := tx.Commit(ctx); errTx != nil {
		return nil, errTx
	}

	return &nTenant, nil
}

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

	tenantsTableName := PGQSTenantsTable()
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

func (tr *TenantRepo) Get(ctx context.Context, id uuid.UUID) (*Tenant, error) {
	tenantsTableName := PGQSTenantsTable()
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

func (tr *TenantRepo) GetAll(ctx context.Context) ([]Tenant, error) {
	tenantsTableName := PGQSTenantsTable()
	sql, _, errSQL := postgres.SQL().
		From(tenantsTableName).
		Order(goqu.L("(status = 'disabled')").Desc()).
		Prepared(true).
		ToSQL()

	if errSQL != nil {
		return nil, errSQL
	}

	log.Debug("TenantRepo.GetAll", log.F("sql", sql))

	var tenants []Tenant
	if errQuery := pgxscan.Select(ctx, tr.pool, &tenants, sql); errQuery != nil {
		return nil, errQuery
	}

	return tenants, nil
}

func (tr *TenantRepo) SoftDelete(ctx context.Context, id uuid.UUID) error {
	tenantsTableName := PGQSTenantsTable()
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
	if errNotify := tr.notifyTenantStatusWithTX(ctx, tx, id, Disabled); errNotify != nil {
		return errNotify
	}

	if errCommit := tx.Commit(ctx); errCommit != nil {
		return errCommit
	}

	return nil
}

func (tr *TenantRepo) createTenantSchemaWithTX(ctx context.Context, tenantID uuid.UUID, tx pgx.Tx) error {
	schemaName := PGQSTenantSchema(tenantID)
	schemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q AUTHORIZATION CURRENT_USER;", schemaName)

	log.Debug("TenantRepo.createTenantSchemaWithTX", log.F("sql", schemaSQL))

	if _, errExec := tx.Exec(ctx, schemaSQL); errExec != nil {
		return errExec
	}

	return nil
}

func (tr *TenantRepo) GetTenantSchemaState(ctx context.Context, tenantID uuid.UUID) (string, error) {
	schemaName := PGQSTenantSchema(tenantID)
	schemaExistsSQL := `
        SELECT
            EXISTS(
                SELECT 1 FROM information_schema.schemata
                WHERE schema_name = $1
                LIMIT 1
            ) AS schema_exists,
            EXISTS(
                SELECT 1 FROM pgqs.tenants
                WHERE id = $2 AND status = 'disabled'
            ) AS is_disabled,
            (
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = $1 AND table_name != 'schema_migrations'
            ) AS schema_tables_count
    `

	log.Debug("TenantRepo.GetTenantSchemaState", log.F("sql", schemaExistsSQL))

	var state TenantSchemaState
	if err := pgxscan.Get(ctx, tr.pool, &state, schemaExistsSQL, schemaName, tenantID); err != nil {
		return "", err
	}

	switch {
	case !state.SchemaExists:
		return "", ErrSchemaNotFound
	case state.SchemaTablesCount == 0:
		return Down, nil
	case state.IsDisabled:
		return Disabled, nil
	}

	return Up, nil
}

func (tr *TenantRepo) DeleteTenantSchema(ctx context.Context, tenantID uuid.UUID) error {
	tx, errTx := tr.pool.Begin(ctx)
	if errTx != nil {
		return errTx
	}

	defer func() { _ = tx.Rollback(ctx) }()

	tenantsTableName := PGQSTenantsTable()
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

	schemaName := PGQSTenantSchema(tenantID)
	schemaSQL := fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE;", schemaName)

	log.Debug("TenantRepo.DeleteTenantSchema.TX", log.F("sql", schemaSQL))

	if _, errExec := tx.Exec(ctx, schemaSQL); errExec != nil {
		return errExec
	}
	if errNotify := tr.notifyTenantStatusWithTX(ctx, tx, tenantID, Purged); errNotify != nil {
		return errNotify
	}

	if errCommit := tx.Commit(ctx); errCommit != nil {
		return errCommit
	}

	return nil
}
