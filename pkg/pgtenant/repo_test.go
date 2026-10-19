package pgtenant

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/barnowlsnest/pgqs-tenants/v3/pkg/database"

	harnesspg "github.com/barnowlsnest/pgqs-harness/v2/postgres"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
)

const (
	testDBName = "pgqs_test"
	testDBUser = "postgres"
	testDBPass = "postgres"
)

type TenantRepoTestSuite struct {
	suite.Suite
	pool    *pgxpool.Pool
	cleanup func()
	repo    *TenantRepo
	ctx     context.Context
}

func (s *TenantRepoTestSuite) SetupSuite() {
	s.pool, s.cleanup = SetupTestContainer(s.T())

	err := database.RollOut(s.T().Context(), s.pool.Config().ConnString())
	s.NoError(err)
	s.repo = NewRepo(s.pool)
	s.ctx = context.Background()
}

func (s *TenantRepoTestSuite) TearDownSuite() {
	s.cleanup()
}

func (s *TenantRepoTestSuite) TearDownTest() {
	// Clean up tenants table and any tenant schemas after each test
	_, _ = s.pool.Exec(s.ctx, "DELETE FROM pgqs.tenants")
}

func TestTenantRepoTestSuite(t *testing.T) {
	suite.Run(t, new(TenantRepoTestSuite))
}

func (s *TenantRepoTestSuite) TestCreate_Success() {
	tenant := &Tenant{
		Name:     "test-tenant",
		Metadata: []byte(`{"key": "value"}`),
	}

	result, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)
	s.NotEqual(uuid.Nil, result.ID)
	s.Equal("test-tenant", result.Name)
	s.Equal("created", result.Status)

	// Verify schema was created
	var schemaExists bool
	schemaName := TenantSchema(result.ID)
	err = s.pool.QueryRow(s.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
		schemaName,
	).Scan(&schemaExists)
	s.Require().NoError(err)
	s.True(schemaExists, "tenant schema should exist")
}

// Payload shape is defined in migrations/000004_tenant_notify_triggers.up.sql (notify_tenant_status_change).
func (s *TenantRepoTestSuite) TestCreate_notifyTenantsChannel_singlePayloadFromTrigger() {
	listener, err := harnesspg.NewListenerFromPool(s.ctx, s.pool, "tenants", 2)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)
	s.Require().NoError(listener.Start(s.ctx))

	name := "notify-" + uuid.New().String()
	created, err := s.repo.Create(s.ctx, &Tenant{Name: name, Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	var payload struct {
		ID     uuid.UUID `json:"id"`
		Schema string    `json:"schema"`
		Event  string    `json:"event"`
	}
	select {
	case n := <-listener.Notifications():
		s.Require().NotNil(n)
		s.Equal("tenants", n.Channel)
		s.Require().NoError(json.Unmarshal([]byte(n.Payload), &payload))
	case <-time.After(15 * time.Second):
		s.FailNow("timed out waiting for NOTIFY")
	}

	s.Equal(created.ID, payload.ID)
	s.Equal(TenantSchema(created.ID), payload.Schema)
	s.Equal("created", payload.Event)

	select {
	case n := <-listener.Notifications():
		s.Fail("unexpected second NOTIFY: " + n.Payload)
	case <-time.After(500 * time.Millisecond):
	}
	s.NoError(listener.Err())
}

func (s *TenantRepoTestSuite) TestCreate_NilTenant() {
	result, err := s.repo.Create(s.ctx, nil)
	s.Nil(result)
	s.ErrorIs(err, ErrNilTenant)
}

func (s *TenantRepoTestSuite) TestCreate_DuplicateName() {
	tenant1 := &Tenant{
		Name:     "duplicate-tenant",
		Metadata: []byte(`{}`),
	}
	_, err := s.repo.Create(s.ctx, tenant1)
	s.Require().NoError(err)

	// Try to create another tenant with the same name
	tenant2 := &Tenant{
		Name:     "duplicate-tenant",
		Metadata: []byte(`{}`),
	}
	_, err = s.repo.Create(s.ctx, tenant2)
	// ON CONFLICT DO UPDATE runs only when the existing row has status = 'disabled'.
	s.Error(err)
}

func (s *TenantRepoTestSuite) TestCreate_ReactivateSoftDeleted() {
	// Create a tenant
	tenant := &Tenant{
		Name:     "reactivate-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET status = 'disabled' WHERE id = $1",
		created.ID,
	)
	s.Require().NoError(err)

	// Create another tenant with the same name - should reactivate
	tenant2 := &Tenant{
		Name:     "reactivate-tenant",
		Metadata: []byte(`{}`),
	}
	reactivated, err := s.repo.Create(s.ctx, tenant2)
	s.Require().NoError(err)
	s.Equal(created.ID, reactivated.ID)
	s.Equal("created", reactivated.Status)
}

func (s *TenantRepoTestSuite) TestCreate_SchemaNameGenerated() {
	tenant := &Tenant{
		Name:     "schema-name-tenant",
		Metadata: []byte(`{}`),
	}

	result, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)
	s.NotEmpty(result.SchemaName)
	s.Equal(TenantSchema(result.ID), result.SchemaName)
}

func (s *TenantRepoTestSuite) TestGet_SchemaNamePresent() {
	tenant := &Tenant{
		Name:     "get-schema-name-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	result, err := s.repo.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(created.SchemaName, result.SchemaName)
	s.Equal(TenantSchema(created.ID), result.SchemaName)
}

func (s *TenantRepoTestSuite) TestGetAll_SchemaNamePresent() {
	for i := range 3 {
		tenant := &Tenant{
			Name:     "getall-schema-" + itoa(i),
			Metadata: []byte(`{}`),
		}
		_, err := s.repo.Create(s.ctx, tenant)
		s.Require().NoError(err)
	}

	results, err := s.repo.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Len(results, 3)
	for i := range results {
		s.NotEmpty(results[i].SchemaName)
		s.Equal(TenantSchema(results[i].ID), results[i].SchemaName)
	}
}

func (s *TenantRepoTestSuite) TestCreate_ReactivatedSchemaNamePreserved() {
	tenant := &Tenant{
		Name:     "reactivate-schema-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)
	originalSchemaName := created.SchemaName

	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET status = 'disabled' WHERE id = $1",
		created.ID,
	)
	s.Require().NoError(err)

	// Reactivate via create with same name
	reactivated, err := s.repo.Create(s.ctx, &Tenant{
		Name:     "reactivate-schema-tenant",
		Metadata: []byte(`{}`),
	})
	s.Require().NoError(err)
	s.Equal(created.ID, reactivated.ID)
	s.Equal(originalSchemaName, reactivated.SchemaName, "schema_name must be preserved on reactivation")
}

func (s *TenantRepoTestSuite) TestGet_Success() {
	// Create a tenant first
	tenant := &Tenant{
		Name:     "get-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Get the tenant by ID
	result, err := s.repo.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(created.ID, result.ID)
	s.Equal("get-tenant", result.Name)
	s.Equal("created", result.Status)
}

func (s *TenantRepoTestSuite) TestGet_NotFound() {
	randomID := uuid.New()
	_, err := s.repo.Get(s.ctx, randomID)
	s.Error(err)
	s.ErrorIs(err, pgx.ErrNoRows)
}

func (s *TenantRepoTestSuite) TestGetAll_Success() {
	// Create multiple tenants
	for i := range 3 {
		tenant := &Tenant{
			Name:     "tenant-" + itoa(i),
			Metadata: []byte(`{}`),
		}
		_, err := s.repo.Create(s.ctx, tenant)
		s.Require().NoError(err)
	}

	result, err := s.repo.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Len(result, 3)
}

func (s *TenantRepoTestSuite) TestGetAll_Empty() {
	result, err := s.repo.GetAll(s.ctx)
	s.Require().NoError(err)
	s.Empty(result)
}

func (s *TenantRepoTestSuite) TestUpdate_StatusOnly() {
	tenant := &Tenant{
		Name:     "status-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	params := &UpdateTenantParams{Status: "ready"}
	updated, err := s.repo.Update(s.ctx, created.ID, params)
	s.Require().NoError(err)
	s.Equal("ready", updated.Status)
}

func (s *TenantRepoTestSuite) TestUpdate_Metadata() {
	// Create a tenant
	tenant := &Tenant{
		Name:     "metadata-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Update metadata
	newMetadata := []byte(`{"environment": "production"}`)
	params := &UpdateTenantParams{
		Metadata: newMetadata,
	}
	updated, err := s.repo.Update(s.ctx, created.ID, params)
	s.Require().NoError(err)
	s.JSONEq(`{"environment": "production"}`, string(updated.Metadata))
}

func (s *TenantRepoTestSuite) TestSoftDelete_Success() {
	// Create a tenant
	tenant := &Tenant{
		Name:     "softdelete-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Soft delete
	err = s.repo.SoftDelete(s.ctx, created.ID)
	s.Require().NoError(err)

	result, err := s.repo.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal("disabled", result.Status)
}

func (s *TenantRepoTestSuite) TestSoftDelete_NotFound() {
	randomID := uuid.New()
	err := s.repo.SoftDelete(s.ctx, randomID)
	s.Error(err)
	s.Contains(err.Error(), "not a single row updated")
}

func (s *TenantRepoTestSuite) TestGetSchemaInfo_ExistsNotMigrated() {
	tenant := &Tenant{
		Name:     "schema-exists-no-tables",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	info, err := s.repo.GetSchemaInfo(s.ctx, created.ID)
	s.Require().NoError(err)
	s.True(info.Exists)
	s.False(info.Migrated)
}

func (s *TenantRepoTestSuite) TestGetSchemaInfo_ExistsAndMigrated() {
	tenant := &Tenant{
		Name:     "schema-exists-with-tables",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	schemaName := TenantSchema(created.ID)
	ensureTenantProbeTable(s.T(), s.ctx, s.pool, schemaName)

	info, err := s.repo.GetSchemaInfo(s.ctx, created.ID)
	s.Require().NoError(err)
	s.True(info.Exists)
	s.True(info.Migrated)
}

func (s *TenantRepoTestSuite) TestGetSchemaInfo_SchemaDoesNotExist() {
	tenant := &Tenant{
		Name:     "schema-dropped",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	schemaName := TenantSchema(created.ID)
	_, err = s.pool.Exec(s.ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	s.Require().NoError(err)

	info, err := s.repo.GetSchemaInfo(s.ctx, created.ID)
	s.Require().NoError(err)
	s.False(info.Exists)
	s.False(info.Migrated)
}

func (s *TenantRepoTestSuite) TestDeleteTenantSchema_Success() {
	// Create a tenant
	tenant := &Tenant{
		Name:     "delete-schema-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Delete the tenant and schema
	err = s.repo.DeleteTenantSchema(s.ctx, created.ID)
	s.Require().NoError(err)

	// Verify tenant record is deleted
	_, err = s.repo.Get(s.ctx, created.ID)
	s.ErrorIs(err, pgx.ErrNoRows)

	// Verify schema is dropped
	var schemaExists bool
	schemaName := TenantSchema(created.ID)
	err = s.pool.QueryRow(s.ctx,
		"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
		schemaName,
	).Scan(&schemaExists)
	s.Require().NoError(err)
	s.False(schemaExists, "tenant schema should not exist")
}

func (s *TenantRepoTestSuite) TestDeleteTenantSchema_NotFound() {
	randomID := uuid.New()
	err := s.repo.DeleteTenantSchema(s.ctx, randomID)
	s.Error(err)
	s.Contains(err.Error(), "tenant not found")
}

func (s *TenantRepoTestSuite) TestRollOut_CreatesTablesInTenantSchema() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-tenant", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	err = s.repo.RollOut(s.ctx, created, s.tenantMigrations())
	s.Require().NoError(err)

	schemaName := TenantSchema(created.ID)
	s.True(tableExists(s.T(), s.ctx, s.pool, schemaName, "orders"))
	s.True(tableExists(s.T(), s.ctx, s.pool, schemaName, "order_items"))
	s.True(tableExists(s.T(), s.ctx, s.pool, schemaName, "schema_migrations"))
	s.False(tableExists(s.T(), s.ctx, s.pool, "public", "orders"), "tenant DDL must not leak into public")

	info, err := s.repo.GetSchemaInfo(s.ctx, created.ID)
	s.Require().NoError(err)
	s.True(info.Migrated)
}

func (s *TenantRepoTestSuite) TestRollOut_Idempotent() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-twice", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.Require().NoError(s.repo.RollOut(s.ctx, created, s.tenantMigrations()))
	s.NoError(s.repo.RollOut(s.ctx, created, s.tenantMigrations()))
}

func (s *TenantRepoTestSuite) TestRollOut_TenantsAreIsolated() {
	migrated, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-isolated-a", Metadata: []byte(`{}`)})
	s.Require().NoError(err)
	untouched, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-isolated-b", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.Require().NoError(s.repo.RollOut(s.ctx, migrated, s.tenantMigrations()))

	info, err := s.repo.GetSchemaInfo(s.ctx, untouched.ID)
	s.Require().NoError(err)
	s.False(info.Migrated)

	// the second tenant tracks its own version, so it still gets the full rollout
	s.Require().NoError(s.repo.RollOut(s.ctx, untouched, s.tenantMigrations()))
	s.True(tableExists(s.T(), s.ctx, s.pool, TenantSchema(untouched.ID), "order_items"))
}

func (s *TenantRepoTestSuite) TestRollOut_InMemoryMigrations() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-in-memory", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	migrations := fstest.MapFS{
		"000001_create_notes.up.sql":   {Data: []byte("CREATE TABLE notes (id SERIAL PRIMARY KEY);")},
		"000001_create_notes.down.sql": {Data: []byte("DROP TABLE notes;")},
	}

	s.Require().NoError(s.repo.RollOut(s.ctx, created, migrations))
	s.True(tableExists(s.T(), s.ctx, s.pool, TenantSchema(created.ID), "notes"))
}

func (s *TenantRepoTestSuite) TestRollOut_NilTenant() {
	err := s.repo.RollOut(s.ctx, nil, s.tenantMigrations())
	s.ErrorIs(err, ErrNilTenant)
}

func (s *TenantRepoTestSuite) TestRollDown_RemovesTables() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rolldown-tenant", Metadata: []byte(`{}`)})
	s.Require().NoError(err)
	s.Require().NoError(s.repo.RollOut(s.ctx, created, s.tenantMigrations()))

	err = s.repo.RollDown(s.ctx, created, s.tenantMigrations())
	s.Require().NoError(err)

	schemaName := TenantSchema(created.ID)
	s.False(tableExists(s.T(), s.ctx, s.pool, schemaName, "orders"))
	s.False(tableExists(s.T(), s.ctx, s.pool, schemaName, "order_items"))

	info, err := s.repo.GetSchemaInfo(s.ctx, created.ID)
	s.Require().NoError(err)
	s.False(info.Migrated)
}

func (s *TenantRepoTestSuite) TestRollDown_NothingToRollDown() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rolldown-empty", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.NoError(s.repo.RollDown(s.ctx, created, s.tenantMigrations()))
}

func (s *TenantRepoTestSuite) TestRollDown_NilTenant() {
	err := s.repo.RollDown(s.ctx, nil, s.tenantMigrations())
	s.ErrorIs(err, ErrNilTenant)
}

func (s *TenantRepoTestSuite) TestRollOut_NilMigrations() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-nil-fs", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.ErrorIs(s.repo.RollOut(s.ctx, created, nil), ErrNilMigrations)
}

func (s *TenantRepoTestSuite) TestRollDown_NilMigrations() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rolldown-nil-fs", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.ErrorIs(s.repo.RollDown(s.ctx, created, nil), ErrNilMigrations)
}

func (s *TenantRepoTestSuite) TestRollOut_PanickingMigrationsReturnedAsError() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rollout-panics", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.NotPanics(func() {
		err = s.repo.RollOut(s.ctx, created, panickingFS{})
	})
	s.ErrorIs(err, ErrMigrationPanic)
	s.Contains(err.Error(), "boom")
}

func (s *TenantRepoTestSuite) TestRollDown_PanickingMigrationsReturnedAsError() {
	created, err := s.repo.Create(s.ctx, &Tenant{Name: "rolldown-panics", Metadata: []byte(`{}`)})
	s.Require().NoError(err)

	s.NotPanics(func() {
		err = s.repo.RollDown(s.ctx, created, panickingFS{})
	})
	s.ErrorIs(err, ErrMigrationPanic)
	s.Contains(err.Error(), "boom")
}

// panickingFS is a broken caller-supplied fs.FS: any read panics.
type panickingFS struct{}

func (panickingFS) Open(string) (fs.File, error) { panic("boom") }

// tenantMigrations returns test migrations rooted at the .sql files, as callers are expected to pass them.
func (s *TenantRepoTestSuite) tenantMigrations() fs.FS {
	return fstest.MapFS{
		"000001_create_orders.up.sql": {Data: []byte(
			"CREATE TABLE IF NOT EXISTS orders (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), total NUMERIC NOT NULL DEFAULT 0);",
		)},
		"000001_create_orders.down.sql": {Data: []byte("DROP TABLE IF EXISTS orders;")},
		"000002_create_order_items.up.sql": {Data: []byte(
			"CREATE TABLE IF NOT EXISTS order_items (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
				"order_id UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE);",
		)},
		"000002_create_order_items.down.sql": {Data: []byte("DROP TABLE IF EXISTS order_items;")},
	}
}

func tableExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName, tableName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schemaName, tableName,
	).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// skipIfDockerNotAvailable skips the test if Docker is not available.
func skipIfDockerNotAvailable(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker is not available, skipping integration test")
	}
}

// SetupTestContainer starts a PostgreSQL container and returns a pool + cleanup function.
func SetupTestContainer(t *testing.T) (pool *pgxpool.Pool, cleanup func()) {
	t.Helper()
	skipIfDockerNotAvailable(t)
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase(testDBName),
		tcpostgres.WithUsername(testDBUser),
		tcpostgres.WithPassword(testDBPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err = pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	cleanup = func() {
		pool.Close()
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

// ensureTenantProbeTable creates a table in the tenant schema so GetSchemaInfo reports Migrated=true.
// Real queue/consumer migrations are not part of this module; tests only need a non-empty schema.
func ensureTenantProbeTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName string) {
	t.Helper()
	q := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s._tenant_schema_probe (id SMALLINT PRIMARY KEY DEFAULT 1)`,
		quotePGIdent(schemaName),
	)
	_, err := pool.Exec(ctx, q)
	require.NoError(t, err)
}

func quotePGIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
