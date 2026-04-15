package pgqstenant

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	harnesspg "github.com/barnowlsnest/pgqs-harness/postgres"
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
	RunControllerMigrations(s.T(), s.pool)
	s.repo = NewTenantRepo(s.pool)
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
	// tenants_state_check: state IN ('created', 'ready', 'disabled')
	s.Equal("created", result.State)
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

	// Align with tenants_state_check (state) and repo ON CONFLICT (status = 'disabled').
	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET state = 'disabled', status = 'disabled' WHERE id = $1",
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
	s.Equal("created", reactivated.State)
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
		"UPDATE pgqs.tenants SET state = 'disabled', status = 'disabled' WHERE id = $1",
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
	s.Equal("created", result.State)
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

func (s *TenantRepoTestSuite) TestUpdate_StatusParam_setsStatusColumnNotState() {
	// TenantRepo.Update writes UpdateTenantParams.Status to the DB "status" column (post-000004),
	// not the "state" column governed by tenants_state_check.
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
	s.Equal("created", updated.State, "state unchanged unless repo sets it")
}

func (s *TenantRepoTestSuite) TestStateColumn_tenantsStateCheck_ready() {
	tenant := &Tenant{
		Name:     "state-ready-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET state = 'ready' WHERE id = $1",
		created.ID,
	)
	s.Require().NoError(err)

	got, err := s.repo.Get(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal("ready", got.State)
	s.Equal("created", got.Status)
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
	// tenants_state_check still allows current state value
	s.Contains([]string{"created", "ready", "disabled"}, result.State)
}

func (s *TenantRepoTestSuite) TestSoftDelete_NotFound() {
	randomID := uuid.New()
	err := s.repo.SoftDelete(s.ctx, randomID)
	s.Error(err)
	s.Contains(err.Error(), "not a single row updated")
}

func (s *TenantRepoTestSuite) TestGetTenantSchemaState_Up() {
	// Create a tenant (schema is created automatically)
	tenant := &Tenant{
		Name:     "schema-state-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Non-controller tables in the tenant schema mean "up" for GetTenantSchemaState.
	schemaName := TenantSchema(created.ID)
	ensureTenantProbeTable(s.T(), s.ctx, s.pool, schemaName)

	// tenants_state_check: move lifecycle state to ready (distinct from "status" column)
	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET state = 'ready' WHERE id = $1",
		created.ID,
	)
	s.Require().NoError(err)

	state, err := s.repo.GetTenantSchemaState(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal("up", state)
}

func (s *TenantRepoTestSuite) TestGetTenantSchemaState_Down() {
	// Create a tenant (schema is created but no tables)
	tenant := &Tenant{
		Name:     "schema-down-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	state, err := s.repo.GetTenantSchemaState(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal("down", state)
}

func (s *TenantRepoTestSuite) TestGetTenantSchemaState_Disabled() {
	// Create a tenant with tables and mark as disabled
	tenant := &Tenant{
		Name:     "schema-disabled-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Non-controller tables in the tenant schema mean "up" for GetTenantSchemaState.
	schemaName := TenantSchema(created.ID)
	ensureTenantProbeTable(s.T(), s.ctx, s.pool, schemaName)

	_, err = s.pool.Exec(s.ctx,
		"UPDATE pgqs.tenants SET state = 'disabled', status = 'disabled' WHERE id = $1",
		created.ID,
	)
	s.Require().NoError(err)

	state, err := s.repo.GetTenantSchemaState(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal("disabled", state)
}

func (s *TenantRepoTestSuite) TestGetTenantSchemaState_SchemaNotFound() {
	// Create a tenant and drop its schema
	tenant := &Tenant{
		Name:     "no-schema-tenant",
		Metadata: []byte(`{}`),
	}
	created, err := s.repo.Create(s.ctx, tenant)
	s.Require().NoError(err)

	// Drop the schema
	schemaName := TenantSchema(created.ID)
	_, err = s.pool.Exec(s.ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	s.Require().NoError(err)

	_, err = s.repo.GetTenantSchemaState(s.ctx, created.ID)
	s.ErrorIs(err, ErrSchemaNotFound)
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

// projectRoot returns the absolute path to the module root (directory containing repo_test.go).
func projectRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get caller information")
	}
	return filepath.Dir(file)
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

// RunControllerMigrations applies SQL from ./migrations (*.sql at repo root).
func RunControllerMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	migrationsPath := filepath.Join(projectRoot(), "migrations")
	sourceURL := "file://" + migrationsPath

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	cfg := conn.Conn().Config()
	dbURL := "postgres://" + cfg.User + ":" + cfg.Password +
		"@" + cfg.Host + ":" + strconv.Itoa(int(cfg.Port)) +
		"/" + cfg.Database + "?sslmode=disable"

	m, err := migrate.New(sourceURL, dbURL)
	require.NoError(t, err)

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		t.Fatalf("failed to run migrations: %v", err)
	}

	srcErr, dbErr := m.Close()
	if srcErr != nil {
		t.Logf("failed to close migration source: %v", srcErr)
	}
	if dbErr != nil {
		t.Logf("failed to close migration db: %v", dbErr)
	}
}

// ensureTenantProbeTable creates a table in the tenant schema so GetTenantSchemaState reports "up".
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
