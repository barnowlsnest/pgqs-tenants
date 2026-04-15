//go:build integration

package pgqstenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	harnesspg "github.com/barnowlsnest/pgqs-harness/postgres"
)

type MgrSuite struct {
	suite.Suite
	ctx       context.Context
	cancel    context.CancelFunc
	container *postgres.PostgresContainer
	dbURL     string
}

func TestMgrSuite(t *testing.T) {
	suite.Run(t, new(MgrSuite))
}

func (s *MgrSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)

	ctr, err := postgres.Run(s.ctx, "docker.io/postgres:16-alpine",
		postgres.WithDatabase("pgqs"),
		postgres.WithUsername("pgqs"),
		postgres.WithPassword("pgqs"),
		postgres.BasicWaitStrategies(),
	)
	s.Require().NoError(err)
	s.container = ctr

	s.dbURL, err = ctr.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)
}

func (s *MgrSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.container != nil {
		s.Require().NoError(testcontainers.TerminateContainer(s.container))
	}
}

func (s *MgrSuite) openDB() *sql.DB {
	db, err := sql.Open("postgres", s.dbURL)
	s.Require().NoError(err)
	s.Require().NoError(db.PingContext(s.ctx))
	return db
}

func tenantSchemaName(id uuid.UUID) string {
	return "pgqs_tenant_" + id.String()
}

func (s *MgrSuite) createTenantSchema(db *sql.DB, id uuid.UUID) {
	_, err := db.ExecContext(s.ctx, `CREATE SCHEMA IF NOT EXISTS `+pq.QuoteIdentifier(tenantSchemaName(id)))
	s.Require().NoError(err)
}

func (s *MgrSuite) TestMigrateUpThenDown() {
	tenantID := uuid.New()
	db := s.openDB()
	defer func() { s.Require().NoError(db.Close()) }()

	s.createTenantSchema(db, tenantID)

	s.Require().NoError(MigrateUP(s.dbURL, tenantID))

	var tenantsExists bool
	err := db.QueryRowContext(s.ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'pgqs' AND table_name = 'tenants'
		)`).Scan(&tenantsExists)
	s.Require().NoError(err)
	s.True(tenantsExists)

	var statusExists bool
	err = db.QueryRowContext(s.ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'pgqs' AND table_name = 'tenants' AND column_name = 'status'
		)`).Scan(&statusExists)
	s.Require().NoError(err)
	s.True(statusExists)

	for {
		err := MigrateDOWN(s.dbURL, tenantID)
		if errors.Is(err, migrate.ErrNoChange) {
			break
		}
		s.Require().NoError(err)
	}

	var pgqsExists bool
	err = db.QueryRowContext(s.ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgqs'
		)`).Scan(&pgqsExists)
	s.Require().NoError(err)
	s.False(pgqsExists, "final down migration should drop pgqs schema")
}

// tenantNotifyPayload matches pgqs.notify_tenant_status_change / pg_notify('tenants', ...).
type tenantNotifyPayload struct {
	ID     uuid.UUID `json:"id"`
	Schema string    `json:"schema"`
	Event  string    `json:"event"`
}

func (s *MgrSuite) TestTenantTriggers_notifyViaHarnessListener() {
	tenantID := uuid.New()
	db := s.openDB()
	defer func() { s.Require().NoError(db.Close()) }()

	s.createTenantSchema(db, tenantID)
	s.Require().NoError(MigrateUP(s.dbURL, tenantID))
	defer func() {
		for {
			err := MigrateDOWN(s.dbURL, tenantID)
			if errors.Is(err, migrate.ErrNoChange) {
				return
			}
			s.Require().NoError(err)
		}
	}()

	pool, err := pgxpool.New(s.ctx, s.dbURL)
	s.Require().NoError(err)
	defer pool.Close()

	listener, err := harnesspg.NewListenerFromPool(s.ctx, pool, "tenants", 2)
	s.Require().NoError(err)
	defer listener.Stop(10 * time.Second)
	s.Require().NoError(listener.Start(s.ctx))

	pub, err := pool.Acquire(s.ctx)
	s.Require().NoError(err)
	defer pub.Release()

	name := "harness-listen-" + uuid.New().String()
	var rowID uuid.UUID
	err = pub.QueryRow(s.ctx, `INSERT INTO pgqs.tenants (name) VALUES ($1) RETURNING id`, name).Scan(&rowID)
	s.Require().NoError(err)

	wantSchema := tenantSchemaName(rowID)
	select {
	case n := <-listener.Notifications():
		s.Require().NotNil(n)
		s.Equal("tenants", n.Channel)
		var p tenantNotifyPayload
		s.Require().NoError(json.Unmarshal([]byte(n.Payload), &p))
		s.Equal(rowID, p.ID)
		s.Equal(wantSchema, p.Schema)
		s.Equal("created", p.Event)
	case <-time.After(15 * time.Second):
		s.Fail("timed out waiting for INSERT notify")
	}
	s.NoError(listener.Err())

	_, err = pub.Exec(s.ctx, `UPDATE pgqs.tenants SET status = 'ready' WHERE id = $1`, rowID)
	s.Require().NoError(err)

	select {
	case n := <-listener.Notifications():
		s.Require().NotNil(n)
		s.Equal("tenants", n.Channel)
		var p tenantNotifyPayload
		s.Require().NoError(json.Unmarshal([]byte(n.Payload), &p))
		s.Equal(rowID, p.ID)
		s.Equal(wantSchema, p.Schema)
		s.Equal("ready", p.Event)
	case <-time.After(15 * time.Second):
		s.Fail("timed out waiting for UPDATE notify")
	}
	s.NoError(listener.Err())
}
