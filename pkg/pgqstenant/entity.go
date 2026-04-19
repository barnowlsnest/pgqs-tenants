package pgqstenant

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	TenantsTableName          = "tenants"
	PGQSSchema                = "pgqs"
	PGQSTenantSchemaPrefix    = PGQSSchema + "_tenant"
	PGQSTenantSchemaSeparator = "_"
)

type (
	Tenant struct {
		ID         uuid.UUID `db:"id"          goqu:"defaultifempty"`
		Name       string    `db:"name"        goqu:"omitempty"`
		SchemaName string    `db:"schema_name" goqu:"skipinsert"`
		Status     string    `db:"status"      goqu:"omitempty"`
		CreatedAt  time.Time `db:"created_at"  goqu:"omitempty"`
		UpdatedAt  time.Time `db:"updated_at"  goqu:"omitempty"`
		Metadata   []byte    `db:"metadata"`
	}

	UpdateTenantParams struct {
		Status   string
		Metadata []byte
	}

	SchemaInfo struct {
		Exists   bool `db:"schema_exists"`
		Migrated bool `db:"migrated"`
	}

	TenantMetadata struct {
		Engine string `json:"engine,omitempty"`
	}
)

func TenantSchema(id uuid.UUID) string {
	return PGQSTenantSchemaPrefix + PGQSTenantSchemaSeparator + id.String()
}

func TenantsTable() string {
	return fmt.Sprintf("%s.%s", PGQSSchema, TenantsTableName)
}
