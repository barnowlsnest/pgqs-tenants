package pgqs_tenants

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	TenantsTableName          = "tenants"
	PGQSSchema                = "pgqs"
	PGQSTenantSchemaPrefix    = PGQSSchema + "_tenant"
	PGQSTenantSchemaSeparator = "_"
)

const (
	Created  State = "created"
	Up       State = "up"
	Down     State = "down"
	Disabled State = "disabled"
	Purged   State = "purged"
)

var States = []State{Created, Up, Down, Disabled, Purged}

type (
	State = string

	Tenant struct {
		ID         uuid.UUID `db:"id"          goqu:"defaultifempty"`
		Name       string    `db:"name"        goqu:"omitempty"`
		SchemaName string    `db:"schema_name" goqu:"skipinsert"`
		State      string    `db:"state"       goqu:"omitempty"`
		Status     string    `db:"status"      goqu:"omitempty"`
		CreatedAt  time.Time `db:"created_at"  goqu:"omitempty"`
		UpdatedAt  time.Time `db:"updated_at"  goqu:"omitempty"`
		Metadata   []byte    `db:"metadata"`
	}

	UpdateTenantParams struct {
		Status   string
		Metadata []byte
	}

	TenantSchemaState struct {
		SchemaTablesCount int  `db:"schema_tables_count"`
		SchemaExists      bool `db:"schema_exists"`
		IsDisabled        bool `db:"is_disabled"`
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

func IsAnyOfStates(state State) bool {
	return slices.Contains(States, state)
}
