package pgqstenants

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	TenantsTable              = "tenants"
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

var ControlPlaneStates = []State{Created, Up, Down, Disabled, Purged}

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

	TenantStatusPayload struct {
		TenantSchema string    `json:"tenant_schema"`
		State        string    `json:"state"`
		ID           uuid.UUID `json:"tenant_id"`
	}

	TenantMetadata struct {
		Engine string `json:"engine,omitempty"`
	}
)

func PGQSTenantSchema(id uuid.UUID) string {
	return PGQSTenantSchemaPrefix + PGQSTenantSchemaSeparator + id.String()
}

func PGQSTenantsTable() string {
	return fmt.Sprintf("%s.%s", PGQSSchema, TenantsTable)
}

func NotifyTenantStatusSQL(id uuid.UUID, status string) (string, error) {
	payload := &TenantStatusPayload{
		TenantSchema: PGQSTenantSchema(id),
		State:        status,
		ID:           id,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("SELECT pg_notify('%s', '%s');", TenantsTable, string(data)), nil
}

func IsAnyOfStates(state State) bool {
	return slices.Contains(ControlPlaneStates, state)
}
