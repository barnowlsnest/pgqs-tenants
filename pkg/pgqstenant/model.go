package pgqstenant

import (
	"time"
	
	"github.com/google/uuid"
)

type (
	Model struct {
		ID                uuid.UUID
		Name              string
		SchemaName        string
		Status            string
		ControlState      State
		CreatedAt         time.Time
		UpdatedAt         time.Time
		Metadata          []byte
		SchemaTablesCount int
		SchemaExists      bool
	}
)

func fromEntity(tenantEntity *Tenant) *Model {
	return &Model{
		ID:         tenantEntity.ID,
		Name:       tenantEntity.Name,
		SchemaName: tenantEntity.SchemaName,
		Status:     tenantEntity.State,
		Metadata:   tenantEntity.Metadata,
		CreatedAt:  tenantEntity.CreatedAt,
		UpdatedAt:  tenantEntity.UpdatedAt,
	}
}

func (m *Model) IDString() string {
	return m.ID.String()
}

func (m *Model) newCopy() *Model {
	var model Model
	if m == nil {
		return &model
	}
	
	model.ID = m.ID
	model.Name = m.Name
	model.SchemaName = m.SchemaName
	model.Status = m.Status
	model.ControlState = m.ControlState
	model.CreatedAt = m.CreatedAt
	model.UpdatedAt = m.UpdatedAt
	model.Metadata = m.Metadata
	model.SchemaTablesCount = m.SchemaTablesCount
	model.SchemaExists = m.SchemaExists
	
	return &model
}
