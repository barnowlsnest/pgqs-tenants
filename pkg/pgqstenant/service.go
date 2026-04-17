package pgqstenant

import (
	"context"
	"errors"
	"fmt"

	log "github.com/barnowlsnest/go-logslib/v2/pkg/sharedlog"
	"github.com/google/uuid"
)

var (
	ErrMigrationConfig = errors.New("migration config in not valid")
	ErrUnknownState    = errors.New("unknown state error")
)

type (
	Repo interface {
		Create(ctx context.Context, t *Tenant) (*Tenant, error)
		Update(ctx context.Context, id uuid.UUID, params *UpdateTenantParams) (*Tenant, error)
		Get(ctx context.Context, id uuid.UUID) (*Tenant, error)
		GetAll(ctx context.Context) ([]*Tenant, error)
		SoftDelete(ctx context.Context, id uuid.UUID) error
		DeleteTenantSchema(ctx context.Context, tenantID uuid.UUID) error
		GetTenantSchemaState(ctx context.Context, tenantID uuid.UUID) (string, error)
	}

	// Service represents a service layer that manages tenant operations through a repository and a database connection URL.
	Service struct {
		dbURL string
		repo  Repo
	}

	ConfigFunc func(srv *Service)
)

// WithDB sets the database connection URL for the service and returns a configuration function.
func WithDB(url string) ConfigFunc {
	return func(srv *Service) {
		srv.dbURL = url
	}
}

// WithTenantRepo sets the tenant repository for the Service configuration.
func WithTenantRepo(repo Repo) ConfigFunc {
	return func(srv *Service) {
		srv.repo = repo
	}
}

// New creates and initializes a new Service instance with the provided configuration options.
// Returns a pointer to the Service and an error if validation fails.
func New(opts ...ConfigFunc) (*Service, error) {
	srv := &Service{}
	for _, opt := range opts {
		opt(srv)
	}

	if err := srv.validate(); err != nil {
		return nil, err
	}

	return srv, nil
}

// CreateTenant creates a new tenant in the repository and returns the created tenant model or an error.
func (s *Service) CreateTenant(ctx context.Context, tenantModel *Model) (*Model, error) {
	m := tenantModel.newCopy()

	nTenant, err := s.repo.Create(ctx, &Tenant{
		Name:     m.Name,
		Metadata: m.Metadata,
	})
	if err != nil {
		log.Error(fmt.Errorf("failed to create tenant: %w", err))
		return nil, err
	}

	return fromEntity(nTenant), nil
}

// UpdateTenant updates an existing tenant record in the repository with the provided model and returns the updated model or an error.
func (s *Service) UpdateTenant(ctx context.Context, tenantModel *Model) (*Model, error) {
	m := tenantModel.newCopy()

	tenantEntity, err := s.repo.Update(ctx, m.ID, &UpdateTenantParams{
		Status:   m.Status,
		Metadata: m.Metadata,
	})

	if err != nil {
		log.Error(fmt.Errorf("failed to update tenant %s: %w", m.IDString(), err))
		return nil, err
	}

	return fromEntity(tenantEntity), nil
}

// GetTenant retrieves a tenant model from the repository by its ID, returning it or an error if one occurs.
func (s *Service) GetTenant(ctx context.Context, tenantModel *Model) (*Model, error) {
	m := tenantModel.newCopy()

	tenantEntity, err := s.repo.Get(ctx, m.ID)
	if err != nil {
		log.Error(fmt.Errorf("failed to get tenant %s: %w", m.IDString(), err))
		return nil, err
	}

	return fromEntity(tenantEntity), nil
}

// GetAllTenants retrieves all tenant models from the repository and returns them along with any encountered error.
func (s *Service) GetAllTenants(ctx context.Context) ([]*Model, error) {
	tenantEntities, err := s.repo.GetAll(ctx)
	if err != nil {
		log.Error(fmt.Errorf("failed to get all tenants: %w", err))
		return nil, err
	}

	models := make([]*Model, len(tenantEntities))
	for i := range len(tenantEntities) {
		models[i] = fromEntity(tenantEntities[i])
	}

	return models, nil
}

// DisableTenant deactivates a tenant by performing a soft delete and returns an error if the operation fails.
func (s *Service) DisableTenant(ctx context.Context, tenantModel *Model) error {
	m := tenantModel.newCopy()

	if err := s.repo.SoftDelete(ctx, m.ID); err != nil {
		log.Error(fmt.Errorf("failed to disable tenant %s: %w", m.IDString(), err))
		return err
	}

	return nil
}

// PurgeTenant permanently removes the tenant's schema from the database and returns an error if the process fails.
func (s *Service) PurgeTenant(ctx context.Context, tenantModel *Model) error {
	m := tenantModel.newCopy()

	if err := s.repo.DeleteTenantSchema(ctx, m.ID); err != nil {
		log.Error(fmt.Errorf("failed to purge tenant %s: %w", m.IDString(), err))
		return err
	}

	return nil
}

// Up applies the latest migrations for a tenant's schema identified by the given tenantID and returns an error if unsuccessful.
func (s *Service) Up(tenantModel *Model) error {
	m := tenantModel.newCopy()

	return MigrateUP(s.dbURL, m.ID)
}

// Down tears down the migration for a tenant's schema identified by the given tenantID and returns an error if unsuccessful.
func (s *Service) Down(tenantModel *Model) error {
	m := tenantModel.newCopy()

	return MigrateDOWN(s.dbURL, m.ID)
}

func (s *Service) validate() error {
	switch {
	case s.dbURL == "":
		return errors.Join(
			ErrMigrationConfig,
			errors.New("database URL cannot be empty"),
		)
	case s.repo == nil:
		return errors.Join(
			ErrMigrationConfig,
			errors.New("tenant repository cannot be nil"),
		)
	}

	return nil
}
