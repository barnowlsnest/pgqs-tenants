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
)

type (
	Repo interface {
		Create(ctx context.Context, t *Tenant) (*Tenant, error)
		Update(ctx context.Context, id uuid.UUID, params *UpdateTenantParams) (*Tenant, error)
		Get(ctx context.Context, id uuid.UUID) (*Tenant, error)
		GetAll(ctx context.Context) ([]*Tenant, error)
		SoftDelete(ctx context.Context, id uuid.UUID) error
		DeleteTenantSchema(ctx context.Context, tenantID uuid.UUID) error
		GetSchemaInfo(ctx context.Context, tenantID uuid.UUID) (*SchemaInfo, error)
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

// NewService creates and initializes a new Service instance with the provided configuration options.
// Returns a pointer to the Service and an error if validation fails.
func NewService(opts ...ConfigFunc) (*Service, error) {
	srv := &Service{}
	for _, opt := range opts {
		opt(srv)
	}

	if err := srv.validate(); err != nil {
		return nil, err
	}

	return srv, nil
}

// CreateTenant creates a new tenant in the repository and returns the created tenant or an error.
func (s *Service) CreateTenant(ctx context.Context, tenant *Tenant) (*Tenant, error) {
	result, err := s.repo.Create(ctx, &Tenant{
		Name:     tenant.Name,
		Metadata: tenant.Metadata,
	})
	if err != nil {
		log.Error(fmt.Errorf("failed to create tenant: %w", err))
		return nil, err
	}

	return result, nil
}

// UpdateTenant updates an existing tenant record in the repository with the provided tenant and returns the updated tenant or an error.
func (s *Service) UpdateTenant(ctx context.Context, tenant *Tenant) (*Tenant, error) {
	result, err := s.repo.Update(ctx, tenant.ID, &UpdateTenantParams{
		Status:   tenant.Status,
		Metadata: tenant.Metadata,
	})
	if err != nil {
		log.Error(fmt.Errorf("failed to update tenant %s: %w", tenant.ID, err))
		return nil, err
	}

	return result, nil
}

// GetTenant retrieves a tenant from the repository by its ID, returning it or an error if one occurs.
func (s *Service) GetTenant(ctx context.Context, id uuid.UUID) (*Tenant, error) {
	result, err := s.repo.Get(ctx, id)
	if err != nil {
		log.Error(fmt.Errorf("failed to get tenant %s: %w", id, err))
		return nil, err
	}

	return result, nil
}

// GetAllTenants retrieves all tenants from the repository and returns them along with any encountered error.
func (s *Service) GetAllTenants(ctx context.Context) ([]*Tenant, error) {
	result, err := s.repo.GetAll(ctx)
	if err != nil {
		log.Error(fmt.Errorf("failed to get all tenants: %w", err))
		return nil, err
	}

	return result, nil
}

// DisableTenant deactivates a tenant by performing a soft delete and returns an error if the operation fails.
func (s *Service) DisableTenant(ctx context.Context, id uuid.UUID) error {
	if err := s.repo.SoftDelete(ctx, id); err != nil {
		log.Error(fmt.Errorf("failed to disable tenant %s: %w", id, err))
		return err
	}

	return nil
}

// PurgeTenant permanently removes the tenant's schema from the database and returns an error if the process fails.
func (s *Service) PurgeTenant(ctx context.Context, id uuid.UUID) error {
	if err := s.repo.DeleteTenantSchema(ctx, id); err != nil {
		log.Error(fmt.Errorf("failed to purge tenant %s: %w", id, err))
		return err
	}

	return nil
}

// GetSchemaInfo retrieves schema existence and migration status for a tenant.
func (s *Service) GetSchemaInfo(ctx context.Context, id uuid.UUID) (*SchemaInfo, error) {
	info, err := s.repo.GetSchemaInfo(ctx, id)
	if err != nil {
		log.Error(fmt.Errorf("failed to get schema info for tenant %s: %w", id, err))
		return nil, err
	}

	return info, nil
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
