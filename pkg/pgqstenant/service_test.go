package pgqstenant

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type mockRepo struct {
	mock.Mock
}

func (m *mockRepo) Create(ctx context.Context, t *Tenant) (*Tenant, error) {
	args := m.Called(ctx, t)
	var tenant *Tenant
	if v := args.Get(0); v != nil {
		tenant = v.(*Tenant)
	}
	return tenant, args.Error(1)
}

func (m *mockRepo) Update(ctx context.Context, id uuid.UUID, params *UpdateTenantParams) (*Tenant, error) {
	args := m.Called(ctx, id, params)
	var tenant *Tenant
	if v := args.Get(0); v != nil {
		tenant = v.(*Tenant)
	}
	return tenant, args.Error(1)
}

func (m *mockRepo) Get(ctx context.Context, id uuid.UUID) (*Tenant, error) {
	args := m.Called(ctx, id)
	var tenant *Tenant
	if v := args.Get(0); v != nil {
		tenant = v.(*Tenant)
	}
	return tenant, args.Error(1)
}

func (m *mockRepo) GetAll(ctx context.Context) ([]*Tenant, error) {
	args := m.Called(ctx)
	var tenants []*Tenant
	if v := args.Get(0); v != nil {
		tenants = v.([]*Tenant)
	}
	return tenants, args.Error(1)
}

func (m *mockRepo) SoftDelete(ctx context.Context, id uuid.UUID) error {
	return m.Called(ctx, id).Error(0)
}

func (m *mockRepo) DeleteTenantSchema(ctx context.Context, tenantID uuid.UUID) error {
	return m.Called(ctx, tenantID).Error(0)
}

func (m *mockRepo) GetSchemaInfo(ctx context.Context, tenantID uuid.UUID) (*SchemaInfo, error) {
	args := m.Called(ctx, tenantID)
	var info *SchemaInfo
	if v := args.Get(0); v != nil {
		info = v.(*SchemaInfo)
	}
	return info, args.Error(1)
}

var _ Repo = (*mockRepo)(nil)

type ServiceTestSuite struct {
	suite.Suite
	ctx  context.Context
	repo *mockRepo
	svc  *Service
}

func TestServiceSuite(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}

func (s *ServiceTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.resetMock()
}

func (s *ServiceTestSuite) TearDownTest() {
	s.repo.AssertExpectations(s.T())
}

func (s *ServiceTestSuite) resetMock() {
	s.repo = new(mockRepo)
	svc, err := NewService(WithDB("postgres://stub"), WithTenantRepo(s.repo))
	s.Require().NoError(err)
	s.svc = svc
}

func (s *ServiceTestSuite) TestNew() {
	cases := []struct {
		name    string
		opts    []ConfigFunc
		wantErr error
	}{
		{
			name: "happy path with dbURL and repo",
			opts: []ConfigFunc{WithDB("postgres://stub"), WithTenantRepo(new(mockRepo))},
		},
		{
			name:    "missing dbURL",
			opts:    []ConfigFunc{WithTenantRepo(new(mockRepo))},
			wantErr: ErrMigrationConfig,
		},
		{
			name:    "missing repo",
			opts:    []ConfigFunc{WithDB("postgres://stub")},
			wantErr: ErrMigrationConfig,
		},
		{
			name:    "both missing",
			opts:    nil,
			wantErr: ErrMigrationConfig,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			got, err := NewService(tc.opts...)

			if tc.wantErr != nil {
				s.Require().Error(err)
				s.ErrorIs(err, tc.wantErr)
				s.Nil(got)
				return
			}
			s.Require().NoError(err)
			s.NotNil(got)
		})
	}
}

func (s *ServiceTestSuite) TestCreateTenant() {
	tenantID := uuid.New()
	now := time.Now().UTC()
	repoErr := errors.New("repo create failed")

	cases := []struct {
		name        string
		input       *Tenant
		matchTenant func(*Tenant) bool
		repoReturn  *Tenant
		repoErr     error
		want        *Tenant
		wantErr     bool
	}{
		{
			name:  "happy path forwards name and metadata",
			input: &Tenant{Name: "alpha", Metadata: []byte(`{"k":"v"}`)},
			matchTenant: func(t *Tenant) bool {
				return t != nil && t.Name == "alpha" && string(t.Metadata) == `{"k":"v"}`
			},
			repoReturn: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"k":"v"}`),
			},
			want: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"k":"v"}`),
			},
		},
		{
			name:  "repo error propagates and result is nil",
			input: &Tenant{Name: "beta"},
			matchTenant: func(t *Tenant) bool {
				return t != nil && t.Name == "beta"
			},
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("Create", mock.Anything, mock.MatchedBy(tc.matchTenant)).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.CreateTenant(s.ctx, tc.input)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.want, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestUpdateTenant() {
	tenantID := uuid.New()
	now := time.Now().UTC()
	repoErr := errors.New("repo update failed")

	cases := []struct {
		name        string
		input       *Tenant
		matchParams func(*UpdateTenantParams) bool
		repoReturn  *Tenant
		repoErr     error
		want        *Tenant
		wantErr     bool
	}{
		{
			name: "happy path forwards id, status, metadata",
			input: &Tenant{
				ID:       tenantID,
				Status:   "ready",
				Metadata: []byte(`{"env":"prod"}`),
			},
			matchParams: func(p *UpdateTenantParams) bool {
				return p != nil && p.Status == "ready" && string(p.Metadata) == `{"env":"prod"}`
			},
			repoReturn: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "ready",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"env":"prod"}`),
			},
			want: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "ready",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"env":"prod"}`),
			},
		},
		{
			name: "repo error propagates and result is nil",
			input: &Tenant{
				ID:     tenantID,
				Status: "ready",
			},
			matchParams: func(p *UpdateTenantParams) bool {
				return p != nil && p.Status == "ready"
			},
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("Update", mock.Anything, tenantID, mock.MatchedBy(tc.matchParams)).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.UpdateTenant(s.ctx, tc.input)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.want, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestGetTenant() {
	tenantID := uuid.New()
	now := time.Now().UTC()
	repoErr := errors.New("repo get failed")

	cases := []struct {
		name       string
		inputID    uuid.UUID
		repoReturn *Tenant
		repoErr    error
		want       *Tenant
		wantErr    bool
	}{
		{
			name:    "happy path returns tenant",
			inputID: tenantID,
			repoReturn: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{}`),
			},
			want: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{}`),
			},
		},
		{
			name:    "repo error propagates and result is nil",
			inputID: tenantID,
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("Get", mock.Anything, tc.inputID).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.GetTenant(s.ctx, tc.inputID)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.want, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestGetAllTenants() {
	id1, id2 := uuid.New(), uuid.New()
	now := time.Now().UTC()
	repoErr := errors.New("repo getall failed")

	multiTenants := []*Tenant{
		{
			ID: id1, Name: "alpha", SchemaName: TenantSchema(id1),
			Status:    "created",
			CreatedAt: now, UpdatedAt: now, Metadata: []byte(`{}`),
		},
		{
			ID: id2, Name: "beta", SchemaName: TenantSchema(id2),
			Status:    "ready",
			CreatedAt: now, UpdatedAt: now, Metadata: []byte(`{}`),
		},
	}

	cases := []struct {
		name       string
		repoReturn []*Tenant
		repoErr    error
		want       []*Tenant
		wantErr    bool
	}{
		{
			name:       "happy empty",
			repoReturn: []*Tenant{},
			want:       []*Tenant{},
		},
		{
			name:       "happy multiple preserves order",
			repoReturn: multiTenants,
			want:       multiTenants,
		},
		{
			name:    "repo error propagates and result is nil",
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("GetAll", mock.Anything).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.GetAllTenants(s.ctx)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.want, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestDisableTenant() {
	tenantID := uuid.New()
	repoErr := errors.New("repo softdelete failed")

	cases := []struct {
		name    string
		inputID uuid.UUID
		repoErr error
		wantErr bool
	}{
		{
			name:    "happy path forwards id",
			inputID: tenantID,
		},
		{
			name:    "repo error propagates",
			inputID: tenantID,
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("SoftDelete", mock.Anything, tc.inputID).
				Return(tc.repoErr)

			err := s.svc.DisableTenant(s.ctx, tc.inputID)

			if tc.wantErr {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestPurgeTenant() {
	tenantID := uuid.New()
	repoErr := errors.New("repo delete schema failed")

	cases := []struct {
		name    string
		inputID uuid.UUID
		repoErr error
		wantErr bool
	}{
		{
			name:    "happy path forwards id",
			inputID: tenantID,
		},
		{
			name:    "repo error propagates",
			inputID: tenantID,
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("DeleteTenantSchema", mock.Anything, tc.inputID).
				Return(tc.repoErr)

			err := s.svc.PurgeTenant(s.ctx, tc.inputID)

			if tc.wantErr {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestGetSchemaInfo() {
	tenantID := uuid.New()
	repoErr := errors.New("repo get schema info failed")

	cases := []struct {
		name       string
		inputID    uuid.UUID
		repoReturn *SchemaInfo
		repoErr    error
		wantInfo   *SchemaInfo
		wantErr    bool
	}{
		{
			name:       "happy path exists and migrated",
			inputID:    tenantID,
			repoReturn: &SchemaInfo{Exists: true, Migrated: true},
			wantInfo:   &SchemaInfo{Exists: true, Migrated: true},
		},
		{
			name:       "happy path exists not migrated",
			inputID:    tenantID,
			repoReturn: &SchemaInfo{Exists: true, Migrated: false},
			wantInfo:   &SchemaInfo{Exists: true, Migrated: false},
		},
		{
			name:       "happy path does not exist",
			inputID:    tenantID,
			repoReturn: &SchemaInfo{Exists: false, Migrated: false},
			wantInfo:   &SchemaInfo{Exists: false, Migrated: false},
		},
		{
			name:    "repo error propagates",
			inputID: tenantID,
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("GetSchemaInfo", mock.Anything, tc.inputID).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.GetSchemaInfo(s.ctx, tc.inputID)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.wantInfo, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}
