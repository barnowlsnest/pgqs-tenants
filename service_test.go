package pgqs_tenants

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

func (m *mockRepo) GetTenantSchemaState(ctx context.Context, tenantID uuid.UUID) (string, error) {
	args := m.Called(ctx, tenantID)
	return args.String(0), args.Error(1)
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
	svc, err := New(WithDB("postgres://stub"), WithTenantRepo(s.repo))
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
			got, err := New(tc.opts...)

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
		input       *Model
		matchTenant func(*Tenant) bool
		repoReturn  *Tenant
		repoErr     error
		wantModel   *Model
		wantErr     bool
	}{
		{
			name:  "happy path forwards name and metadata, maps response",
			input: &Model{Name: "alpha", Metadata: []byte(`{"k":"v"}`)},
			matchTenant: func(t *Tenant) bool {
				return t != nil && t.Name == "alpha" && string(t.Metadata) == `{"k":"v"}`
			},
			repoReturn: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				State:      "created",
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"k":"v"}`),
			},
			wantModel: &Model{
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
			input: &Model{Name: "beta"},
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
				s.Equal(tc.wantModel, got)
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
		input       *Model
		matchParams func(*UpdateTenantParams) bool
		repoReturn  *Tenant
		repoErr     error
		wantModel   *Model
		wantErr     bool
	}{
		{
			name: "happy path forwards id, status, metadata",
			input: &Model{
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
				State:      "ready",
				Status:     "ready",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{"env":"prod"}`),
			},
			wantModel: &Model{
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
			input: &Model{
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
				s.Equal(tc.wantModel, got)
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
		input      *Model
		repoReturn *Tenant
		repoErr    error
		wantModel  *Model
		wantErr    bool
	}{
		{
			name:  "happy path maps entity to model",
			input: &Model{ID: tenantID},
			repoReturn: &Tenant{
				ID:         tenantID,
				Name:       "alpha",
				SchemaName: TenantSchema(tenantID),
				State:      "created",
				Status:     "created",
				CreatedAt:  now,
				UpdatedAt:  now,
				Metadata:   []byte(`{}`),
			},
			wantModel: &Model{
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
			input:   &Model{ID: tenantID},
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("Get", mock.Anything, tenantID).
				Return(tc.repoReturn, tc.repoErr)

			got, err := s.svc.GetTenant(s.ctx, tc.input)

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
			} else {
				s.Require().NoError(err)
				s.Equal(tc.wantModel, got)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestGetAllTenants() {
	id1, id2 := uuid.New(), uuid.New()
	now := time.Now().UTC()
	repoErr := errors.New("repo getall failed")

	multiEntities := []*Tenant{
		{
			ID: id1, Name: "alpha", SchemaName: TenantSchema(id1),
			State: "created", Status: "created",
			CreatedAt: now, UpdatedAt: now, Metadata: []byte(`{}`),
		},
		{
			ID: id2, Name: "beta", SchemaName: TenantSchema(id2),
			State: "ready", Status: "ready",
			CreatedAt: now, UpdatedAt: now, Metadata: []byte(`{}`),
		},
	}
	multiModels := []*Model{
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
		wantModels []*Model
		wantErr    bool
	}{
		{
			name:       "happy empty",
			repoReturn: []*Tenant{},
			wantModels: []*Model{},
		},
		{
			name:       "happy multiple preserves order and maps each",
			repoReturn: multiEntities,
			wantModels: multiModels,
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
				s.Equal(tc.wantModels, got)
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
		input   *Model
		repoErr error
		wantErr bool
	}{
		{
			name:  "happy path forwards id",
			input: &Model{ID: tenantID},
		},
		{
			name:    "repo error propagates",
			input:   &Model{ID: tenantID},
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("SoftDelete", mock.Anything, tenantID).
				Return(tc.repoErr)

			err := s.svc.DisableTenant(s.ctx, tc.input)

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
		input   *Model
		repoErr error
		wantErr bool
	}{
		{
			name:  "happy path forwards id",
			input: &Model{ID: tenantID},
		},
		{
			name:    "repo error propagates",
			input:   &Model{ID: tenantID},
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("DeleteTenantSchema", mock.Anything, tenantID).
				Return(tc.repoErr)

			err := s.svc.PurgeTenant(s.ctx, tc.input)

			if tc.wantErr {
				s.Require().Error(err)
			} else {
				s.Require().NoError(err)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestGetTenantControlPlaneState() {
	tenantID := uuid.New()
	repoErr := errors.New("repo state query failed")

	cases := []struct {
		name          string
		repoState     string
		repoErr       error
		wantControl   string
		wantErr       bool
		wantErrTarget error
	}{
		{name: "created state maps through", repoState: Created, wantControl: Created},
		{name: "up state maps through", repoState: Up, wantControl: Up},
		{name: "down state maps through", repoState: Down, wantControl: Down},
		{name: "disabled state maps through", repoState: Disabled, wantControl: Disabled},
		{name: "purged state maps through", repoState: Purged, wantControl: Purged},
		{
			name:          "unknown state returns ErrUnknownState",
			repoState:     "bogus",
			wantErr:       true,
			wantErrTarget: ErrUnknownState,
		},
		{
			name:    "repo error propagates",
			repoErr: repoErr,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			s.repo.
				On("GetTenantSchemaState", mock.Anything, tenantID).
				Return(tc.repoState, tc.repoErr)

			got, err := s.svc.GetTenantControlPlaneState(s.ctx, &Model{ID: tenantID})

			if tc.wantErr {
				s.Require().Error(err)
				s.Nil(got)
				if tc.wantErrTarget != nil {
					s.ErrorIs(err, tc.wantErrTarget)
				}
			} else {
				s.Require().NoError(err)
				s.Require().NotNil(got)
				s.Equal(tenantID, got.ID)
				s.Equal(tc.wantControl, got.ControlState)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}

func (s *ServiceTestSuite) TestSetTenantControlPlaneState() {
	tenantID := uuid.New()
	repoErr := errors.New("repo branch failed")

	type expectCall struct {
		method string
		id     uuid.UUID
		err    error
	}

	cases := []struct {
		name          string
		input         *Model
		expect        expectCall
		wantErr       bool
		wantErrTarget error
	}{
		{
			name:    "nil model returns error",
			input:   nil,
			wantErr: true,
		},
		{
			name:   "disabled calls SoftDelete happy",
			input:  &Model{ID: tenantID, ControlState: Disabled},
			expect: expectCall{method: "SoftDelete", id: tenantID},
		},
		{
			name:    "disabled propagates SoftDelete error",
			input:   &Model{ID: tenantID, ControlState: Disabled},
			expect:  expectCall{method: "SoftDelete", id: tenantID, err: repoErr},
			wantErr: true,
		},
		{
			name:   "purged calls DeleteTenantSchema happy",
			input:  &Model{ID: tenantID, ControlState: Purged},
			expect: expectCall{method: "DeleteTenantSchema", id: tenantID},
		},
		{
			name:    "purged propagates DeleteTenantSchema error",
			input:   &Model{ID: tenantID, ControlState: Purged},
			expect:  expectCall{method: "DeleteTenantSchema", id: tenantID, err: repoErr},
			wantErr: true,
		},
		{
			name:          "unknown control state returns ErrUnknownState",
			input:         &Model{ID: tenantID, ControlState: "bogus"},
			wantErr:       true,
			wantErrTarget: ErrUnknownState,
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			s.resetMock()
			switch tc.expect.method {
			case "SoftDelete":
				s.repo.On("SoftDelete", mock.Anything, tc.expect.id).Return(tc.expect.err)
			case "DeleteTenantSchema":
				s.repo.On("DeleteTenantSchema", mock.Anything, tc.expect.id).Return(tc.expect.err)
			case "":
			}

			err := s.svc.SetTenantControlPlaneState(s.ctx, tc.input)

			if tc.wantErr {
				s.Require().Error(err)
				if tc.wantErrTarget != nil {
					s.ErrorIs(err, tc.wantErrTarget)
				}
			} else {
				s.Require().NoError(err)
			}
			s.repo.AssertExpectations(s.T())
		})
	}
}
