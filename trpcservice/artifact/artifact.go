package artifact

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

var (
	// ErrNotFound reports that no accessible artifact metadata exists.
	ErrNotFound = errors.New("artifact metadata not found")
	// ErrCleanupLeaseLost reports that another worker owns an artifact cleanup.
	ErrCleanupLeaseLost = errors.New("artifact cleanup lease lost")
)

// Record is the SQL-authoritative metadata for one session-scoped artifact
// version. ObjectKey identifies storage only and is never an authorization
// input from an external caller.
type Record struct {
	ID                 string
	TenantID           string
	AppID              string
	SessionPrincipalID string
	SessionID          string
	Filename           string
	Version            int
	ObjectKey          string
	MIMEType           string
	Size               int64
	Status             Status
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Status identifies the lifecycle state of an artifact record.
type Status string

const (
	// StatusPending reserves an immutable object version before its storage
	// upload completes. Pending records never authorize reads.
	StatusPending Status = "PENDING"
	// StatusAvailable permits a trusted session-scoped read.
	StatusAvailable Status = "AVAILABLE"
	// StatusDeleted prevents subsequent reads while retaining the audit record.
	StatusDeleted Status = "DELETED"
)

// CleanupStatus identifies the durable retry lifecycle for an unreachable
// object. Cleanup records never grant artifact read access.
type CleanupStatus string

const (
	// CleanupPending is ready for a worker to delete the object.
	CleanupPending CleanupStatus = "PENDING"
	// CleanupRunning is leased by one worker.
	CleanupRunning CleanupStatus = "RUNNING"
	// CleanupSucceeded retained the completed cleanup audit record.
	CleanupSucceeded CleanupStatus = "SUCCEEDED"
	// CleanupFailed requires manual reconciliation and is never claimable.
	CleanupFailed CleanupStatus = "FAILED"
)

// CleanupRecord identifies one artifact object cleanup operation. The
// immutable config version retains the backend route after later config
// versions become active.
type CleanupRecord struct {
	ID                 string
	TenantID           string
	AppID              string
	ConfigVersion      string
	SessionPrincipalID string
	SessionID          string
	Filename           string
	ObjectKey          string
	// Version is the exact immutable object version selected for cleanup.
	Version       int
	Status        CleanupStatus
	Attempt       int
	NextAttemptAt time.Time
	LeaseOwner    string
	LeaseUntil    time.Time
	RunToken      string
	LastError     string
}

// Access binds an artifact service to one trusted execution session.
type Access struct {
	Scope              tenant.Scope
	ConfigVersion      string
	SessionPrincipalID string
	SessionID          string
}

// Validate checks that Access can bind a framework artifact request to one
// tenant application session.
func (a Access) Validate() error {
	if err := a.Scope.Validate(); err != nil {
		return err
	}
	if a.ConfigVersion == "" || a.SessionPrincipalID == "" || a.SessionID == "" {
		return errors.New("artifact access config and session are required")
	}
	return nil
}

// Validate checks the identity and retry fields of CleanupRecord.
func (r CleanupRecord) Validate() error {
	if r.ID == "" || r.TenantID == "" || r.AppID == "" || r.ConfigVersion == "" ||
		r.SessionPrincipalID == "" || r.SessionID == "" || strings.TrimSpace(r.Filename) == "" {
		return errors.New("artifact cleanup identity is required")
	}
	if r.Version < -1 {
		return errors.New("artifact cleanup version is invalid")
	}
	if r.Status != CleanupFailed && (r.ObjectKey == "" || r.Version < 0) {
		return errors.New("artifact cleanup requires an exact object version")
	}
	if r.Attempt < 0 {
		return errors.New("artifact cleanup attempt must not be negative")
	}
	if r.Status != CleanupPending && r.Status != CleanupRunning &&
		r.Status != CleanupSucceeded && r.Status != CleanupFailed {
		return errors.New("artifact cleanup status is invalid")
	}
	if r.LeaseOwner == "" && (!r.LeaseUntil.IsZero() || r.RunToken != "") {
		return errors.New("artifact cleanup lease owner is required")
	}
	if r.LeaseOwner != "" && (r.LeaseUntil.IsZero() || r.RunToken == "") {
		return errors.New("artifact cleanup lease is incomplete")
	}
	return nil
}

// MetadataStore persists and authorizes session-scoped artifact metadata.
type MetadataStore interface {
	ReserveArtifact(context.Context, Access, string, string, int64) (Record, error)
	BindArtifactObject(context.Context, Record, string) error
	PublishArtifact(context.Context, Record) error
	AbandonArtifact(context.Context, Record) error
	EnqueueArtifactCleanup(context.Context, CleanupRecord) error
	FindArtifact(context.Context, Access, string, *int) (Record, error)
	ListArtifactKeys(context.Context, Access) ([]string, error)
	ListArtifactVersions(context.Context, Access, string) ([]int, error)
	MarkArtifactsDeleted(context.Context, Access, string) ([]Record, error)
}

// VersionedStorage can remove exactly one immutable artifact version. Platform
// production storage must implement it so failed metadata writes cannot delete
// older available versions of the same filename.
type VersionedStorage interface {
	frameworkartifact.Service
	SaveArtifactVersion(context.Context, frameworkartifact.SessionInfo, string, int, *frameworkartifact.Artifact) error
	DeleteArtifactVersion(context.Context, frameworkartifact.SessionInfo, string, int) error
}

// StorageResolver resolves the configured backing artifact service for one
// execution. It owns the backing service lifecycle.
type StorageResolver interface {
	ResolveArtifact(context.Context, worker.Execution) (frameworkartifact.Service, error)
}

// ExecutionResolver combines a configured backing service with authoritative
// SQL metadata and the trusted execution session scope.
type ExecutionResolver struct {
	storage  StorageResolver
	metadata MetadataStore
}

// NewExecutionResolver creates a resolver for SQL-guarded artifact services.
func NewExecutionResolver(storage StorageResolver, metadata MetadataStore) (*ExecutionResolver, error) {
	if storage == nil {
		return nil, errors.New("artifact storage resolver is required")
	}
	if metadata == nil {
		return nil, errors.New("artifact metadata store is required")
	}
	return &ExecutionResolver{storage: storage, metadata: metadata}, nil
}

// ResolveArtifact returns a service bound to exec's trusted session. It
// returns nil when the immutable app configuration has no artifact backend.
func (r *ExecutionResolver) ResolveArtifact(ctx context.Context, exec worker.Execution) (frameworkartifact.Service, error) {
	if r == nil || r.storage == nil || r.metadata == nil {
		return nil, errors.New("artifact execution resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return nil, err
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return nil, nil
	}
	storage, err := r.storage.ResolveArtifact(ctx, exec)
	if err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, errors.New("configured artifact storage service is required")
	}
	return NewService(storage, r.metadata, Access{
		Scope:              exec.Tenant.Scope(),
		ConfigVersion:      exec.Tenant.ConfigVersion,
		SessionPrincipalID: exec.Tenant.SessionPrincipalID,
		SessionID:          exec.Tenant.SessionID,
	})
}

// DeleteCleanup removes the backing object selected by record without
// consulting artifact metadata. It is reserved for the durable cleanup path
// after reads have already been denied or metadata creation failed.
func (r *ExecutionResolver) DeleteCleanup(
	ctx context.Context,
	exec worker.Execution,
	record CleanupRecord,
) error {
	if r == nil || r.storage == nil || r.metadata == nil {
		return errors.New("artifact execution resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.TenantID != exec.Tenant.TenantID || record.AppID != exec.Tenant.AppID ||
		record.ConfigVersion != exec.Tenant.ConfigVersion ||
		record.SessionPrincipalID != exec.Tenant.SessionPrincipalID || record.SessionID != exec.Tenant.SessionID {
		return errors.New("artifact cleanup record does not match execution")
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return errors.New("artifact backend is not configured")
	}
	storage, err := r.storage.ResolveArtifact(ctx, exec)
	if err != nil {
		return err
	}
	if storage == nil {
		return errors.New("configured artifact storage service is required")
	}
	service, err := NewService(storage, r.metadata, Access{
		Scope:              exec.Tenant.Scope(),
		ConfigVersion:      exec.Tenant.ConfigVersion,
		SessionPrincipalID: exec.Tenant.SessionPrincipalID,
		SessionID:          exec.Tenant.SessionID,
	})
	if err != nil {
		return err
	}
	info, storageFilename, err := service.storageRequest(record.Filename)
	if err != nil {
		return err
	}
	expectedKey, err := objectKey(storage, info, storageFilename, record.Version)
	if err != nil {
		return err
	}
	if record.ObjectKey != expectedKey {
		return errors.New("artifact cleanup object key does not match record")
	}
	if err := service.deleteArtifactVersion(ctx, info, storageFilename, record.Version); err != nil {
		return fmt.Errorf("delete artifact version: %w", err)
	}
	return nil
}

// Service guards a framework Artifact service with SQL-authoritative metadata
// and one trusted execution session.
type Service struct {
	storage  frameworkartifact.Service
	metadata MetadataStore
	access   Access
}

// NewService creates an artifact service scoped to one trusted execution.
func NewService(storage frameworkartifact.Service, metadata MetadataStore, access Access) (*Service, error) {
	if storage == nil {
		return nil, errors.New("artifact storage service is required")
	}
	if metadata == nil {
		return nil, errors.New("artifact metadata store is required")
	}
	if err := access.Validate(); err != nil {
		return nil, err
	}
	return &Service{storage: storage, metadata: metadata, access: access}, nil
}

// SaveArtifact writes one trusted session artifact and records its metadata.
// The underlying object remains inaccessible through this service until its
// SQL record has been written successfully.
func (s *Service) SaveArtifact(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	value *frameworkartifact.Artifact,
) (int, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return 0, err
	}
	if value == nil {
		return 0, errors.New("artifact is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return 0, err
	}
	record, err := s.metadata.ReserveArtifact(ctx, s.access, filename, value.MimeType, int64(len(value.Data)))
	if err != nil {
		return 0, err
	}
	objectKey, err := objectKey(s.storage, storageInfo, storageFilename, record.Version)
	if err != nil {
		return 0, s.abandonReservedArtifact(ctx, record, err)
	}
	if err := s.metadata.BindArtifactObject(ctx, record, objectKey); err != nil {
		return 0, s.abandonReservedArtifact(ctx, record, err)
	}
	record.ObjectKey = objectKey
	if err := s.saveArtifactVersion(ctx, storageInfo, storageFilename, record.Version, value); err != nil {
		return 0, s.compensateSave(ctx, storageInfo, storageFilename, record, err)
	}
	if err := s.metadata.PublishArtifact(ctx, record); err != nil {
		return 0, s.compensateSave(ctx, storageInfo, storageFilename, record, err)
	}
	return record.Version, nil
}

// LoadArtifact authorizes the requested session artifact before loading its
// exact recorded version from the backing storage service.
func (s *Service) LoadArtifact(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version *int,
) (*frameworkartifact.Artifact, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record, err := s.metadata.FindArtifact(ctx, s.access, filename, version)
	if err != nil {
		return nil, err
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return nil, err
	}
	resolvedVersion := record.Version
	return s.storage.LoadArtifact(ctx, storageInfo, storageFilename, &resolvedVersion)
}

// ListArtifactKeys returns only filenames with available SQL metadata in the
// trusted session scope.
func (s *Service) ListArtifactKeys(ctx context.Context, info frameworkartifact.SessionInfo) ([]string, error) {
	if err := s.validateRequest(info, "listed"); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.metadata.ListArtifactKeys(ctx, s.access)
}

// DeleteArtifact removes all storage versions for one trusted session filename
// and prevents subsequent reads by marking its metadata deleted.
func (s *Service) DeleteArtifact(ctx context.Context, info frameworkartifact.SessionInfo, filename string) error {
	if err := s.validateRequest(info, filename); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	records, err := s.metadata.MarkArtifactsDeleted(ctx, s.access, filename)
	if err != nil {
		return err
	}
	storageInfo, storageFilename, err := s.storageRequest(filename)
	if err != nil {
		return err
	}
	var failures []error
	for _, record := range records {
		if err := s.deleteArtifactVersion(ctx, storageInfo, storageFilename, record.Version); err != nil {
			failures = append(failures, s.enqueueVersionCleanup(ctx, record, fmt.Errorf("delete artifact storage: %w", err)))
		}
	}
	return errors.Join(failures...)
}

func (s *Service) compensateSave(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	record Record,
	metadataErr error,
) error {
	metadataFailure := fmt.Errorf("record artifact metadata: %w", metadataErr)
	if err := s.metadata.AbandonArtifact(context.WithoutCancel(ctx), record); err != nil {
		metadataFailure = errors.Join(metadataFailure, fmt.Errorf("abandon artifact metadata: %w", err))
	}
	if err := s.deleteArtifactVersion(ctx, info, filename, record.Version); err == nil {
		return metadataFailure
	} else {
		return s.enqueueVersionCleanup(ctx, record, errors.Join(
			metadataFailure,
			fmt.Errorf("compensate artifact storage: %w", err),
		))
	}
}

func (s *Service) enqueueVersionCleanup(ctx context.Context, record Record, cause error) error {
	task := CleanupRecord{
		ID:                 uuid.NewString(),
		TenantID:           s.access.Scope.TenantID,
		AppID:              s.access.Scope.AppID,
		ConfigVersion:      s.access.ConfigVersion,
		SessionPrincipalID: s.access.SessionPrincipalID,
		SessionID:          s.access.SessionID,
		Filename:           record.Filename,
		ObjectKey:          record.ObjectKey,
		Version:            record.Version,
		Status:             CleanupPending,
		LastError:          platformlog.SafeError(cause),
	}
	if err := s.metadata.EnqueueArtifactCleanup(context.WithoutCancel(ctx), task); err != nil {
		return errors.Join(cause, fmt.Errorf("record artifact cleanup: %w", err))
	}
	return cause
}

func (s *Service) abandonReservedArtifact(ctx context.Context, record Record, cause error) error {
	if err := s.metadata.AbandonArtifact(context.WithoutCancel(ctx), record); err != nil {
		return errors.Join(cause, fmt.Errorf("abandon artifact reservation: %w", err))
	}
	return cause
}

func (s *Service) saveArtifactVersion(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version int,
	value *frameworkartifact.Artifact,
) error {
	storage, ok := s.storage.(VersionedStorage)
	if !ok {
		return errors.New("artifact storage does not support reserved versions")
	}
	return storage.SaveArtifactVersion(ctx, info, filename, version, value)
}

func (s *Service) deleteArtifactVersion(
	ctx context.Context,
	info frameworkartifact.SessionInfo,
	filename string,
	version int,
) error {
	storage, ok := s.storage.(VersionedStorage)
	if !ok {
		return errors.New("artifact storage does not support versioned deletion")
	}
	return storage.DeleteArtifactVersion(ctx, info, filename, version)
}

// ListVersions returns only versions with available SQL metadata in the
// trusted session scope.
func (s *Service) ListVersions(ctx context.Context, info frameworkartifact.SessionInfo, filename string) ([]int, error) {
	if err := s.validateRequest(info, filename); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	versions, err := s.metadata.ListArtifactVersions(ctx, s.access, filename)
	if err != nil {
		return nil, err
	}
	sort.Ints(versions)
	return versions, nil
}

func (s *Service) validateRequest(info frameworkartifact.SessionInfo, filename string) error {
	if s == nil || s.storage == nil || s.metadata == nil {
		return errors.New("artifact service is not initialized")
	}
	if err := s.access.Validate(); err != nil {
		return err
	}
	expectedAppName, err := s.artifactAppName()
	if err != nil {
		return err
	}
	if info.AppName != expectedAppName || info.UserID != s.access.SessionPrincipalID ||
		info.SessionID != s.access.SessionID {
		return errors.New("artifact session does not match execution scope")
	}
	if filename != "listed" && strings.TrimSpace(filename) == "" {
		return errors.New("artifact filename is required")
	}
	if strings.HasPrefix(filename, "user:") {
		return errors.New("user-scoped artifacts are not supported")
	}
	return nil
}

func (s *Service) storageSessionInfo() (frameworkartifact.SessionInfo, error) {
	appName, err := s.artifactAppName()
	if err != nil {
		return frameworkartifact.SessionInfo{}, err
	}
	return frameworkartifact.SessionInfo{
		AppName:   appName,
		UserID:    storageSegment(s.access.SessionPrincipalID),
		SessionID: storageSegment(s.access.SessionID),
	}, nil
}

func (s *Service) storageRequest(filename string) (frameworkartifact.SessionInfo, string, error) {
	info, err := s.storageSessionInfo()
	if err != nil {
		return frameworkartifact.SessionInfo{}, "", err
	}
	return info, storageSegment(filename), nil
}

func (s *Service) artifactAppName() (string, error) {
	return s.access.Scope.Key("runner")
}

func storageSegment(value string) string {
	return "artifact-v1-" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

type objectKeyService interface {
	ObjectKey(frameworkartifact.SessionInfo, string, int) (string, error)
}

func objectKey(storage frameworkartifact.Service, info frameworkartifact.SessionInfo, filename string, version int) (string, error) {
	keyer, ok := storage.(objectKeyService)
	if !ok {
		return "", errors.New("artifact storage does not expose object keys")
	}
	return keyer.ObjectKey(info, filename, version)
}

var _ frameworkartifact.Service = (*Service)(nil)
