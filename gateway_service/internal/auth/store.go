package auth

import (
	"context"
	"strings"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type UserStore interface {
	CreateUser(ctx context.Context, username, password string, role Role) (*User, error)
	Authenticate(ctx context.Context, username, password string) (*User, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (*User, error)
}

type MemoryUserStore struct {
	mu          sync.RWMutex
	usersByID   map[uuid.UUID]*User
	usersByName map[string]*User
}

func NewMemoryUserStore() *MemoryUserStore {
	store := &MemoryUserStore{
		usersByID:   make(map[uuid.UUID]*User),
		usersByName: make(map[string]*User),
	}
	_, _ = store.CreateUser(context.Background(), "demo_customer", "demo_password", RoleCustomer)
	_, _ = store.CreateUser(context.Background(), "demo_operator", "demo_password", RoleWarehouseOperator)
	_, _ = store.CreateUser(context.Background(), "demo_admin", "demo_password", RoleAdmin)
	_, _ = store.CreateUser(context.Background(), "operator", "operatorpass123", RoleWarehouseOperator)
	_, _ = store.CreateUser(context.Background(), "admin", "adminpass123", RoleAdmin)
	return store
}

func (s *MemoryUserStore) CreateUser(ctx context.Context, username, password string, role Role) (*User, error) {
	cleanName := strings.TrimSpace(username)
	if cleanName == "" || len(password) < 4 {
		return nil, ErrInvalidCredentials
	}

	validRole, err := ValidateRole(string(role))
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	lookupName := strings.ToLower(cleanName)
	if _, exists := s.usersByName[lookupName]; exists {
		return nil, ErrUserAlreadyExists
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	user := &User{
		ID:           uuid.New(),
		Username:     cleanName,
		PasswordHash: string(hash),
		Role:         validRole,
	}

	s.usersByID[user.ID] = user
	s.usersByName[lookupName] = user
	return user, nil
}

func (s *MemoryUserStore) Authenticate(ctx context.Context, username, password string) (*User, error) {
	lookupName := strings.ToLower(strings.TrimSpace(username))
	s.mu.RLock()
	user, exists := s.usersByName[lookupName]
	s.mu.RUnlock()

	if !exists {
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return user, nil
}

func (s *MemoryUserStore) GetUserByID(ctx context.Context, id uuid.UUID) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, exists := s.usersByID[id]
	if !exists {
		return nil, ErrUserNotFound
	}
	return user, nil
}
