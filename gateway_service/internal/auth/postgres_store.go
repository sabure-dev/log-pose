package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type PostgresUserStore struct {
	pool *pgxpool.Pool
}

var _ UserStore = (*PostgresUserStore)(nil)

func NewPostgresUserStore(ctx context.Context, pool *pgxpool.Pool) (*PostgresUserStore, error) {
	s := &PostgresUserStore{pool: pool}
	if err := s.initSchema(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize users schema: %w", err)
	}
	if err := s.seedDemoUsers(ctx); err != nil {
		return nil, fmt.Errorf("failed to seed demo users: %w", err)
	}
	return s, nil
}

func (s *PostgresUserStore) initSchema(ctx context.Context) error {
	schema := `
		CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY,
			username VARCHAR(100) NOT NULL,
			password_hash VARCHAR(255) NOT NULL,
			role VARCHAR(50) NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users (LOWER(username));
	`
	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *PostgresUserStore) seedDemoUsers(ctx context.Context) error {
	demos := []struct {
		username string
		password string
		role     Role
	}{
		{"demo_customer", "demo_password", RoleCustomer},
		{"demo_operator", "demo_password", RoleWarehouseOperator},
		{"demo_admin", "demo_password", RoleAdmin},
		{"operator", "operatorpass123", RoleWarehouseOperator},
		{"admin", "adminpass123", RoleAdmin},
	}

	for _, d := range demos {
		var exists bool
		err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE LOWER(username) = LOWER($1))", d.username).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check demo user %s: %w", d.username, err)
		}
		if !exists {
			if _, err := s.CreateUser(ctx, d.username, d.password, d.role); err != nil && !errors.Is(err, ErrUserAlreadyExists) {
				return fmt.Errorf("failed to seed demo user %s: %w", d.username, err)
			}
		}
	}
	return nil
}

func (s *PostgresUserStore) CreateUser(ctx context.Context, username, password string, role Role) (*User, error) {
	cleanName := strings.TrimSpace(username)
	if cleanName == "" || len(password) < 4 {
		return nil, ErrInvalidCredentials
	}

	validRole, err := ValidateRole(string(role))
	if err != nil {
		return nil, err
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

	query := `
		INSERT INTO users (id, username, password_hash, role, created_at)
		VALUES ($1, $2, $3, $4, NOW())
	`
	_, err = s.pool.Exec(ctx, query, user.ID, user.Username, user.PasswordHash, string(user.Role))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, ErrUserAlreadyExists
		}
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return user, nil
}

func (s *PostgresUserStore) Authenticate(ctx context.Context, username, password string) (*User, error) {
	cleanName := strings.TrimSpace(username)
	if cleanName == "" || password == "" {
		return nil, ErrInvalidCredentials
	}

	query := `
		SELECT id, username, password_hash, role
		FROM users
		WHERE LOWER(username) = LOWER($1)
		LIMIT 1
	`
	var user User
	var roleStr string
	err := s.pool.QueryRow(ctx, query, cleanName).Scan(&user.ID, &user.Username, &user.PasswordHash, &roleStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	user.Role = Role(roleStr)

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return &user, nil
}

func (s *PostgresUserStore) GetUserByID(ctx context.Context, id uuid.UUID) (*User, error) {
	query := `
		SELECT id, username, password_hash, role
		FROM users
		WHERE id = $1
		LIMIT 1
	`
	var user User
	var roleStr string
	err := s.pool.QueryRow(ctx, query, id).Scan(&user.ID, &user.Username, &user.PasswordHash, &roleStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to query user by id: %w", err)
	}

	user.Role = Role(roleStr)
	return &user, nil
}
