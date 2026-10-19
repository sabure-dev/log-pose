package auth

import (
	"errors"
	"strings"

	"github.com/google/uuid"
)

type Role string

const (
	RoleCustomer          Role = "customer"
	RoleWarehouseOperator Role = "warehouse_operator"
	RoleAdmin             Role = "admin"
)

var (
	ErrUserAlreadyExists  = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid username or password")
	ErrInvalidRole        = errors.New("invalid role; must be customer, warehouse_operator, or admin")
	ErrUserNotFound       = errors.New("user not found")
)

func ValidateRole(role string) (Role, error) {
	clean := strings.ToLower(strings.TrimSpace(role))
	switch Role(clean) {
	case RoleCustomer, "":
		return RoleCustomer, nil
	case RoleWarehouseOperator:
		return RoleWarehouseOperator, nil
	case RoleAdmin:
		return RoleAdmin, nil
	default:
		return "", ErrInvalidRole
	}
}

type User struct {
	ID           uuid.UUID `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	Role         Role      `json:"role"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Role     string `json:"role,omitempty"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AuthResponse struct {
	Token    string    `json:"token"`
	UserID   uuid.UUID `json:"user_id"`
	Username string    `json:"username"`
	Role     Role      `json:"role"`
}
