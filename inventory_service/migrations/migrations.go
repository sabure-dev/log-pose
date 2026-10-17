package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var FS embed.FS

const migrationAdvisoryLockID int64 = 84920174

// Apply runs embedded SQL migrations in lexical order using an advisory lock and schema_migrations table.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(FS, ".")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	var filenames []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		filenames = append(filenames, entry.Name())
	}
	slices.Sort(filenames)

	for _, name := range filenames {
		var alreadyApplied bool
		err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)", name).Scan(&alreadyApplied)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if alreadyApplied {
			continue
		}

		sqlBytes, err := FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("execute migration %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}

	return tx.Commit(ctx)
}

// SeedCatalog idempotently inserts demo catalog items when SEED_CATALOG is enabled.
func SeedCatalog(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin seed tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO products (id, name, price)
		VALUES
			('3fa85f64-5717-4562-b3fc-2c963f66afa6', 'Mechanical Keyboard', 1500.50),
			('123e4567-e89b-12d3-a456-426614174000', 'Wireless Mouse', 300.00)
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("seed products: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO inventory_stocks (product_id, available, reserved)
		VALUES
			('3fa85f64-5717-4562-b3fc-2c963f66afa6', 100, 0),
			('123e4567-e89b-12d3-a456-426614174000', 50, 0)
		ON CONFLICT (product_id) DO NOTHING
	`)
	if err != nil {
		return fmt.Errorf("seed inventory_stocks: %w", err)
	}

	return tx.Commit(ctx)
}
