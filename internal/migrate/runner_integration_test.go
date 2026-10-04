package migrate_test

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/migrate"
	"github.com/frederickmarvel/inflora-saruman/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresMigrationLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err = database.ExecContext(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	runner := migrate.Runner{DB: database, FS: migrations.Files, Environment: "test", AllowDown: true, LockTimeout: 5 * time.Second}

	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors <- runner.Run(ctx, migrate.Up, 1)
		}()
	}
	wg.Wait()
	close(errors)
	for runErr := range errors {
		if runErr != nil {
			t.Fatalf("concurrent up: %v", runErr)
		}
	}

	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations`, 1)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`, 21)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_type WHERE typname IN ('ledger_direction','ledger_entry_type','ledger_account_type','donation_status','settlement_status','payment_method','payout_status','payout_batch_status','token_purpose','fraud_rule','fraud_action','fund_hold_status','fund_hold_target','email_receipt_status')`, 14)

	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("seed up: %v", err)
	}
	if err = runner.Run(ctx, migrate.Up, 0); err != nil {
		t.Fatalf("idempotent seed up: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM streamers`, 5)
	assertCount(t, database, `SELECT COUNT(*) FROM donations`, 2)

	if err = runner.Run(ctx, migrate.Down, 0); err != nil {
		t.Fatalf("down: %v", err)
	}
	assertCount(t, database, `SELECT COUNT(*) FROM schema_migrations`, 0)
	assertCount(t, database, `SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename <> 'schema_migrations'`, 0)
}

func assertCount(t *testing.T, database *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := database.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query count: %v", err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d for %s", got, want, query)
	}
}
