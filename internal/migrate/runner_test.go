package migrate

import (
	"context"
	"database/sql"
	"testing"

	"github.com/frederickmarvel/inflora-saruman/migrations"
)

func TestLoadEmbeddedMigrations(t *testing.T) {
	t.Parallel()

	got, err := load(migrations.Files)
	if err != nil {
		t.Fatalf("load migrations: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("migration count = %d, want 6", len(got))
	}
	if got[0].Version != 1 || got[0].Name != "init" {
		t.Fatalf("first migration = %#v", got[0])
	}
	if got[1].Version != 2 || got[1].Name != "seed_dev" {
		t.Fatalf("second migration = %#v", got[1])
	}
	if got[2].Version != 3 || got[2].Name != "reliability" {
		t.Fatalf("third migration = %#v", got[2])
	}
	if got[3].Version != 4 || got[3].Name != "palantir_topup_idempotency" {
		t.Fatalf("fourth migration = %#v", got[3])
	}
	if got[4].Version != 5 || got[4].Name != "runtime_reliability" {
		t.Fatalf("fifth migration = %#v", got[4])
	}
	if got[5].Version != 6 || got[5].Name != "donation_display_pricing_snapshot" {
		t.Fatalf("sixth migration = %#v", got[5])
	}
	for _, migration := range got {
		if migration.Up == "" || migration.Down == "" || len(migration.Checksum) != 64 {
			t.Fatalf("incomplete migration: %#v", migration)
		}
	}
}

func TestMigrationDirectionConstants(t *testing.T) {
	t.Parallel()

	if Up != "up" || Down != "down" || Status != "status" {
		t.Fatalf("unexpected directions: %q %q %q", Up, Down, Status)
	}
}

func TestRunnerRejectsNilDatabase(t *testing.T) {
	t.Parallel()

	err := (Runner{DB: (*sql.DB)(nil), FS: migrations.Files}).Run(context.Background(), Up, 0)
	if err == nil || err.Error() != "migrate: nil database" {
		t.Fatalf("Run error = %v", err)
	}
}
