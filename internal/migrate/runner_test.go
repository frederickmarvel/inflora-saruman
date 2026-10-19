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
	if len(got) != 15 {
		t.Fatalf("migration count = %d, want 15", len(got))
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
	if got[6].Version != 7 || got[6].Name != "admin" {
		t.Fatalf("seventh migration = %#v", got[6])
	}
	if got[7].Version != 8 || got[7].Name != "seed_admin" {
		t.Fatalf("eighth migration = %#v", got[7])
	}
	if got[8].Version != 9 || got[8].Name != "streamer_handle_live_sessions" {
		t.Fatalf("ninth migration = %#v", got[8])
	}
	if got[9].Version != 10 || got[9].Name != "donation_rich_intent" {
		t.Fatalf("tenth migration = %#v", got[9])
	}
	if got[10].Version != 11 || got[10].Name != "donation_review" {
		t.Fatalf("eleventh migration = %#v", got[10])
	}
	if got[11].Version != 12 || got[11].Name != "ledger_source_tag" {
		t.Fatalf("twelfth migration = %#v", got[11])
	}
	if got[12].Version != 13 || got[12].Name != "supporter_identity" {
		t.Fatalf("thirteenth migration = %#v", got[12])
	}
	if got[13].Version != 14 || got[13].Name != "payout_withdrawals" {
		t.Fatalf("fourteenth migration = %#v", got[13])
	}
	if got[14].Version != 16 || got[14].Name != "seed_payout_dev" {
		t.Fatalf("fifteenth migration = %#v", got[14])
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
