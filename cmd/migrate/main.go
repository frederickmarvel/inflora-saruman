// Command migrate applies Saruman's embedded PostgreSQL migrations.
package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/migrate"
	"github.com/frederickmarvel/inflora-saruman/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	direction := flag.String("dir", "up", "up, down, or status")
	target := flag.Int64("target", 0, "target version (0 means all)")
	flag.Parse()
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://inflora:dev@localhost:5432/inflora?sslmode=disable"
	}
	db, err := sqlOpen(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close database: %v", closeErr)
		}
	}()
	allowDown, _ := strconv.ParseBool(os.Getenv("ALLOW_DOWN_MIGRATIONS"))
	environment := os.Getenv("ENV")
	if environment == "" {
		environment = "prod"
	}
	err = (migrate.Runner{DB: db, FS: migrations.Files, Environment: environment, AllowDown: allowDown, LockTimeout: 30 * time.Second}).Run(context.Background(), migrate.Direction(*direction), *target)
	if err != nil {
		log.Fatal(err)
	}
}

func sqlOpen(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
