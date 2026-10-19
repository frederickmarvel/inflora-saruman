package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/frederickmarvel/inflora-saruman/internal/config"
	"github.com/frederickmarvel/inflora-saruman/internal/handler"
	"github.com/frederickmarvel/inflora-saruman/internal/service"
	"github.com/frederickmarvel/inflora-saruman/internal/subscriber"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/outbox"
	infloraruntime "github.com/frederickmarvel/inflora-shared/runtime"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", cfg.DBDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)

	natsCfg := events.NATSConfig{URL: cfg.NATSURL, Name: cfg.ServiceName, Stream: cfg.NATSStream, Subjects: []string{">"}, AckWait: cfg.AckWait, MaxDeliver: cfg.MaxDeliver}
	publisher, err := events.NewNATSPublisher(natsCfg)
	if err != nil {
		return err
	}
	defer publisher.Close()
	natsSubscriber, err := events.NewNATSSubscriber(natsCfg)
	if err != nil {
		return err
	}
	defer natsSubscriber.Close()
	dispatcher, err := outbox.NewDispatcher(db, publisher, outbox.DispatcherConfig{Producer: "saruman", WorkerName: "saruman-outbox"})
	if err != nil {
		return err
	}

	internalKey := os.Getenv("INTERNAL_API_KEY")
	if internalKey == "" {
		log.Fatal("INTERNAL_API_KEY is required")
	}
	mux := http.NewServeMux()
	mux.Handle("/", handler.HealthMux(db))
	review := &handler.ReviewHandler{DB: db, InternalKey: internalKey}
	mux.HandleFunc("POST /v1/internal/donations/{id}/review", review.Review)
	payoutCommands := &handler.PayoutCommandHandler{DB: db, InternalKey: internalKey, Lease: cfg.PayoutLease}
	mux.HandleFunc("POST /v1/internal/payout-commands", payoutCommands.Handle)

	httpWorker := &handler.Server{HTTP: &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
	eventWorker := &subscriber.Worker{Subscriber: natsSubscriber, Processor: &subscriber.Processor{DB: db}}
	reviewReconciler := &service.ReviewReconciler{DB: db}
	sweeper := &service.Sweeper{DB: db, Interval: cfg.PayoutSweepInterval, LeaseWindow: cfg.PayoutLease}
	probe := func(probeCtx context.Context) (bool, error) {
		err := db.PingContext(probeCtx)
		return err == nil, err
	}
	return infloraruntime.Run(ctx, 15*time.Second, 10*time.Second, probe, httpWorker, eventWorker, reviewReconciler, sweeper, dispatcher)
}
