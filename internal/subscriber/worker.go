package subscriber

import (
	"context"
	"errors"

	"github.com/frederickmarvel/inflora-shared/events"
)

type Worker struct {
	Subscriber *events.NATSSubscriber
	Processor  *Processor
}

func (w *Worker) Name() string { return "saruman-event-consumers" }

func (w *Worker) Run(ctx context.Context) error {
	if w.Subscriber == nil || w.Processor == nil {
		return errors.New("subscriber: worker dependencies are required")
	}
	type subscription struct{ subject, durable string }
	subs := []subscription{
		{IntentCreatedSubject, "saruman-donation-intent-created"},
		{TopupCompletedType + ".>", "saruman-topup-completed"},
		{TopupFailedType + ".>", "saruman-topup-failed"},
		{TopupExpiredType + ".>", "saruman-topup-expired"},
		// Phase C payout-family closures (D-052/D-055): the settled event is
		// the dev payment-settlement closure, the withdrawal pair closes
		// PROCESSING payouts.
		{TopupSettledType + ".>", "saruman-topup-settled"},
		{WithdrawalCompletedType + ".>", "saruman-withdrawal-completed"},
		{WithdrawalFailedType + ".>", "saruman-withdrawal-failed"},
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, len(subs))
	for _, sub := range subs {
		sub := sub
		go func() { errs <- w.Subscriber.Subscribe(runCtx, sub.subject, sub.durable, w.Processor.Handle) }()
	}
	select {
	case <-ctx.Done():
		cancel()
		for range subs {
			<-errs
		}
		return ctx.Err()
	case err := <-errs:
		cancel()
		for i := 1; i < len(subs); i++ {
			<-errs
		}
		return err
	}
}
