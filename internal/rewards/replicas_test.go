package rewards_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

// Production runs 3 replicas with MaxOutstandingMessages = 10 each.
const replicaCount, maxOutstanding = 3, 10

func newReplicas(store ledger.Store) []*rewards.Handler {
	hs := make([]*rewards.Handler, replicaCount)
	for i := range hs {
		hs[i] = newHandler(store)
	}
	return hs
}

// outage fails every ledger call except balance reads while down is set.
func outage(down *atomic.Bool) ledger.Option {
	return ledger.WithFaults(func(op string) error {
		if down.Load() && op != "balance" {
			return ledger.ErrUnavailable
		}
		return nil
	})
}

func TestHandle_EventDuringOutageIsNackedThenAwarded(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	store := ledger.NewMemoryStore(outage(&down))
	h := newHandler(store)
	ev := settled("pay_1", "mbr_1", 1200)

	msg := newMessage(t, ev)
	h.Handle(context.Background(), msg)
	if msg.outcome(t) != nacked {
		t.Fatal("event was acked while the ledger was down; it would never earn points")
	}

	down.Store(false)
	deliver(t, h, ev)
	if got := balance(t, store, "mbr_1"); got != 1200 {
		t.Fatalf("balance = %d, want 1200", got)
	}
}

func TestHandle_ReversalDuringOutageIsNackedThenApplied(t *testing.T) {
	var down atomic.Bool
	store := ledger.NewMemoryStore(outage(&down))
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 1500))

	down.Store(true)
	msg := newMessage(t, reversed("pay_1", "mbr_1", 1500))
	h.Handle(context.Background(), msg)
	if msg.outcome(t) != nacked {
		t.Fatal("reversal was acked while the ledger was down")
	}

	down.Store(false)
	deliver(t, h, reversed("pay_1", "mbr_1", 1500))
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestHandle_InterruptedProcessingIsNacked(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // e.g. the replica is shutting down

	msg := newMessage(t, settled("pay_1", "mbr_1", 1000))
	newHandler(store).Handle(ctx, msg)

	if msg.outcome(t) != nacked {
		t.Fatal("interrupted event was acked; it would be lost")
	}
}

func TestAward_RepublishedWithNewEventIDsAcrossReplicasIsAwardedOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := newReplicas(store)

	var fns []func()
	for i := 0; i < replicaCount*maxOutstanding; i++ {
		h, ev := replicas[i%replicaCount], settled("pay_1", "mbr_1", 2000)
		ev.EventID = fmt.Sprintf("evt_%d", i)
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000", got)
	}
}

func TestAward_CapHoldsAcrossReplicasWithTransientFaults(t *testing.T) {
	var failures atomic.Int32
	failures.Store(5)
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency), ledger.WithFaults(func(op string) error {
		if op != "balance" && failures.Add(-1) >= 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	replicas := newReplicas(store)

	var fns []func()
	for i := 0; i < 15; i++ {
		h := replicas[i%replicaCount]
		ev, again := settled(fmt.Sprintf("pay_%d", i), "mbr_1", 1000), settled(fmt.Sprintf("pay_%d", i), "mbr_1", 1000)
		other := settled(fmt.Sprintf("pay_other_%d", i), "mbr_2", 100)
		fns = append(fns, func() { deliver(t, h, ev) }, func() { deliver(t, h, again) }, func() { deliver(t, h, other) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != rewards.DefaultPolicy.MonthlyCap {
		t.Fatalf("mbr_1 balance = %d, want %d", got, rewards.DefaultPolicy.MonthlyCap)
	}
	if got := balance(t, store, "mbr_2"); got != 1500 {
		t.Fatalf("mbr_2 balance = %d, want 1500", got)
	}
}

func TestReversal_ConcurrentRedeliveryAcrossReplicasRemovesPointsOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := newReplicas(store)
	deliver(t, replicas[0], settled("pay_1", "mbr_1", 2000))
	deliver(t, replicas[0], settled("pay_2", "mbr_1", 1000))

	var fns []func()
	for i := 0; i < replicaCount*maxOutstanding; i++ {
		h := replicas[i%replicaCount]
		fns = append(fns, func() { deliver(t, h, reversed("pay_1", "mbr_1", 2000)) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 1000 {
		t.Fatalf("balance = %d, want 1000 (reversal applied more than once)", got)
	}
}

func TestReversal_DoesNotGiveCapRoomBack(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 9500))
	deliver(t, h, settled("pay_2", "mbr_1", 1000)) // capped: 500 awarded
	deliver(t, h, reversed("pay_2", "mbr_1", 1000))
	deliver(t, h, settled("pay_3", "mbr_1", 1000)) // October is still full

	if got := balance(t, store, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500", got)
	}
}

func TestReversal_OfPaymentCappedToZeroIsAcked(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 10_000))
	deliver(t, h, settled("pay_2", "mbr_1", 1000)) // capped: 0 awarded
	msg := deliver(t, h, reversed("pay_2", "mbr_1", 1000))

	if msg.attempt != 1 {
		t.Fatalf("reversal acked on attempt %d, want 1", msg.attempt)
	}
	if got := balance(t, store, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}
