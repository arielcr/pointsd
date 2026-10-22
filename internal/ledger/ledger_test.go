package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
)

const cap10k = 10_000

var ctx = context.Background()

func award(paymentID, memberID, month string, points int64) ledger.Entry {
	return ledger.Entry{MemberID: memberID, PaymentID: paymentID, Points: points, Month: month}
}

func mustAward(t *testing.T, s *ledger.MemoryStore, e ledger.Entry) ledger.Entry {
	t.Helper()
	got, _, err := s.Award(ctx, e, cap10k)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustBalance(t *testing.T, s *ledger.MemoryStore, member string) int64 {
	t.Helper()
	b, err := s.Balance(ctx, member)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// parallel runs fn(0..n-1) at the same time and waits for all of them.
func parallel(n int, fn func(i int)) {
	var start, done sync.WaitGroup
	start.Add(1)
	for i := 0; i < n; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			fn(i)
		}()
	}
	start.Done()
	done.Wait()
}

func TestAward_CutsPointsOnTheWrittenEntry(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", "2026-10", 9500))

	got := mustAward(t, s, award("pay_2", "mbr_1", "2026-10", 1000))
	if got.Points != 500 || got.Kind != ledger.KindAward {
		t.Fatalf("entry = %+v, want an award of 500", got)
	}
	if got := mustAward(t, s, award("pay_3", "mbr_1", "2026-10", 1000)); got.Points != 0 {
		t.Fatalf("points after cap = %d, want 0", got.Points)
	}
	if got := mustAward(t, s, award("pay_4", "mbr_1", "2026-11", 1000)); got.Points != 1000 {
		t.Fatalf("points in a new month = %d, want 1000", got.Points)
	}
	if got := mustAward(t, s, award("pay_5", "mbr_2", "2026-10", 1000)); got.Points != 1000 {
		t.Fatalf("points for another member = %d, want 1000", got.Points)
	}
}

func TestAward_IsIdempotentPerPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	first, created, err := s.Award(ctx, award("pay_1", "mbr_1", "2026-10", 1800), cap10k)
	if err != nil || !created {
		t.Fatalf("first award: created=%v err=%v", created, err)
	}
	// A republished payment may carry different data; the first award wins.
	again, created, err := s.Award(ctx, award("pay_1", "mbr_1", "2026-11", 9999), cap10k)
	if err != nil || created {
		t.Fatalf("second award: created=%v err=%v, want created=false", created, err)
	}
	if again != first {
		t.Fatalf("second award returned %+v, want the stored %+v", again, first)
	}
	if got := mustBalance(t, s, "mbr_1"); got != 1800 {
		t.Fatalf("balance = %d, want 1800", got)
	}
}

func TestAward_RecognizesSeededEntries(t *testing.T) {
	s := ledger.NewMemoryStore()
	if err := s.Append(ctx, ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindAward, Points: 750, Month: "2026-10"}); err != nil {
		t.Fatal(err)
	}
	if _, created, _ := s.Award(ctx, award("pay_1", "mbr_1", "2026-10", 750), cap10k); created {
		t.Fatal("award written twice for a payment already in the ledger")
	}
	if got := mustAward(t, s, award("pay_2", "mbr_1", "2026-10", 9500)); got.Points != 9250 {
		t.Fatalf("points = %d, want 9250 (seeded award counts toward the cap)", got.Points)
	}
}

func TestAward_ConcurrentSamePaymentIsWrittenOnce(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	var created atomic.Int32
	parallel(30, func(int) {
		if _, ok, err := s.Award(ctx, award("pay_1", "mbr_1", "2026-10", 2000), cap10k); err == nil && ok {
			created.Add(1)
		}
	})
	if created.Load() != 1 {
		t.Fatalf("created %d awards, want 1", created.Load())
	}
	if got := mustBalance(t, s, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000", got)
	}
}

func TestAward_ConcurrentPaymentsNeverExceedCap(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	parallel(30, func(i int) {
		_, _, _ = s.Award(ctx, award(fmt.Sprintf("pay_%d", i), "mbr_1", "2026-10", 777), cap10k)
	})
	if got := mustBalance(t, s, "mbr_1"); got != cap10k {
		t.Fatalf("balance = %d, want %d", got, cap10k)
	}
}

func TestAward_ReversalsDoNotGiveCapRoomBack(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", "2026-10", cap10k))
	if _, _, err := s.Reverse(ctx, "pay_1"); err != nil {
		t.Fatal(err)
	}
	if got := mustAward(t, s, award("pay_2", "mbr_1", "2026-10", 500)); got.Points != 0 {
		t.Fatalf("points = %d, want 0 (reversal must not free cap room)", got.Points)
	}
}

func TestReverse_NegatesTheAwardedEntry(t *testing.T) {
	s := ledger.NewMemoryStore()
	mustAward(t, s, award("pay_1", "mbr_1", "2026-10", 9500))
	awarded := mustAward(t, s, award("pay_2", "mbr_1", "2026-10", 1000)) // cut to 500

	rev, created, err := s.Reverse(ctx, "pay_2")
	if err != nil || !created {
		t.Fatalf("reverse: created=%v err=%v", created, err)
	}
	want := ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_2", Kind: ledger.KindReversal, Points: -awarded.Points, Month: "2026-10"}
	if rev != want {
		t.Fatalf("reversal = %+v, want %+v", rev, want)
	}
	if got := mustBalance(t, s, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500", got)
	}
}

func TestReverse_IsIdempotentUnderConcurrency(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	mustAward(t, s, award("pay_1", "mbr_1", "2026-10", 2000))
	var created atomic.Int32
	parallel(30, func(int) {
		if _, ok, err := s.Reverse(ctx, "pay_1"); err == nil && ok {
			created.Add(1)
		}
	})
	if created.Load() != 1 {
		t.Fatalf("created %d reversals, want 1", created.Load())
	}
	if got := mustBalance(t, s, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestReverse_UnknownPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Reverse(ctx, "pay_never_settled"); !errors.Is(err, ledger.ErrNotAwarded) {
		t.Fatalf("err = %v, want ErrNotAwarded", err)
	}
}

func TestFaults_LeaveTheLedgerUnchanged(t *testing.T) {
	var down atomic.Bool
	s := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if down.Load() && op != "balance" {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	mustAward(t, s, award("pay_1", "mbr_1", "2026-10", 1000))

	down.Store(true)
	if _, _, err := s.Award(ctx, award("pay_2", "mbr_1", "2026-10", 1000), cap10k); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("award err = %v, want ErrUnavailable", err)
	}
	if _, _, err := s.Reverse(ctx, "pay_1"); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("reverse err = %v, want ErrUnavailable", err)
	}
	if got := mustBalance(t, s, "mbr_1"); got != 1000 {
		t.Fatalf("balance = %d, want 1000 (failed calls must have no effect)", got)
	}
}
