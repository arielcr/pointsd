// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrNotAwarded is returned by Reverse when the payment has no award entry.
var ErrNotAwarded = errors.New("ledger: payment has no award")

// Store is shared by every pointsd replica. Award and Reverse are each a single
// atomic operation (one transaction in Postgres), so the dedupe and cap checks
// cannot be interleaved by another replica between the read and the write.
type Store interface {
	// Award writes the award entry for e.PaymentID, cutting e.Points so the
	// member's awards in e.Month never exceed monthlyCap. If the payment already
	// has an award, nothing is written and the stored entry is returned with
	// created == false.
	Award(ctx context.Context, e Entry, monthlyCap int64) (stored Entry, created bool, err error)
	// Reverse writes a reversal that negates exactly the points awarded to
	// paymentID, in the award's member and month. If the payment was already
	// reversed, nothing is written and the stored reversal is returned with
	// created == false. It returns ErrNotAwarded if the payment has no award.
	Reverse(ctx context.Context, paymentID string) (stored Entry, created bool, err error)
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.find(e.PaymentID, KindAward); ok {
		return prev, false, nil
	}
	// Reversals do not give cap room back, so only awards count.
	var used int64
	for _, x := range s.entries {
		if x.MemberID == e.MemberID && x.Month == e.Month && x.Kind == KindAward {
			used += x.Points
		}
	}
	e.Kind = KindAward
	e.Points = min(e.Points, max(monthlyCap-used, 0))
	s.entries = append(s.entries, e)
	return e, true, nil
}

func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.find(paymentID, KindReversal); ok {
		return prev, false, nil
	}
	award, ok := s.find(paymentID, KindAward)
	if !ok {
		return Entry{}, false, ErrNotAwarded
	}
	rev := award
	rev.Kind = KindReversal
	rev.Points = -award.Points
	s.entries = append(s.entries, rev)
	return rev, true, nil
}

// find returns the entry of the given kind for a payment. s.mu must be held.
func (s *MemoryStore) find(paymentID string, kind Kind) (Entry, bool) {
	for _, e := range s.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return e, true
		}
	}
	return Entry{}, false
}

// Append writes e as is, without the dedupe and cap rules of Award and Reverse.
// It is meant for seeding a ledger, for example in tests.
func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
