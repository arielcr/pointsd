# Notes

In Postgres I'd keep the same two operations from `ledger.Store`. `ledger_entries` gets a unique index on `(payment_id, kind)` and a regular index on `(member_id, month)`. `Award` runs in one transaction, read committed is enough:
```sql
SELECT pg_advisory_xact_lock(hashtextextended($member || '/' || $month, 0));
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month) SELECT $member, $payment, 'award', LEAST($points, GREATEST($cap - COALESCE(SUM(points), 0), 0)), $month
  FROM ledger_entries WHERE member_id = $member AND month = $month AND kind = 'award' ON CONFLICT (payment_id, kind) DO NOTHING RETURNING points;
```
If no row comes back the payment was already awarded, so I read that row and ack.

`Reverse` is a single statement that copies the award row with the points negated:
```sql
INSERT INTO ledger_entries (member_id, payment_id, kind, points, month) SELECT member_id, payment_id, 'reversal', -points, month
  FROM ledger_entries WHERE payment_id = $payment AND kind = 'award' ON CONFLICT (payment_id, kind) DO NOTHING RETURNING points;
```
No row back means it was already reversed, or there was never an award (that one gets logged and acked).

Why this holds with several replicas: duplicates are stopped by the unique index, not by anything in a replica's memory, so if two replicas get the same payment at once only one insert goes through. For the cap, the advisory lock is per member and month, so two replicas can't read the same total and both write. The lock is taken before the insert, and at read committed each statement gets a fresh snapshot, so the `SUM` sees what the previous holder committed. Other members use other locks and keep running in parallel, and the lock is released on commit, rollback or if the connection drops.
The reversal uses the stored points, so it only takes back what was actually awarded, and only awards count toward the cap. Both operations are idempotent, so when the ledger is down I just nack and let Pub/Sub redeliver. If the first attempt did commit, the retry finds the row and does nothing.
