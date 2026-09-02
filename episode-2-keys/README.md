# Episode 2 — Your Idempotency Key Has a Race Condition

Episode 1 ended with an obvious fix: give every press of Pay a key, and check
whether you have seen that key before.

This is a runnable demonstration that **the obvious fix does not work**, that
adding the missing `UNIQUE` constraint does not work either, and that the
difference between the version that fails and the version that works is where
the `INSERT` goes — not whether there is one.

**Watch the episode:** https://www.youtube.com/watch?v=g19jax6Auxc

```bash
docker compose up --build
```

Then, in another terminal:

```bash
./scripts/capture-demo.sh
```

That script is the whole episode. It fires the same load four times — same
twenty-five customers, same keys, same retry policy — changing only
`IDEMPOTENCY_MODE`, and writes everything it measured to `capture/`.

---

## What it measured here

Twenty-five customers. Each pressed Pay exactly once, for $40, and each sent an
`Idempotency-Key` header that stayed the same across their retry. Unlike
Episode 1 they all pressed at the same moment, because a race that needs
concurrency will not show up in a demo that has none.

| | `naive` | `late` | `claim` |
| --- | --- | --- | --- |
| | check, work, insert | *same handler*, `UNIQUE` added | insert first, `ON CONFLICT` |
| Requests those 25 checkouts sent | 41 | 42 | 65 |
| Charges the processor captured | 41 | 42 | **25** |
| Money owed | $1,000 | $1,000 | $1,000 |
| Money collected | **$1,640** | **$1,680** | **$1,000** |
| Over-collected | $640 | $680 | **$0** |
| Customers charged twice | 16 (64%) | 17 (68%) | **0** |
| Told their payment failed | 14 | 17 | **0** |
| Duplicate keys the table accepted | 16 | 0 | 0 |
| Inserts the database refused | — | 17 | 0 |

Read the middle column twice. The constraint **worked**: `idempotency_keys` has
no duplicate rows in it, and the database refused seventeen inserts. Seventeen
customers were charged twice anyway, and three of them got an HTTP 500 for
their trouble — because a constraint behind the payment call can only tell you
that the money moved twice.

### The window is not microseconds

Time-of-check to time-of-use is usually drawn as an instant. Measured in the
naive handler — from the `SELECT` that said "never seen this key" to the
`INSERT` that finally recorded it:

```
requests=41  min=1310.3 ms  median=2248.9 ms  max=3571.3 ms
```

The window is however long your payment call takes. And a retry is not a random
arrival: the client fires it the moment its two-second timeout expires, which
lands it in the middle of that window by construction.

### The retry gets the same answer, not just a 200

```
A  HTTP 200  Idempotency-Replayed: false
A  sha256 411349a4c8629bca
B  HTTP 200  Idempotency-Replayed: true
B  sha256 411349a4c8629bca
bodies identical: YES
```

Declining to repeat the work is half the pattern. Returning the *same response*
is the other half, and it is the half that gets skipped.

### The in-flight case

Two requests three milliseconds apart, on a customer whose charge takes 3.5
seconds. The second one finds a row and no response to replay yet:

```
A  HTTP 200  in  3.55s
B  HTTP 409  in  0.00s   {"type":"idempotency_in_flight"}
```

Wait for the first request, or answer `409` and let the client come back? Both
are defensible. This one answers `409` with a `Retry-After`, because waiting
ties up a worker for as long as the dependency is slow — which under a retry
storm is exactly when you have none to spare.

### Keys expire, and then they charge again

With the TTL set to two seconds and a retry arriving after three:

```
 ch_f9ab725c2eff4ddb | 18 | 4000 | 15:00:10.184
 ch_523dacb780e1490a | 18 | 4000 | 15:00:13.176
```

$40 owed, $80 collected. That is not a bug in the fix. An expired key is a key
you no longer have any record of, and Stripe's is 24 hours for the same reason
yours will be: you cannot keep them forever.

Your numbers will differ by a charge or two — under concurrency a handful of
customers sit right on the two-second boundary. The counts moved between 16 and
17 across two runs of this script on the same machine.

---

## The three handlers

All three are in [`app/main.py`](app/main.py), and they are shorter than this
section.

**`naive` — what everybody writes.**

```python
seen = await con.fetchrow("SELECT ... WHERE scope = $1 AND idempotency_key = $2", ...)
if seen is not None:
    return replay(seen)

result = await charge_customer(req, key)          # <- the window is this line

await con.execute("INSERT INTO idempotency_keys ...")
```

**`late` — the identical handler, with the constraint in place.** One line of
DDL apart. The loser of the race charges the customer, then dies on the way out
with a `500`, because nobody writes a `try/except` around a constraint they did
not think could fire.

**`claim` — the fix.** The key is claimed *before* the processor is called:

```sql
INSERT INTO idempotency_keys (scope, idempotency_key, ..., status, expires_at)
VALUES ($1, $2, ..., 'in_flight', now() + make_interval(secs => $4))
ON CONFLICT (scope, idempotency_key) DO UPDATE
   SET ...
 WHERE idempotency_keys.expires_at <= now()
RETURNING id
```

The application never asks *"have I seen this?"* — a question about the past
that stops being true while you are reading the answer. It asserts *"this one
is mine"*, and the database tells exactly one of the two callers that it is
not. `RETURNING` coming back empty is the loser finding out it lost.

The `ON CONFLICT DO UPDATE ... WHERE expires_at <= now()` clause is how expiry
works: an expired key is taken over rather than replayed.

## What the key is unique within

`scope` is `customer:17|POST /api/checkout`. Per user and per endpoint, not
global. Clients generate these keys and two clients will eventually pick the
same string; a global key space would let one customer's retry replay another
customer's response, which is worse than the bug being fixed.

A `request_fingerprint` goes in beside it. Same key, different body, is a client
bug, and it answers `400` rather than charging again — the same call Stripe
makes.

## One detail that is not a typo

`response_body` is `TEXT`, not `JSONB`. JSONB parses and re-serialises, so it
drops key order: what comes back out is the same *values* in a different
arrangement. The first cut of this demo used JSONB and its replay failed a byte
comparison. Store the bytes you sent.

## How the pieces fit

```
scripts/checkout.py          the client. 2s timeout, one retry, one key per press.
  │  POST /api/checkout      Idempotency-Key: k_...
  ▼
app/  (FastAPI, :8000)       naive | late | claim — IDEMPOTENCY_MODE picks one
  │  POST /charges
  ▼
processor/  (FastAPI, :9000) the stand-in for Stripe. Slow for some customers.
  │
  ▼
postgres:16                  public.charges       — what our application believes
                             public.idempotency_keys — one row per press of Pay
                             processor.ledger     — what happened to money
```

Unchanged from Episode 1: the processor is slow for some customers and fast for
others, by `1200 + (customer_id * 137) % 2400` ms, straddling the client's
2000 ms timeout. A processor that is *always* too slow would be a rigged demo.

## Try it yourself

Two requests, one key, a measured distance apart:

```bash
python3 scripts/race.py --customer 17 --gap-ms 3        # B arrives mid-flight -> 409
python3 scripts/race.py --customer 18 --gap-ms 2000     # B arrives after A    -> replay
python3 scripts/race.py --customer 18 --gap-ms 2000 --different-body   # -> 400
```

### The knob

```bash
IDEMPOTENCY_MODE=naive docker compose up --build
python3 scripts/checkout.py --concurrent $(seq 1 25)
```

Every duplicate comes back, and not one line of application logic moved — only
where the `INSERT` sits relative to the work. Try `late` too, and watch the
constraint do its job perfectly while the money still leaves twice.

## What this repository deliberately does not have

Nothing, yet, that survives the client going away entirely. Every retry here is
the client's choice: it decided to come back, and it kept its key while it did.

Put a queue in the middle and neither of those is true any more. That is
Episode 3.

## Files

| Path | What it is |
| --- | --- |
| `app/main.py` | the three handlers. One statement moved separates the broken one from the fix. |
| `db/init.sql` | the column and the constraint Episode 1 shipped without |
| `scripts/checkout.py` | the client — one key per press of Pay, fired concurrently |
| `scripts/race.py` | two requests, one key, a measured distance apart |
| `scripts/capture-demo.sh` | runs all four modes and records what happened |
| `scripts/summarise.py` | turns the logs into `capture/metrics.json` |
| `capture/` | the committed evidence. Every number in the video comes from here. |

---

Part of the **System Sense — Idempotency** mini-series.
Full playlist: https://www.youtube.com/playlist?list=PLMlexv0Ndaog
Previous episode: [Episode 1 — The Retry That Charged Your Customer Twice](../episode-1-duplicates/)
