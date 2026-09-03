# Idempotency Keys, and the Race in the Obvious Version

**A written companion to Episode 2 of System Sense — [Exactly-Once Is a Lie](../).**

The video is about fourteen minutes. This covers the same ground more slowly,
with the SQL in full, and then goes on into what would not fit: who mints the
key and why it cannot be the server, what to store when the work *fails*, what
scoping gets you and what a global key space costs, why an in-process lock is not
a substitute, and what happens the moment a key expires.

Every figure here comes from `capture/metrics.json`, produced by
`./scripts/capture-demo.sh` in this folder. Nothing is estimated.

**Who this is for:** you have read [Episode 1](../episode-1-duplicates/), you
accept that retries will happen, and your plan is to give each attempt a key and
check whether you have seen it. That plan has a race in it. This is where it is.

---

## Contents

1. [The obvious fix, measured](#1-the-obvious-fix-measured)
2. [Why check-then-insert is a race](#2-why-check-then-insert-is-a-race)
3. [The window is not microseconds](#3-the-window-is-not-microseconds)
4. [Adding the UNIQUE constraint does not fix it](#4-adding-the-unique-constraint-does-not-fix-it)
5. [The fix: claim the key before you do the work](#5-the-fix-claim-the-key-before-you-do-the-work)
6. [Replaying the response is half the pattern](#6-replaying-the-response-is-half-the-pattern)
7. [The in-flight case](#7-the-in-flight-case)
8. [Who mints the key](#8-who-mints-the-key)
9. [Scoping: what the key is unique within](#9-scoping-what-the-key-is-unique-within)
10. [The fingerprint: same key, different body](#10-the-fingerprint-same-key-different-body)
11. [What to store when the work fails](#11-what-to-store-when-the-work-fails)
12. [Keys expire, and then they charge again](#12-keys-expire-and-then-they-charge-again)
13. [Why an in-process lock is not enough](#13-why-an-in-process-lock-is-not-enough)
14. [What this does not protect](#14-what-this-does-not-protect)
15. [Exercises](#15-exercises)

---

## 1. The obvious fix, measured

Same twenty-five customers, same keys, same retry policy, four runs, one setting
different. Unlike Episode 1 they all press Pay at the same moment — a race that
needs concurrency will not show up in a demo that has none.

| | `naive` | `late` | `claim` |
| --- | --- | --- | --- |
| | check, work, insert | *same handler*, `UNIQUE` added | insert first, `ON CONFLICT` |
| Requests those 25 checkouts sent | 41 | 42 | **65** |
| Charges the processor captured | 41 | 42 | **25** |
| Money owed | $1,000 | $1,000 | $1,000 |
| Money collected | **$1,640** | **$1,680** | **$1,000** |
| Over-collected | $640 | $680 | **$0** |
| Customers charged twice | 16 (**64%**) | 17 (**68%**) | **0** |
| Told their payment failed | 14 | 17 | **0** |
| Duplicate keys the table accepted | 16 | 0 | 0 |
| Inserts the database refused | — | 17 | 0 |

Read the middle column twice. **The constraint worked.** `idempotency_keys` has
no duplicate rows in it and the database refused seventeen inserts. Seventeen
customers were charged twice anyway.

That column is the entire episode. A constraint that sits *behind* the payment
call can only tell you the money moved twice.

---

## 2. Why check-then-insert is a race

Here is the handler everybody writes, from [`app/main.py`](app/main.py):

```python
async with state["db"].acquire() as con:
    seen = await con.fetchrow(SEEN, scope, key)      # have I seen this key?

if seen is not None:
    return replay(seen)

# ── the window ──────────────────────────────────────────────────────────
result = await charge_customer(req, key)             # the money moves here
body = serialise(result)

async with state["db"].acquire() as con:
    await con.execute(RECORD, scope, key, fingerprint(req), body, TTL)
```

It reads correctly. Check whether you have seen the key; if not, do the work and
write it down.

The problem is that "have I seen this key?" is **a question about the past whose
answer stops being true while you are reading it**. Between the `SELECT` and the
`INSERT`, a second request carrying the same key can run the same `SELECT` and be
told, truthfully, that this key has never been seen.

This is a **time-of-check to time-of-use** bug — TOCTOU — and it is the same
shape as `if not os.path.exists(p): create(p)`. The difference here is that the
thing between the check and the use is a payment.

Two requests, both correct, both told the truth, two charges:

```
t=0.000  A: SELECT key -> absent
t=0.003  B: SELECT key -> absent          <- still true. nothing has been written.
t=0.004  A: POST /charges  ...
t=0.005  B: POST /charges  ...            <- $40 leaves the account. twice.
t=3.550  A: INSERT key
t=3.551  B: INSERT key                    <- no constraint: accepted. 16 times.
```

---

## 3. The window is not microseconds

TOCTOU is usually drawn as an instant — two operations racing over nanoseconds,
requiring bad luck to hit. That intuition is why this bug ships.

Measured in the naive handler, from the `SELECT` that said "never seen this key"
to the `INSERT` that finally recorded it:

```
requests=41   min=1310.3 ms   median=2248.9 ms   max=3571.3 ms
```

**The window is however long your payment call takes.** Over two seconds, at the
median. It is not a narrow target.

And the retry is not a random arrival. The client fires it the moment its
two-second timeout expires, which lands it *in the middle of that window by
construction*. This is not a race you lose occasionally under load. It is a race
you lose whenever the dependency is slow enough to trigger the retry that causes
it — which is to say, exactly when it happens at all.

---

## 4. Adding the UNIQUE constraint does not fix it

The natural next move is the constraint the table was missing:

```sql
CREATE UNIQUE INDEX idempotency_keys_scope_key_uniq
    ON idempotency_keys (scope, idempotency_key);
```

Run the identical handler with it in place — that is the `late` column — and the
database does its job perfectly. Seventeen inserts refused. Zero duplicate rows.

And **$1,680 collected against $1,000 owed**, which is worse than the run
without the constraint.

Two things went wrong, and they are both instructive:

**The constraint is behind the work.** By the time the `INSERT` runs, the charge
has been captured. A `UNIQUE` violation at that point is a very precise report
that you have already taken the money twice. It is a smoke alarm wired to fire
after the building has burned down.

**Nobody writes a `try/except` around that insert.** You do not guard against a
constraint you did not think could fire, so the losing request dies on the way
out with an HTTP 500 — and the customer is told their payment failed. Again.
Three of them saw that 500 in this run, and the `late` column reports seventeen
customers told their payment failed against fourteen in the naive run. **Adding
the constraint made the customer experience worse.**

The lesson generalises past idempotency: **a constraint tells you an invariant
was violated. It cannot prevent an effect that already left your system.** If the
thing you are protecting is outside the database, the arbiter has to run before
you call it.

---

## 5. The fix: claim the key before you do the work

Invert it. Do not ask whether you have seen the key. **Assert that this one is
yours**, and let the database tell exactly one of the callers that it is not.

```sql
INSERT INTO idempotency_keys (scope, idempotency_key, request_fingerprint,
                              status, expires_at)
VALUES ($1, $2, $3, 'in_flight', now() + make_interval(secs => $4))
ON CONFLICT (scope, idempotency_key) DO UPDATE
   SET request_fingerprint = EXCLUDED.request_fingerprint,
       status              = 'in_flight',
       response_status     = NULL,
       response_body       = NULL,
       created_at          = now(),
       completed_at        = NULL,
       expires_at          = EXCLUDED.expires_at
 WHERE idempotency_keys.expires_at <= now()
RETURNING id, xmax::text::bigint <> 0 AS revived;
```

The mechanics, line by line, because every clause is load-bearing:

- **`INSERT ... ON CONFLICT`** is one statement, so it is atomic. There is no
  window between deciding and acting, because they are the same operation.
- **`RETURNING` coming back empty is the loser finding out it lost.** No
  exception, no 500 — a normal result the handler branches on.
- **`DO UPDATE ... WHERE expires_at <= now()`** revives a key that has expired,
  rather than refusing forever. Without it an expired key is a tombstone that
  blocks the customer from ever paying again.
- **`xmax <> 0`** is true only on the `DO UPDATE` path: the row existed, it had
  expired, and we took it over. Worth logging, because a revived key means a real
  second charge is about to happen (see §12).

The status column matters as much as the constraint. The row is written
`in_flight` **before** the processor is called and updated to `completed`
afterwards, so the table is also the record of a request still in progress.

Result: **$1,000 owed, $1,000 collected, nobody charged twice, and nobody told
their payment failed.** It took 65 requests to get there rather than 41, because
the losers now come back on a 409 instead of giving up — which is the system
working, not overhead.

---

## 6. Replaying the response is half the pattern

Declining to repeat the work is the half everybody implements. Returning the
**same response** is the half that gets skipped, and skipping it breaks the
caller in a way that is hard to trace.

Consider a retry that gets back `200 OK` with an empty body, or a fresh `200`
with a *different* `charge_id` than the first call would have returned. The
customer was not charged twice — and the client still cannot reconcile its own
records, cannot show a receipt, and may store the wrong id.

So store the status and the bytes:

```
A  HTTP 200  Idempotency-Replayed: false
A  sha256 411349a4c8629bca
B  HTTP 200  Idempotency-Replayed: true
B  sha256 411349a4c8629bca
bodies identical: YES
```

Two implementation details that are easy to get wrong:

**Store `TEXT`, not `JSONB`.** JSONB parses and re-serialises: it drops key order
and normalises whitespace, so what comes back out is the same *values* in a
different arrangement. That is not the same response, and a client that hashes
the body or compares it to what it received will say so. The first cut of this
demo used JSONB and its replay failed a byte comparison.

**Serialise once.** Build the body, send those bytes to the first caller, store
those bytes for the second. Re-encoding on the way out reintroduces the same
problem one layer up.

**Tell the caller it is a replay.** The `Idempotency-Replayed: true` header costs
nothing and turns an invisible behaviour into a debuggable one.

---

## 7. The in-flight case

There is a state the naive version never has to think about: a second request
arrives, the key is claimed, and there is **no response to replay yet** because
the first request is still talking to the processor.

Two requests three milliseconds apart, on a customer whose charge takes 3.5
seconds:

```
A  HTTP 200  in  3.55s
B  HTTP 409  in  0.00s   {"type":"idempotency_in_flight"}
```

Three defensible answers, and this is genuinely under-discussed:

| Answer | What it costs |
| --- | --- |
| **Block** until the first request finishes, then replay its response | ties up a worker for as long as the dependency is slow — under a retry storm, exactly when you have none to spare |
| **409 + `Retry-After`**, let the client come back | the client already knows how to come back; that is how it got here |
| **Block with a short bound**, then fall back to 409 | best of both, most code |

This repo answers `409` with `Retry-After: 1`. Stripe's API does something
similar, returning a conflict when a request with the same key is still in
progress.

Whichever you pick, **pick it deliberately**, and make sure the client handles
it. A client that treats 409 as a permanent failure turns your correct answer
into a lost sale. In this repo the caller polls: the fixed run made 23 conflict
polls and 17 replays across 65 requests, and lost nothing.

---

## 8. Who mints the key

**The client. Never the server.**

This looks like a detail and it is the whole pattern. A key exists to say "all of
these attempts are one intent". The only party that knows that is the party that
formed the intent.

If the server generates the key, it generates a different one for every request
it receives — including the retry — so the key groups nothing and protects
nothing.

Practically:

- **A browser or app** generates a UUIDv4 when the user opens the checkout form,
  or when they press the button, and reuses it for every retry of that press.
  Regenerating it on retry is the most common way to implement this pattern and
  get nothing from it.
- **A server calling another server** generates one per logical operation and
  keeps it across its own retries.
- **A queue consumer cannot mint one at all** — it cannot tell delivery two from
  delivery one. The key has to travel *in the message*, minted by the producer.
  That is [Episode 3](../episode-3-queues/).
- **A producer that publishes the same intent twice** needs the key to be older
  than the publish, minted with the business fact. That is
  [Episode 4](../episode-4-outbox/).

The whole series is that one idea moving one layer outward at a time: **the key
belongs to the intent, and it must be minted by whoever formed the intent.**

A useful alternative to random keys: derive the key deterministically from
something stable about the intent — a cart id plus a version, an order number, a
`(run_id, step_index)` pair. Then even a client that lost its state can
regenerate the same key. Do not derive it from the request *body* alone unless
the body is genuinely stable; [Episode 4 §18](../episode-4-outbox/GUIDE.md#18-the-same-problem-in-agent-workflows)
is a case where it is not.

---

## 9. Scoping: what the key is unique within

The constraint in this repo is on `(scope, idempotency_key)`, not on the key
alone, and `scope` is `customer:{id}|POST /api/checkout`.

```python
def scope_for(req: CheckoutRequest) -> str:
    return f"customer:{req.customer_id}|{ENDPOINT}"
```

Two reasons, and the first is a security argument rather than a correctness one.

**Clients generate these keys, and two clients will eventually pick the same
string.** Sometimes by collision, sometimes because somebody hardcoded
`"test-key-1"` and shipped it. In a global key space, that means one customer's
retry can be served *another customer's stored response* — which leaks their
charge id and amount, and does not take the payment. That is a worse bug than the
one you set out to fix.

**Per-endpoint scoping keeps unrelated operations from colliding.** A client that
reuses one key for "create order" and "cancel order" gets the wrong replay.

In a multi-tenant system, put the tenant in the scope too. The rule: **scope by
everything that must not be shared.**

The cost is that a key is only meaningful within its scope, so you cannot ask
"has this key ever been used" globally. That question is not usually one you need.

---

## 10. The fingerprint: same key, different body

What should happen if a client sends the same key with a *different* request?

```
A  HTTP 200  {"customer_id": 18, "amount_cents": 4000}
B  HTTP 400  {"customer_id": 18, "amount_cents": 9900}   <- same key
```

This is a client bug, and there are two ways to answer it:

- **Replay the first response.** Now the client believes it charged $99 and it
  charged $40. You have silently done the wrong thing.
- **Refuse with a 400.** The client finds out immediately that it has a bug.

Stripe answers with an error, and so does this repo. The mechanism is a hash of
the body the key was first used with:

```python
def fingerprint(req: CheckoutRequest) -> str:
    canonical = json.dumps(req.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]
```

Note `sort_keys=True`: the fingerprint must be of the *canonical* form, or a
client that serialises its JSON keys in a different order on the retry gets a
spurious 400. This is the one place in the pattern where you *want* to normalise,
and it is the exact opposite of the rule for the stored response in §6 — because
here you are comparing meaning, and there you are reproducing bytes.

---

## 11. What to store when the work fails

The demo's happy path stores `status = 'completed'` with the response. What about
when the processor returns a decline, or times out, or throws?

This is where implementations quietly go wrong in both directions:

**Do not store a failure as `completed`.** If you do, the customer's legitimate
retry replays the failure forever and they can never pay you. A declined card
that the customer then fixes must be retryable.

**Do not leave the row `in_flight` forever either.** A process that dies
mid-charge leaves a claimed key with no response, and every retry gets a 409
until the heat death of the universe. That is the `late` column's failure mode
wearing a different hat.

The workable rule:

| Outcome | What to do with the key |
| --- | --- |
| Success | `completed`, store status + body, replay it |
| Deterministic client error (400, validation) | `completed`, store it — the retry will fail identically anyway |
| Transient failure (5xx, timeout, network) | **release the key** — delete the row, or leave it expiring soon |
| Process died mid-flight | the `expires_at` lease reclaims it; that is what §5's `DO UPDATE ... WHERE expires_at <= now()` is for |

Note the third row is the one that reintroduces a duplicate risk: if you release
the key after a timeout to the processor, and the processor actually captured,
the retry charges again. Which is Episode 1, one level in. There is no way out of
that with keys alone — you need the processor to accept an idempotency key too,
which is exactly why Stripe's API has one.

**A short in-flight lease is the practical answer.** Long enough that a slow-but-
alive request is not stolen, short enough that a dead one is reclaimed in seconds
rather than hours. If that trade sounds familiar, it is the visibility timeout
from [Episode 3](../episode-3-queues/), and it has the same unfixable guess at
its centre.

---

## 12. Keys expire, and then they charge again

You cannot keep keys forever, so they have a TTL. Stripe's is 24 hours; this repo
defaults to the same and the capture drops it to two seconds to show what happens
at the boundary.

TTL of two seconds, retry arriving after three:

```
 ch_f9ab725c2eff4ddb | 18 | 4000 | 15:00:10.184
 ch_523dacb780e1490a | 18 | 4000 | 15:00:13.176
```

$40 owed, $80 collected.

**That is not a bug in the fix.** An expired key is a key you no longer have any
record of, and a request carrying one is indistinguishable from a new intent. The
guarantee the pattern actually offers is:

> the same key will not be honoured twice **within its TTL**

Which means the TTL is not a storage-cleanup setting. It is the width of your
guarantee, and it should be chosen against how long a client might keep retrying:
longer than any retry schedule you ship, longer than a user might leave a tab
open and hit refresh, longer than your longest queue redelivery window.

The storage cost is usually small enough not to matter — one row per intent, with
the response body being the only variable part — and §11's rules keep the table
from filling with abandoned in-flight rows. If it does become large, prune by
`expires_at` on a schedule; the index in this repo exists for exactly that.

---

## 13. Why an in-process lock is not enough

A tempting shortcut: keep a mutex or a set of in-flight keys in memory, and serve
duplicates from that instead of hitting the database.

It works on one process, and it fails the moment you have two, which is every
production deployment. Both replicas hold a different in-memory set, both see the
key as new, both charge.

Two variants that also fail, for the record:

- **A distributed lock in Redis.** Better, and now you have a lock with a lease
  that can expire while the work is still running — which is
  [Episode 3](../episode-3-queues/)'s visibility timeout, with all of the same
  problems, plus a second system that can be unavailable independently of your
  database.
- **A `SELECT ... FOR UPDATE` on some other row.** This does work, but it holds a
  transaction open across a network call to the payment processor. Now your
  database's connection pool is coupled to your payment provider's latency.

**The `INSERT ... ON CONFLICT` in §5 is better than all of them** because it is a
single atomic statement, needs no second system, holds nothing open across the
slow call, and its "lease" is a timestamp column you can query.

---

## 14. What this does not protect

The pattern in this episode makes one HTTP endpoint safe to call twice. It does
not make your system exactly-once, and it is worth being precise about the gaps —
each one is a later episode.

- **It does not protect work that happens after the response.** If the handler
  charges, records, returns, and *then* a background task sends an email, the key
  did not cover the email.
- **It does not protect the caller from a lost response.** The client still
  cannot tell whether its request succeeded; it just now has a safe way to ask
  again.
- **It does not survive a queue in front of it.** The endpoint stays correct, and
  a queue redelivering the same job with a freshly-minted key charges again.
  That is [Episode 3](../episode-3-queues/), which puts *this exact handler*
  behind a queue, unchanged, and watches it charge people twice.
- **It does not help if the event never gets published.** If the thing that was
  supposed to call this endpoint died before it did, no key anywhere helps. That
  is [Episode 4](../episode-4-outbox/).

---

## 15. Exercises

**1. Watch the race directly.**

```bash
python3 scripts/race.py --customer 17 --gap-ms 3      # -> 409, in flight
python3 scripts/race.py --customer 18 --gap-ms 2000   # -> replayed
```

Two requests, one key, and the gap decides which branch of §5 and §7 you land in.

**2. Run the naive handler and watch the window.** The capture prints the
measured time between the `SELECT` and the `INSERT` for every request. Compare
the median to your client's timeout and convince yourself the retry lands inside
it by construction, not by luck.

**3. Break the replay.** Change `response_body` from `TEXT` to `JSONB` and
compare the two response hashes. This is the byte-comparison failure from §6, and
it is a two-minute reproduction.

**4. Expire a key under a retry.** Set `IDEMPOTENCY_TTL_SECONDS=2` and send a
retry after three seconds. $40 owed, $80 collected — then reason about what TTL
your own retry schedule actually requires.

**5. Delete the scope.** Change the unique index to be on `idempotency_key`
alone, then have two different customers use the key `"k_test"`. Watch one
customer receive the other's charge id.

---

## Where to go next

This endpoint is now correct, and it stays correct for the rest of the series —
Episodes 3 and 4 both ship it byte-for-byte and you can `diff` it.

What changes is **who decides when to retry**. A client that retries is making a
choice you can read in its source. A queue redelivers on a schedule nobody wrote
down, to a worker that has no idea it is the second one.

- [Episode 3 — queues](../episode-3-queues/GUIDE.md)
- [Episode 4 — the outbox](../episode-4-outbox/GUIDE.md)
- [Episode 1 — where duplicates come from](../episode-1-duplicates/GUIDE.md)

---

Part of the **System Sense — Idempotency** mini-series.
