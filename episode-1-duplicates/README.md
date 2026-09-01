# Episode 1 — The Retry That Charged Your Customer Twice

A runnable demonstration that **a timeout is not a failure**. The request
succeeded. Only the response was lost. Retrying it charges the customer again.

**Watch the episode:** https://www.youtube.com/watch?v=KE7CCnTfQqk

```bash
docker compose up --build
```

Then, in another terminal:

```bash
./scripts/capture-demo.sh
```

That script is the whole episode. It runs in about three minutes and writes
everything it measured to `capture/`.

---

## What it measured here

Twenty-five customers. Each of them pressed Pay exactly once, for $40.

| | |
| --- | --- |
| Checkouts | 25 |
| HTTP requests those 25 checkouts actually sent | 39 |
| Checkouts the customer was told had **failed** | 14 |
| Customers charged **twice** | 14 (56%) |
| Money owed | **$1,000** |
| Money collected | **$1,560** |
| Over-collected | **$560** |

The fourteen people who were charged twice are the same fourteen who were shown
an error message.

```
-- who paid twice
 customer_id | times_charged | cents
-------------+---------------+-------
           6 |             2 |  8000
           7 |             2 |  8000
           8 |             2 |  8000
          ...
```

Your numbers will differ slightly in the timings and not at all in the counts —
the processor's latency is a deterministic function of the customer id, so the
same fourteen customers time out on any machine.

---

## Where the bug is

Nowhere. That is the point.

Read [`app/main.py`](app/main.py). `POST /api/checkout` calls the payment
processor once, records the charge once, and returns. There is no race, no
missing `await`, no swallowed exception. Every line of it is correct.

Read [`scripts/checkout.py`](scripts/checkout.py). A two-second timeout and one
retry — what an HTTP client library does by default, what a mobile app does when
the user is on a train, what a load balancer does when an upstream is slow. The
retry is not a mistake either: without it, one dropped packet loses the sale.

The bug is in the assumption underneath the retry — that a timeout means the
request did not happen. It means the **response** did not arrive.

## How the pieces fit

```
scripts/checkout.py          the client. 2s timeout, one retry.
  │  POST /api/checkout
  ▼
app/  (FastAPI, :8000)       charges once, records once. No bug in it.
  │  POST /charges
  ▼
processor/  (FastAPI, :9000) the stand-in for Stripe. Slow for some customers.
  │
  ▼
postgres:16                  public.charges     — what our application believes
                             processor.ledger   — what happened to money
```

Two schemas on purpose. Every system that touches money keeps a second set of
books somewhere it does not control, and the two are separated by a network
call. When they disagree, the difference is somebody's money.

Two details that make the demo honest rather than staged:

- **The processor is slow for some customers and fast for others**, by
  `1200 + (customer_id * 137) % 2400` ms — a range that straddles the client's
  2000 ms timeout. A processor that is *always* too slow would be a rigged demo.
  This is a tail-latency problem, which is what these actually are.
- **A capture is not abandoned because a socket closed.** The processor's write
  runs under `asyncio.shield`, so hanging up does not cancel it. Real processors
  behave this way, and it is exactly why a client-side timeout tells you nothing
  about whether the money moved.

## Try it yourself

One customer, one press of Pay:

```bash
python3 scripts/checkout.py 18      # answers in 1.3s — one charge
python3 scripts/checkout.py 17      # takes 3.5s — times out twice, charged twice
```

Then ask the processor's books what really happened:

```bash
docker compose exec postgres psql -U sysense -d sysense \
  -c "SELECT * FROM processor.ledger WHERE customer_id = 17;"
```

### The knob

```bash
LATENCY_BASE_MS=100 LATENCY_SPREAD_MS=200 docker compose up --build
```

Every duplicate disappears. Nothing has been fixed — the bug is hidden behind a
dependency that happens to be fast today. That is exactly how it reaches
production, and exactly why it surfaces on the day traffic doubles.

### The other direction

Point the same retry at methods the HTTP spec calls idempotent:

```bash
for i in 1 2 3 4 5; do curl -s localhost:8000/api/customers/18; echo; done
```

Five identical responses, and nothing changed. `GET`, `PUT` and `DELETE` are
defined as idempotent in RFC 9110. `POST` is not. That is a spec fact, not a
style opinion.

## What this repository deliberately does not have

`public.charges` has no unique constraint and no idempotency key column.

That absence is not an oversight — it is the next episode. Episode 2 adds the
constraint, and then shows that the obvious way to use it still has a race
condition in it.

## Files

| Path | What it is |
| --- | --- |
| `app/main.py` | the checkout handler. Correct, and charged 14 people twice. |
| `processor/main.py` | the payment processor stand-in |
| `scripts/checkout.py` | the client, with the retry that does the damage |
| `scripts/capture-demo.sh` | runs the whole thing and records what happened |
| `scripts/summarise.py` | turns the logs into `capture/metrics.json` |
| `db/init.sql` | two schemas, and the constraint that is missing on purpose |
| `capture/` | the committed evidence. Every number in the video comes from here. |

---

Part of the **System Sense — Idempotency** mini-series.
Full playlist: https://www.youtube.com/playlist?list=PLMlexv0Ndaog
Previous series: [Caching](https://github.com/system-sense/system-sense-caching-series).
