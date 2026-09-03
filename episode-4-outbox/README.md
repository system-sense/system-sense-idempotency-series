# Episode 4 — Exactly-Once Delivery Is a Lie

**Watch the episode:** [Exactly-Once Delivery Is a Lie: The Outbox Pattern (Part 4)](https://youtu.be/z7bLGBmXlck)

Episode 2 shipped an endpoint that cannot charge twice for one key. Episode 3
shipped a consumer that holds its lease, dead-letters what it cannot process,
and passes the producer's key on. **Both of them are in this folder, unchanged:**

```bash
diff -r ../episode-3-queues/app app          # no output
diff -r ../episode-3-queues/worker worker    # no output
```

Then twelve orders were placed, the producer was killed three times, and
**$120 disappeared** — with nothing wrong in the endpoint, nothing wrong in the
consumer, and no queue anywhere able to help.

```bash
docker compose up --build
```

Then, in another terminal:

```bash
./scripts/capture-demo.sh
```

That script is the whole episode. Six scenarios and one agent segment, the same
twelve orders throughout, and every variable in the fifteen lines between a
database and a queue. It writes what it measured to `capture/`.

> ### Would you rather read it?
>
> **[GUIDE.md](GUIDE.md)** is the written companion: the same ground more
> slowly, with the code in full, plus the questions the video's runtime budget
> cut — running more than one relay, what happens as the table grows, what the
> pattern does and does not say about ordering, and why two-phase commit is not
> the answer it looks like.
>
> This README is what the demo *is* and how to run it. The guide is the concept,
> taught, with this demo as the evidence.

---

## The bug is two lines apart, and both of them are correct

An order was placed. Two things have to become true because of it:

```python
async with state["db"].acquire() as con:                        # PostgreSQL
    order_id = await con.fetchval(INSERT_ORDER, ...)
                                                                # <- here
mid = await publish(fields)                                     # Redis
```

There is no transaction that spans those two systems. Whichever you do first,
there is a moment when one is true and the other is not, and nothing anywhere
will ever reconcile them, because neither side knows the other exists.

**This is not a bug in the handler.** There is nothing in it to fix. It is
called the dual write, and no amount of care, no retry policy and no queue
feature removes it.

## What it measured here

Twelve orders, $40 each: **$480**. The producer is killed on every fourth one —
a real `os._exit(1)`, no unwinding, no shutdown hook, which is what a kill -9,
an OOM kill, a rolling deploy and a spot-instance reclaim all look like from
inside the process. Docker restarts it in about half a second, which is why
nobody notices.

| | orders committed | events published | lost | phantom | owed | taken |
| --- | --- | --- | --- | --- | --- | --- |
| `COMMIT` then publish | 12 | **9** | **3** | 0 | $480 | **$360** |
| publish then `COMMIT` | **9** | 12 | 0 | **3** | $360 | **$480** |
| both, in one transaction | 12 | 12 | **0** | **0** | $480 | **$480** |

Same twelve orders. Same three kills, at the same three sequence numbers, in the
same place in the code. Read the first two rows as one finding, because swapping
the order of two lines did not remove the window — it moved it:

```
[orders] seq=3  customer=3  order=3 COMMITTED -> PUBLISHED 1788436266442-0
[orders] seq=4  customer=4  *** KILLED after COMMIT, before publish ***
[orders] seq=5  customer=5  order=5 COMMITTED -> PUBLISHED 1788436266966-0
```

**Commit first and you lose events.** Three orders sit in `orders` looking
completely normal. Nothing downstream will ever hear about them, so nothing will
ever charge for them, and nothing will retry — because nothing knows there is
anything to retry. $120 of revenue, gone, with every dashboard green.

**Publish first and you lose orders, which is worse.** Three customers were
charged $40 each for an order that is not in the database. Support has a payment
and nothing to attach it to. That is not a lost sale, it is a chargeback.

## The transactional outbox

Do not publish from the request handler. **Write the event into a table in the
same database, inside the same transaction as the order:**

```python
async with con.transaction():
    order_id  = await con.fetchval(INSERT_ORDER,  o.seq, o.customer_id, ...)
    outbox_id = await con.fetchval(INSERT_OUTBOX, order_id, STREAM, payload)
```

One commit. Both facts. One system. There is no window to be killed in — and
the kill switch still fired, on the same three orders, and cost nothing:

```
   orders committed          12
   events on the stream      12   (12 distinct keys)
   events lost                0   <- committed, never published
   phantom events             0   <- published, never committed
   owed  $    480.00
   taken $    480.00
```

Something else publishes the events afterwards, from the table. That is
`relay/main.py`, and the delay it adds is the honest price of the pattern:
**131.3 ms median, 169.6 ms at the worst** across the twelve rows here.

## The relay has the same bug, and this time it does not matter

Look at what the relay does: publish, then mark the row sent. Two systems again.
It cannot not have the producer's bug. So kill it in the same place:

```
[relay] outbox=1 seq=1 customer=1 key=k_efed0395701d PUBLISHED 1788436366875-0 attempt=1
[relay] outbox=1 *** KILLED after publish, before marking sent ***
[relay] outbox=1 seq=1 customer=1 key=k_efed0395701d PUBLISHED 1788436367105-0 attempt=2
```

The row was still unpublished as far as the database was concerned, so the next
relay published it again. One order, one key, **two message ids**. Three times
over, and here is the entire argument of the series in two rows:

| twelve orders, relay killed 3× | events | duplicate publishes | charges | taken |
| --- | --- | --- | --- | --- |
| Episode 2's key **off** | 15 | **3** | **15** | **$600** |
| Episode 2's key **on** | 15 | **3** | **12** | **$480** |

**The duplicate publishing did not stop. The second charge did.**

```
 seq | customer_id | deliveries |       outcomes
-----+-------------+------------+-----------------------
   1 |           1 |          2 | charged then replayed
   2 |           2 |          2 | charged then replayed
   3 |           3 |          2 | charged then replayed
```

That is what the outbox actually promises, and it is worth being precise about
because the pattern is routinely oversold:

> **The outbox does not eliminate duplicates. It eliminates *loss*.**
> It converts an unrecoverable failure into a duplicate — and a duplicate is
> what Episodes 2 and 3 were built to absorb.

Which is the title of the series, stated the other way around. Exactly-once
*delivery* is not achievable — not here, not in Kafka, not anywhere, because two
parties over a lossy channel cannot both become certain in any finite number of
messages. Exactly-once *effects* are achievable, and this is all they have ever
been:

**at-least-once delivery + an idempotent consumer.**

## Everything killed at once

The finale scenario runs all of it together: the producer killed on every fourth
order, the relay killed three times before it could mark a row sent.

```
   orders committed          12
   events on the stream      15   (12 distinct keys)
   events lost                0
   phantom events             0
   duplicate publishes        3
   charges                   12
   owed  $    480.00
   taken $    480.00
```

Six process deaths. Nothing lost, nothing charged twice, and not one line of
`app/` or `worker/` different from what Episodes 2 and 3 shipped.

## What Kafka's "exactly-once semantics" actually covers

It is real, it is genuinely useful, and it is narrower than its name.

Kafka's EOS gives you an **idempotent producer** (a sequence number per producer
per partition, so a retried `produce` is deduplicated by the broker) and
**transactions** across topics and partitions, which makes
consume-transform-produce atomic: the output records and the input offsets
commit together, or neither does.

Every one of those guarantees ends at the edge of Kafka.

Your call to Stripe is not in Kafka's transaction. Your write to Postgres is not
in Kafka's transaction. Read a message, charge a customer, commit the offset —
Kafka can make the offset commit atomic with a write *to Kafka*, and it has
nothing whatsoever to say about the $40. **Exactly-once within the log is not
exactly-once in your system**, and the gap between those two sentences is where
this entire series lives.

CDC / log tailing (Debezium, or Postgres logical decoding directly) is the same
outbox pattern with the relay deleted and the write-ahead log read instead. Same
at-least-once property, for the same reason: something has to record how far it
got, and that record is not in the same transaction as the send. The honest
trade is that you run no relay and instead take on a Debezium-shaped operational
dependency — and your table schema quietly becomes a public API.

## The same problem, where it is being rediscovered

An agent workflow is a durable execution: steps, checkpointed, so a crash
resumes rather than restarts. Resuming means **replaying** the steps already
taken, so every side effect in the workflow is about to be attempted again.
That is this series, in a costume.

There is one genuinely new wrinkle, and it is why this is not a rerun:
**the model's output is not deterministic.** So the thing everybody reaches for
first — hash the step's payload, skip it if you have seen that hash — cannot
work. Not "works badly". Cannot work.

Four agent runs, each replayed three times, twelve attempts, and the stub model
returned **twelve different strings**:

| twelve attempts, keyed on | charged | replayed | owed | taken |
| --- | --- | --- | --- | --- |
| the payload (content) | **12** | 0 | $160 | **$480** |
| `(run_id, step_index, action_type)` | **4** | **8** | $160 | **$160** |

```
run_001 replay 1  customer 1   key=k_run_001:2:charge_customer   CHARGED   ch_85663d8e...
run_001 replay 2  customer 1   key=k_run_001:2:charge_customer   replayed  (the same response, no second charge)
run_001 replay 3  customer 1   key=k_run_001:2:charge_customer   replayed  (the same response, no second charge)
```

You do not key on **what** the step produced. You key on **where the step is**.
That triple is fixed by the workflow's structure before the model is ever
called, and it is identical on every replay — which is exactly what Episode 2
needed a key to be. It is Episode 2's idempotency key, derived from position
instead of from content.

**There is no model in this repository, and there must not be one.** A real
model call costs money and returns something different every time, and this
series' rule is that every number on screen is reproducible by anyone who clones
the repo. `draft_note()` in `scripts/agent-run.py` is a stub that does the one
thing that matters: it returns a different string on every call.

## The reconciler is the tell

`scripts/reconcile.py` puts the database's orders beside the queue's events and
counts the ways they disagree. It is useful, and needing it is the diagnosis:

**you cannot write a reconciler for a failure whose whole nature is that neither
side knows it happened.** The only reason this one works is that the key is on
both sides of it — which is the fix, not the diagnosis.

## How the pieces fit

```
scripts/place-orders.py      the client. Places orders. Never retries one.
  │  POST /api/orders
  ▼
producer/  (FastAPI, :8100)  NEW. The dual write, and the window inside it.
  │                          PUBLISH_MODE=commit_first|publish_first|outbox
  │                          CRASH_EVERY=N  -> os._exit(1) in the window
  ├── INSERT orders ─────────┐
  └── INSERT outbox ─────────┤ one transaction
                             ▼
                        postgres:16         orders, outbox, and Episodes 1–3's books
                             │
relay/  (a loop)             │  SELECT ... WHERE published_at IS NULL
  │  NEW. Publishes from the outbox, then marks the row sent. Two systems again:
  │  CRASH_AFTER_PUBLISH=N kills it in between, and the row goes out twice.
  ▼
redis:7 (:6379)              stream `checkouts`, consumer group `payments`
  ▼
worker-1, worker-2           EPISODE 3'S, UNCHANGED. Lease held, key passed on.
  │  POST /api/checkout      Idempotency-Key: <the order's key>
  ▼
app/  (FastAPI, :8000)       EPISODE 2'S, UNCHANGED. IDEMPOTENCY_MODE=claim.
  ▼
processor/  (FastAPI, :9000) the stand-in for Stripe. Slow for some customers.
```

## Why the key is minted by the producer

`producer/main.py` generates one key per **order**, before either write, and
carries it in the event. Episode 3 established that a consumer cannot mint it —
a consumer cannot tell delivery two from delivery one. Episode 4 is why the
message id will not do either: **the relay can publish the same intent under two
different message ids**, and it did, three times, in the table above.

The order is the intent. The key belongs to the order.

## Try it yourself

### The knob

```bash
# Lose three events and $120, with nothing wrong in any handler:
PUBLISH_MODE=commit_first CRASH_EVERY=4 docker compose up --build
python3 scripts/place-orders.py --customers $(seq 1 12)
python3 scripts/reconcile.py --label the-damage

# The mirror image. Money moves for orders that do not exist:
PUBLISH_MODE=publish_first CRASH_EVERY=4 docker compose up -d --force-recreate orders

# The fix. Same kills, same places, nothing lost:
PUBLISH_MODE=outbox CRASH_EVERY=4 docker compose up -d --force-recreate orders
```

Now set `CRASH_EVERY=0` and watch the bug disappear without having been fixed —
which is where most systems are, right up until a deploy.

### The relay, and the point of the whole series

```bash
# Kill the relay before it can mark rows sent, with Episode 2's key OFF:
PUBLISH_MODE=outbox CRASH_AFTER_PUBLISH=3 IDEMPOTENT_CONSUMER=0 \
  docker compose up -d --force-recreate orders relay worker-1 worker-2
python3 scripts/place-orders.py --customers $(seq 1 12)
python3 scripts/reconcile.py --label key-off        # 15 events, 15 charges, $600

# The identical run, key ON:
IDEMPOTENT_CONSUMER=1 docker compose up -d --force-recreate worker-1 worker-2
python3 scripts/reconcile.py --label key-on         # 15 events, 12 charges, $480
```

### The agent segment

```bash
python3 scripts/agent-run.py --runs 4 --replays 3 --key payload
python3 scripts/agent-run.py --runs 4 --replays 3 --key position
```

Episodes 2 and 3's probes still work, because their code is still here:

```bash
python3 scripts/race.py --customer 17 --gap-ms 3     # -> 409, in flight
python3 scripts/enqueue.py --poison                  # -> never succeeds
```

Your numbers will differ in the timings and not in the counts. The kills are
deterministic on `seq`, so three orders are lost in `commit_first` on any
machine; the relay lag (131 ms median here) depends on your disk.

## Four sets of books, and the pair that matters

```
public.orders           the business fact                            (NEW)
public.outbox           the event, committed with it                 (NEW)
public.charges          what our application believes it did
public.idempotency_keys one row per intent, claimed before the work   (Ep 2)
public.job_runs         one row per DELIVERY — what the queue did     (Ep 3)
processor.ledger        what actually happened to money
```

The pair this episode is about is `orders` against the stream itself, and it is
the pair nobody has, because the two of them live in different systems and no
transaction spans them.

## Files

| Path | What it is |
| --- | --- |
| `producer/main.py` | the dual write, three arrangements of it, and the kill switch |
| `relay/main.py` | publishes the outbox, and has the same bug harmlessly |
| `scripts/place-orders.py` | the client. Places orders and never retries one. |
| `scripts/reconcile.py` | the database's orders beside the queue's events |
| `scripts/agent-run.py` | a replayed agent run, keyed two ways. No model, on purpose. |
| `db/init.sql` | Episode 3's schema, plus `orders` and `outbox` |
| `app/`, `worker/`, `processor/` | Episodes 2 and 3's, unchanged |
| `scripts/capture-demo.sh` | runs all of it and records what happened |
| `capture/` | the committed evidence. Every number in the video comes from here. |

---

Part of the **System Sense — Idempotency** mini-series.
Watch this episode: https://youtu.be/z7bLGBmXlck
Full playlist: https://www.youtube.com/playlist?list=PLMlexv0Ndaog
Previous episode: [Episode 3 — 3 Ways Your Queue Silently Loses Work](../episode-3-queues/) · [watch](https://youtu.be/rR81FtkdqT8)
