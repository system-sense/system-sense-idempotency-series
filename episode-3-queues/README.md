# Episode 3 — 3 Ways Your Queue Silently Loses Work

Episode 2 ended with an HTTP endpoint that cannot be made to charge twice for
one key. **That endpoint is in this folder, unchanged.** `app/` is byte-for-byte
Episode 2's, pinned to the handler that works:

```bash
diff -r ../episode-2-keys/app app        # no output
```

Then a queue was put in front of it, and the same job started running twice
again — with nothing wrong in the application, nothing wrong in the queue, and
nobody having decided to retry anything.

```bash
docker compose up --build
```

Then, in another terminal:

```bash
./scripts/capture-demo.sh
```

That script is the whole episode. Eight scenarios, one application, and every
variable on the consumer. It writes what it measured to `capture/`.

---

## What it measured here

Twenty-five jobs, $40 each, published to a Redis Stream. Two workers in a
consumer group. A **two-second visibility timeout** — an entry a worker has held
for longer than that may be taken by somebody else.

The payment takes `1200 + (customer_id * 137) % 2400` ms, which is 1.2 to 3.6
seconds. **Fourteen of the twenty-five jobs outlive the two-second lease by
construction.** Nothing is killed. Nothing fails. Nothing times out.

| | jobs | deliveries | run twice | charges | owed | collected |
| --- | --- | --- | --- | --- | --- | --- |
| Lease expires mid-job | 25 | 32 | **7** | 32 | $1,000 | **$1,280** |
| The same, holding the lease | 25 | 25 | 0 | 25 | $1,000 | $1,000 |
| **The same, with Episode 2's key** | 25 | **29** | **4** | **25** | $1,000 | **$1,000** |

Read the first row and the last row together. That is the episode.

In the first, seven jobs were handed to a second worker and seven customers paid
twice — $280 over. In the last, the queue did **exactly the same thing**: four
jobs were still handed to a second worker, four entries were still claimed off
an expired lease. The books:

```
 seq | customer_id | deliveries |       outcomes
-----+-------------+------------+-----------------------
  12 |          12 |          2 | charged then replayed
  14 |          14 |          2 | charged then replayed
  16 |          16 |          2 | charged then replayed
  17 |          17 |          2 | charged then replayed
```

**The redelivery did not stop. The second charge did.** You do not fix a queue
by making it deliver once. You cannot. You fix it by making the second delivery
cost nothing.

### The lease is a guess, and it is a guess about the future

Nothing in `XREADGROUP` knows how long the work takes. `VISIBILITY_TIMEOUT_MS`
is a number somebody typed into a config file before the work existed, and every
time it is lower than the work, the job runs twice. Two seconds against a
payment that takes up to 3.6:

```
[worker-1] NEW    1788373859599-0 seq=12 customer=12 delivery=1
[worker-2] CLAIM  1788373859599-0 seq=12 customer=12 delivery=2  (idle > 2000 ms)
[worker-1] CHARGED 1788373859599-0 seq=12 customer=12 ch_8ec6bb91ac0449bf in 2.86s
[worker-2] CHARGED 1788373859599-0 seq=12 customer=12 ch_ec2b634f572e4b2e in 2.86s
```

One entry, two deliveries, two charge ids. Both workers were correct. Both
finished. Customer 12 paid $80.

### Extending the lease helps, and is not the fix

`HEARTBEAT=1` re-claims the entry every second while the work runs, so the lease
cannot expire underneath it. It cost **43 lease extensions** across 25 jobs, and
duplicates went to zero.

It is still not the fix, and the next scenario is why: a lease you are holding
can still be lost. The process can be killed, the network can partition, a
garbage collection can stop the world for longer than the timeout. Heartbeating
lowers the rate. Only an idempotent consumer changes the outcome.

## Ack-before and ack-after are the two delivery guarantees

Not a style preference. There is no third branch, and your queue library already
picked one for you.

Five jobs. One worker, which reads all five in one batch — as every queue client
does (SQS `MaxNumberOfMessages`, Kafka `max.poll.records`). At 1.5 seconds into
the first of them, a 3.5-second payment:

```bash
docker compose kill worker-1
```

Identical setup, identical kill, identical moment. The only difference is which
side of the work the `XACK` is on:

| | `ACK_MODE=after` | `ACK_MODE=before` |
| --- | --- | --- |
| | at-least-once | at-most-once |
| Pending after the kill | **5** | **0** |
| Deliveries | 6 | 1 |
| Jobs run twice | **1** | 0 |
| Jobs never attempted | 0 | **4** |
| Owed | $200 | $200 |
| Collected | **$240** | **$40** |

Ack after the work and a crash duplicates. Ack before it and a crash **loses**:
four jobs, $160 of work, acknowledged and never done. And the queue is not
hiding it — the queue has no idea. `pending=0`, `lag=0`, depth flat. Every
dashboard green.

Both runs leave one row in `job_runs` reading `started` with no `finished_at`.
That row is the only trace a killed worker leaves anywhere.

## The poison message

One message with a payload the consumer rejects on sight — `amount_cents: -1`.
Not a transient failure. No number of retries converts it into one.

With no delivery limit, watched for thirty seconds:

```
   t=+  0.0s  dead-letter depth 0   pending 1
   t=+ 10.1s  dead-letter depth 0   pending 1
   t=+ 20.2s  dead-letter depth 0   pending 1
   t=+ 29.3s  dead-letter depth 0   pending 1
```

**16 deliveries in 31.4 seconds** — one every two seconds, which is the
visibility timeout, forever. The dead-letter queue was empty the whole time and
no alert fired, because there was nothing to fire on. A dead-letter queue at
depth zero is not evidence that nothing is wrong.

Meanwhile `XLEN` reads 1, `lag` reads 0, and a graph of queue depth is a
horizontal line. The queue has depth, has a consumer, has throughput, and is
doing no work at all.

With `MAX_DELIVERIES=5` and somewhere to put it:

| | no limit | limit of 5 |
| --- | --- | --- |
| Deliveries | 16 and counting | **5** |
| Time occupying a worker | 31.4 s and counting | **9.6 s** |
| Dead-letter depth | 0 | **1** |
| Time to first alert | never | **10.1 s** |

```
   t=+  9.1s  dead-letter depth 0   pending 1
   t=+ 10.1s  dead-letter depth 1   pending 0   *** ALERT: work this system accepted will never be done ***
```

The three good jobs published behind it were charged once each, $120 for $120.

**A dead-letter queue is a diagnostic instrument, not a bin.** Its depth is not
"failures we are ignoring", it is "the processing SLO is broken and here is the
evidence, with the error and the delivery count that sent it there". The
threshold in `scripts/dlq-watch.py` is **one**, deliberately: this is not a
capacity problem. One message in it is one piece of work this system accepted
and will never do.

A dead-letter queue nobody alerts on is a bin.

## Retries break ordering

Ten jobs, published in order. Number five fails once, goes back, and comes
around again:

```
published   1  2  3  4  5  6  7  8  9  10
completed   1  2  3  4  6  7  8  9  10  5
```

**Five pairs finished out of order**, and the retried message finished tenth of
ten — behind five messages published after it. If any consumer downstream
assumed ordering, it does not hold any more, and nothing anywhere reported an
error.

---

## The eight lines the episode is about

All of it is in [`worker/main.py`](worker/main.py), and the fork is two `XACK`
calls. In the read loop, before any work has happened:

```python
if ACK_MODE == "before":                                      # at-most-once
    await r.xack(STREAM, GROUP, *[mid for mid, _ in batch])
for mid, fields in batch:
    await handle(r, mid, fields, claimed)
```

and at the end of `handle`, reached only if the work actually succeeded:

```python
if ACK_MODE == "after":                                       # at-least-once
    await r.xack(STREAM, GROUP, mid)
```

That second one is not reached when the handler throws, which is what turns a
failure into a redelivery rather than a silent drop.

And the loop above it, which is the visibility timeout made visible:

```python
batch, claimed = await claim_expired(r), True   # XAUTOCLAIM: idle > N ms
if not batch:
    batch, claimed = await read_new(r), False   # XREADGROUP >
```

`XAUTOCLAIM` does not ask whether the previous holder is dead. It cannot know.
All it knows is that the entry has been idle, and **idle is not a synonym for
abandoned** — a worker three seconds into a three-and-a-half-second payment is
idle by this definition, and is about to have its job taken.

## Why the key is minted by the producer

`scripts/enqueue.py` generates one key per job and puts it in the message. It
has to be there, and not in the consumer: **a consumer cannot tell delivery two
from delivery one.** That is the entire problem. A key it generates for itself
is a fresh key every time and protects nothing.

The message id would work too, and is tempting, and is not enough — for the
reason Episode 4 is about.

## Three sets of books

```
public.charges          what our application believes it did
public.idempotency_keys one row per job, claimed before the work   (Episode 2)
processor.ledger        what actually happened to money
public.job_runs         one row per DELIVERY — what the queue did      (NEW)
```

`job_runs` is new here and it earns its place in exactly one scenario: the last
one. Money alone cannot tell "the queue stopped redelivering" from "the queue
redelivered and it stopped mattering", and those are very different systems.
It redelivered.

## How the pieces fit

```
scripts/enqueue.py           the producer. XADDs and exits. Never retries anything.
  │  XADD checkouts
  ▼
redis:7 (:6379)              stream `checkouts`, consumer group `payments`
  │  XREADGROUP / XAUTOCLAIM / XACK / XPENDING
  ▼
worker-1, worker-2           ack before or after; lease; delivery limit; the key
  │  POST /api/checkout      Idempotency-Key: <the producer's key>
  ▼
app/  (FastAPI, :8000)       EPISODE 2'S, UNCHANGED. IDEMPOTENCY_MODE=claim.
  │  POST /charges
  ▼
processor/  (FastAPI, :9000) the stand-in for Stripe. Slow for some customers.
  │
  ▼
postgres:16                  the three books above
```

## Try it yourself

### The knob

```bash
# The bug, with nothing killed and nothing failing:
ACK_MODE=after VISIBILITY_TIMEOUT_MS=2000 docker compose up --build
python3 scripts/enqueue.py --customers $(seq 1 25)
python3 scripts/queue-state.py --label mid-flight

# Now raise the lease above the slowest payment. The duplicates disappear
# without a line of code changing:
VISIBILITY_TIMEOUT_MS=5000 docker compose up -d --force-recreate worker-1 worker-2
```

They have not been fixed. They have been hidden, which is how this reaches
production. Set `VISIBILITY_TIMEOUT_MS=5000` and add one slow customer, or one
GC pause, and they are back.

The setting that actually fixes it:

```bash
IDEMPOTENT_CONSUMER=1 docker compose up -d --force-recreate worker-1 worker-2
```

### The rest

```bash
python3 scripts/enqueue.py --poison                       # never succeeds
python3 scripts/dlq-watch.py --seconds 30                 # watch nothing happen
MAX_DELIVERIES=5 docker compose up -d --force-recreate worker-1  # now watch it

docker compose kill worker-1                              # at 1.5s into a 3.5s job
python3 scripts/queue-state.py --label after-the-kill
```

Episode 2's probes still work, because Episode 2's endpoint is still here:

```bash
python3 scripts/race.py --customer 17 --gap-ms 3          # -> 409, in flight
python3 scripts/race.py --customer 18 --gap-ms 2000       # -> replayed
```

Your numbers will differ. Seven jobs were stolen here out of the fourteen that
outlive the two-second lease, because a steal also needs a worker to be free at
the moment the lease expires — the other seven were slow enough to qualify and
nobody was available to take them. Seven in both runs of this script on this
machine; on yours it depends on how fast your disk and Docker are.

## What this repository deliberately does not have

Everything fixed here is on the **consumer** side. The producer in
`scripts/enqueue.py` is one `XADD` and an exit, and it has never been asked what
happens if it crashes between writing to the database and publishing to the
stream — or if it retries the publish and the same job goes on twice under two
different message ids.

No queue can fix that for you. That is Episode 4.

## Files

| Path | What it is |
| --- | --- |
| `worker/main.py` | the consumer. Two `XACK` calls, eight lines apart. |
| `scripts/enqueue.py` | the producer. Mints the key, publishes, exits. |
| `scripts/queue-state.py` | `XLEN`, `XINFO GROUPS`, `XPENDING` — the instrument panel |
| `scripts/dlq-watch.py` | something that alerts on the dead-letter queue |
| `scripts/resp.py` | a Redis client in fifty lines, so `scripts/` needs no `pip install` |
| `db/init.sql` | Episode 2's schema, plus `job_runs` |
| `app/`, `processor/` | Episode 2's, unchanged |
| `scripts/capture-demo.sh` | runs all eight scenarios and records what happened |
| `capture/` | the committed evidence. Every number in the video comes from here. |

---

Part of the **System Sense — Idempotency** mini-series.
Full playlist: https://www.youtube.com/playlist?list=PLMlexv0Ndaog
Previous episode: [Episode 2 — Your Idempotency Key Has a Race Condition](../episode-2-keys/)
