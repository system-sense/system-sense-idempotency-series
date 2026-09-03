# Visibility Timeouts, Poison Messages, and What a Dead Letter Queue Is For

**A written companion to Episode 3 of System Sense — [Exactly-Once Is a Lie](../).**

The video is about fifteen minutes. This covers the same ground more slowly, with
the consumer loop in full, and then goes on into what would not fit: how these
three bugs appear in SQS, Kafka and RabbitMQ specifically, why raising the
visibility timeout is not a fix, what actually belongs in a dead letter queue and
how to redrive it safely, and which queue metric is the one worth alerting on.

Every figure here comes from `capture/metrics.json`, produced by
`./scripts/capture-demo.sh` in this folder. Nothing is estimated.

**Who this is for:** you have work on a queue, a consumer that processes it, and
a config file with a timeout in it that somebody set before the code existed. By
the end you will know what that number actually decides.

---

## Contents

1. [The failure, with nothing killed](#1-the-failure-with-nothing-killed)
2. [The lease is a guess about the future](#2-the-lease-is-a-guess-about-the-future)
3. [The same bug in SQS, Kafka and RabbitMQ](#3-the-same-bug-in-sqs-kafka-and-rabbitmq)
4. [Why raising the timeout is not a fix](#4-why-raising-the-timeout-is-not-a-fix)
5. [Heartbeating, and what it still cannot promise](#5-heartbeating-and-what-it-still-cannot-promise)
6. [Where the acknowledgement goes](#6-where-the-acknowledgement-goes)
7. [Batch size is where at-most-once does its damage](#7-batch-size-is-where-at-most-once-does-its-damage)
8. [The poison message](#8-the-poison-message)
9. [A dead letter queue is an instrument, not a bin](#9-a-dead-letter-queue-is-an-instrument-not-a-bin)
10. [Redriving a dead letter queue safely](#10-redriving-a-dead-letter-queue-safely)
11. [Backoff, and retry topics](#11-backoff-and-retry-topics)
12. [Retries break ordering](#12-retries-break-ordering)
13. [The fix, and why the key has to come from the producer](#13-the-fix-and-why-the-key-has-to-come-from-the-producer)
14. [What to alert on](#14-what-to-alert-on)
15. [Exercises](#15-exercises)

---

## 1. The failure, with nothing killed

```bash
docker compose up --build
./scripts/capture-demo.sh
```

Twenty-five jobs on a queue, $40 each, two workers pulling from it. No client.
Nothing retrying. Nobody's browser, nobody's timeout, nobody's decision.

| | jobs | deliveries | run twice | charges | owed | collected |
| --- | --- | --- | --- | --- | --- | --- |
| Lease expires mid-job | 25 | **32** | **7** | 32 | $1,000 | **$1,280** |
| The same, holding the lease | 25 | 25 | 0 | 25 | $1,000 | $1,000 |
| **The same, with Episode 2's key** | 25 | **29** | **4** | **25** | $1,000 | **$1,000** |

$280 over-collected in the first row, and here is what did **not** happen:

- Nothing crashed. Nothing timed out. Nothing threw an exception.
- No worker died, no network dropped, no request failed.
- **Every job succeeded on its first attempt.**

Twenty-five jobs went in. Thirty-two of them ran.

And the endpoint in front of the queue is [Episode 2](../episode-2-keys/)'s,
unchanged, in the mode that cannot charge twice for one key. You can diff it:

```bash
diff -r ../episode-2-keys/app app        # no output
```

The duplicates come from a number in a config file.

---

## 2. The lease is a guess about the future

Redis Streams consumer groups are used here because they have real
visibility-timeout semantics rather than an imitation:

| Command | What it does |
| --- | --- |
| `XADD` | the producer puts work on the stream |
| `XREADGROUP` | a consumer takes work; it enters that consumer's **pending list** |
| `XACK` | the consumer says it is done; the entry leaves the pending list |
| `XPENDING` | everything delivered and not acknowledged, with idle time and delivery count |
| `XAUTOCLAIM` | hand any entry idle longer than N ms to somebody else |

That **N** is the visibility timeout, and it is the whole episode.

Note what it is *not*: it is not "how long the work takes". Nothing in this
protocol knows how long the work takes. It is a number somebody typed into a
config file before the code existed.

The payment here takes `1200 + (customer_id * 137) % 2400` ms — 1.2 to 3.6
seconds — against a two-second lease. **Fourteen of the twenty-five jobs outlive
their lease by construction.** Seven were actually stolen, because a steal also
needs a second worker free at the moment the lease expires.

```
[worker-1] NEW    1788373859599-0 seq=12 customer=12 delivery=1
[worker-2] CLAIM  1788373859599-0 seq=12 customer=12 delivery=2  (idle > 2000 ms)
[worker-1] CHARGED 1788373859599-0 seq=12 customer=12 ch_8ec6bb91ac0449bf in 2.86s
[worker-2] CHARGED 1788373859599-0 seq=12 customer=12 ch_ec2b634f572e4b2e in 2.86s
```

One entry, two deliveries, two charge ids. **Both workers were correct. Both
finished.** Customer 12 paid $80.

The critical sentence, from `worker/main.py`:

> `XAUTOCLAIM` does not ask whether the previous holder is dead. It cannot know.
> All it knows is that the entry has been idle — and **idle is not a synonym for
> abandoned.** A worker three seconds into a three-and-a-half-second payment is
> idle by this definition, and is about to have its job taken.

---

## 3. The same bug in SQS, Kafka and RabbitMQ

Nothing above is a Redis quirk. Every queue has this mechanism under a different
name, and every one of them makes you guess the same number:

| | the lease | how you extend it | what happens when it expires |
| --- | --- | --- | --- |
| **SQS** | `VisibilityTimeout` (default 30 s) | `ChangeMessageVisibility` | the message becomes visible to other consumers |
| **Kafka** | `max.poll.interval.ms` (default 5 min) | return to `poll()` in time; heartbeats are a separate thread | the consumer is considered dead, the group **rebalances**, the partition is reassigned and its offsets are re-read |
| **RabbitMQ** | no timer by default; the ack lives as long as the channel | `consumer_timeout` (default 30 min) kills the channel | unacked messages are requeued to another consumer |
| **Redis Streams** | `min-idle-time` you pass to `XAUTOCLAIM` | `XCLAIM` with `min-idle-time 0` and `JUSTID` | another consumer may claim it |

Two differences worth knowing:

**Kafka's version is worse than it looks.** Exceeding `max.poll.interval.ms` does
not just redeliver one message — it ejects the consumer from the group and
triggers a rebalance, so every partition it held gets reassigned and every
uncommitted message on them is re-processed. One slow message can cause a
multi-partition duplicate storm. This is why the Kafka answer is usually to
reduce `max.poll.records` rather than to raise the interval.

**RabbitMQ's default of "no timeout" trades this bug for a different one.** A
consumer that hangs forever holds its message forever and nothing redelivers it.
The `consumer_timeout` that was added to address that reintroduces exactly the
guess in this episode.

There is no configuration of any of them that removes the guess.

---

## 4. Why raising the timeout is not a fix

The immediate reaction is to raise the number until it is comfortably above the
work:

```bash
VISIBILITY_TIMEOUT_MS=5000 docker compose up -d --force-recreate worker-1 worker-2
```

The duplicates disappear. Nothing has been fixed. Three reasons:

**You are guessing against a tail, not an average.** You sized it against your
p99. Your p999 exists, and a garbage collection pause, a slow DNS lookup or a
noisy neighbour will produce a job that outlives any timeout you pick.

**Raising it makes real failures slower to recover.** The timeout does double
duty: it bounds how long you wait for a slow worker *and* how long a genuinely
dead worker's messages sit unprocessed. Setting it to five minutes means a pod
that OOMs takes five minutes to have its work picked up. You are trading
duplicates for latency on the failure path, and both are real costs.

**It is hidden, not fixed** — which is how this reaches production in the first
place. Add one slow customer, or one GC pause, and the duplicates come back.

---

## 5. Heartbeating, and what it still cannot promise

The right *mitigation* is to hold the lease while the work runs, re-claiming the
entry so the idle clock resets:

```python
async def hold_lease(r, mid):
    period = max(VISIBILITY_TIMEOUT_MS / 2000, 0.2)
    while True:
        await asyncio.sleep(period)
        await r.xclaim(STREAM, GROUP, NAME, min_idle_time=0,
                       message_ids=[mid], justid=True)
```

Measured: duplicates went to zero, and it cost **43 lease extensions across 25
jobs**. That is a round trip per job per half-timeout, for the whole duration of
every job — worth knowing before you turn it on for a high-throughput consumer.

And it is still not the fix, because a lease you are holding can still be lost:

- the process can be killed (§6 measures exactly this)
- the network can partition, so your extension never arrives
- a stop-the-world garbage collection can exceed the timeout
- the container can be evicted mid-job

**Heartbeating lowers the rate. Only an idempotent consumer changes the
outcome.** That distinction — rate versus outcome — is the through-line of the
whole series.

---

## 6. Where the acknowledgement goes

Kill a worker and you have to answer a question the happy path let you avoid:
does the acknowledgement happen before the work, or after it?

That is not a style preference. **It is the entire delivery guarantee of your
system, and your queue library has already picked one for you.**

Five jobs, one worker that reads all five in a single batch, killed 1.5 seconds
into the first — a 3.5-second payment. Identical setup, identical kill, identical
moment; the only difference is which side of the work the `XACK` is on:

| | `ACK_MODE=after` | `ACK_MODE=before` |
| --- | --- | --- |
| | at-least-once | at-most-once |
| Pending after the kill | **5** | **0** |
| Deliveries | 6 | 1 |
| Jobs run twice | **1** | 0 |
| Jobs never attempted | 0 | **4** |
| Owed | $200 | $200 |
| Collected | **$240** | **$40** |

Ack after the work and a crash **duplicates**. Ack before it and a crash
**loses**: four jobs, $160 of work, acknowledged and never done.

And the queue is not hiding it — the queue has no idea. `pending=0`, `lag=0`,
depth flat. **Every dashboard is green, and it is green because it is telling the
truth. There is nothing left to process.**

In code it is two `XACK` calls eight lines apart. Before the work:

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

That second one is *not reached when the handler throws*, which is what turns a
failure into a redelivery rather than a silent drop.

**There is no third branch.** Not in Redis, not in SQS, not in Kafka. And
at-least-once is the right default for the reason
[Episode 1 §8](../episode-1-duplicates/GUIDE.md#8-why-not-simply-stop-retrying)
gives: duplicates you can solve, lost work you cannot.

Both runs leave one row in `job_runs` reading `started` with no `finished_at`.
That row is the only trace a killed worker leaves anywhere.

---

## 7. Batch size is where at-most-once does its damage

The measurement above needed a batch of five, and that is not staging — it is the
ordinary state of a queue with a backlog. Every queue client reads in batches:
SQS `MaxNumberOfMessages` (up to 10), Kafka `max.poll.records` (default 500),
RabbitMQ's consumer prefetch, `XREADGROUP COUNT`.

It matters because **a batch of one cannot show what at-most-once costs.**
Measured while building this demo: with the worker already running, the
producer's writes raced its blocking read, it was handed one message at a time,
and ack-before lost nothing at all — the four messages it had not read yet had
not been acknowledged either.

Two practical consequences:

- **Your duplicate/loss blast radius is your batch size**, not one message. A
  Kafka consumer with the default `max.poll.records=500` that acknowledges early
  can lose 500 messages to one crash.
- **Large batches interact badly with the lease.** The timeout usually applies
  per message, but your loop processes them serially, so message 500 has been
  sitting in the pending list for the duration of the other 499. This is the most
  common cause of surprise redelivery in Kafka consumers, and reducing
  `max.poll.records` is the standard fix.

---

## 8. The poison message

A payload the consumer rejects on sight and always will — here, `amount_cents:
-1`. Not a transient failure. No number of retries converts it into one.

With no delivery limit, watched for thirty seconds:

```
   t=+  0.0s  dead-letter depth 0   pending 1
   t=+ 10.1s  dead-letter depth 0   pending 1
   t=+ 20.2s  dead-letter depth 0   pending 1
   t=+ 29.3s  dead-letter depth 0   pending 1
```

**16 deliveries in 31.4 seconds** — one every two seconds, which is the
visibility timeout, forever. Nothing was reported. Nothing errored, from the
queue's point of view.

Now look at what the instrument panel says while that happens:

- `XLEN` reads **1**. A Redis Stream is a log: acknowledging does not shorten it,
  so a graph of "queue depth" is a horizontal line whether you are working or not.
- **lag** — entries never delivered — reads **0**. There is no work waiting.
- **pending** reads 1. That is the only number that knows.

The queue has depth, has a consumer, has throughput on every dashboard you own,
and is doing no work at all.

**A dead-letter queue at depth zero is not evidence that nothing is wrong.** It
is evidence that nothing has been *given up on*, which is a different claim.

The delivery count is the thing standing between a poison message and an infinite
loop, and most consumers never read it:

```python
rows = await r.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)
return int(rows[0]["times_delivered"])
```

Redis keeps that count itself — it is the queue's own answer, not a counter the
application maintains. SQS exposes the same thing as
`ApproximateReceiveCount`; Kafka does not track it at all, which is why
Kafka-based dead-lettering has to be built in the application.

---

## 9. A dead letter queue is an instrument, not a bin

Set a limit and give it somewhere to go:

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

The three good jobs published behind it were charged once each, $120 for $120 —
which is the other half of the argument. Without a limit, one bad message
occupies a worker forever and everything behind it waits.

**The threshold in this repo is one, deliberately.** A dead letter queue is not a
capacity problem, it is a correctness signal: one message in it is one piece of
work this system accepted, promised to do, and will never do. "Alert at depth
> 100" is a statement that you are willing to silently drop 100 pieces of work.

What belongs in it, and what does not:

| | goes to the DLQ | does not |
| --- | --- | --- |
| Malformed payload, unknown schema version | ✅ | |
| A referenced entity that does not exist | ✅ | |
| A bug in the consumer that throws on valid input | ✅ (and it will tell you) | |
| A downstream service that is down | | ❌ retry, with backoff — it will recover |
| A rate limit | | ❌ retry, with backoff |
| A transient network error | | ❌ retry |

Getting this wrong in the second direction is the common failure: a dependency
has a bad ten minutes, your delivery limit is 3, and you dead-letter thousands of
perfectly good messages that would have succeeded on the fourth attempt.

**Dead-letter with the evidence attached.** This repo writes the error and the
delivery count alongside the payload:

```python
await r.xadd(DLQ, {**fields, "failed_after_deliveries": delivery,
                   "error": f"{type(exc).__name__}: {exc}", "worker": NAME})
```

A message in a dead letter queue with no reason attached is a message somebody
has to reproduce from scratch. **A dead letter queue nobody alerts on is a bin.**

---

## 10. Redriving a dead letter queue safely

The half nobody documents. You fixed the bug; now what happens to the 4,000
messages you parked?

**Redrive is republishing**, which means everything in this series applies to it.
Specifically:

- **The messages must still carry their original keys.** If your redrive tool
  mints new ones, or your DLQ entry dropped the key field, you are about to
  re-run work that may already have partially succeeded — with no protection. The
  DLQ entry must preserve the whole original message, which is why this repo
  writes `{**fields, ...}` rather than just the error.
- **Some of them already succeeded.** A message dead-lettered after five
  deliveries may have completed on delivery three and failed to acknowledge. Only
  an idempotent consumer makes redrive safe, and this is the most common place
  people discover that the hard way.
- **Rate-limit the redrive.** Four thousand messages arriving at once, into a
  system already sized for steady state, is how a redrive becomes a second
  incident.
- **Check the schema is still one you handle.** Messages sitting in a DLQ for
  three weeks may predate a format change.

SQS has a built-in redrive; Kafka setups usually need a small tool. Either way
the checklist is the same, and item one is "is the consumer idempotent" — which
is §13.

---

## 11. Backoff, and retry topics

The demo redelivers immediately, every two seconds, forever. Real systems should
not.

For transient failures you want **exponential backoff with jitter** between
attempts, for the same reason
[Episode 1 §7](../episode-1-duplicates/GUIDE.md#7-how-to-configure-a-retry-that-is-not-a-duplicate-generator)
wants it in an HTTP client: fixed-interval retries synchronise across your fleet
and turn a slow dependency into an outage.

Most brokers make this awkward, because the redelivery timer is the visibility
timeout and it is one number:

- **SQS** lets you set per-message visibility on failure
  (`ChangeMessageVisibility` with an increasing value), which is the cleanest fit.
- **Kafka has no per-message delay at all.** The standard pattern is **retry
  topics**: on failure, produce the message to `orders.retry.5s`, then
  `orders.retry.1m`, then `orders.retry.10m`, each consumed by a consumer that
  sleeps or is scheduled accordingly, with the DLQ at the end of the chain. It is
  more machinery than it sounds and it is the accepted answer.
- **RabbitMQ** does it with a dead-letter exchange and a per-queue message TTL —
  a message expires out of a delay queue and is routed back to the work queue.

Whichever you use, note that a retry topic **breaks ordering by design** (§12)
and that each hop is another republish, so each hop needs the key to survive it.

---

## 12. Retries break ordering

Ten jobs, published in order. Number five fails once, goes back, and comes around
again:

```
published   1  2  3  4  5  6  7  8  9  10
completed   1  2  3  4  6  7  8  9  10  5
```

**Five pairs finished out of order**, and the retried message finished tenth of
ten — behind five messages published after it. Nothing errored. Nothing was
logged.

If any consumer downstream assumed ordering, it does not hold any more.

The general rule: **ordering and retries are in tension, and you can have strict
ordering only by accepting head-of-line blocking.** If message five must be
processed before six, then a failure of five must stop six — which means one bad
message halts the partition. That is the trade Kafka makes you choose explicitly
by processing a partition serially, and it is why dead-lettering exists at all:
dead-lettering is how you *break* ordering deliberately in order to keep moving.

Practical guidance:

- **Do not assume global ordering.** You almost never had it.
- **Key related work onto the same partition** if you need per-entity ordering,
  and design the consumer so that out-of-order arrival within an entity is
  detectable (a version number, an updated-at) rather than silently wrong.
- **Prefer commutative operations.** "Set status to shipped" survives reordering;
  "increment balance" does not.

---

## 13. The fix, and why the key has to come from the producer

Three bugs, three fixes, and not one of them is *the* fix, because every one of
them only lowers a probability. The heartbeat makes an expiring lease less
likely, not impossible. Acknowledging after the work guarantees you will
sometimes duplicate. The delivery limit stops an infinite loop after five
perfectly good attempts at the same work.

You cannot make a queue deliver exactly once. So stop trying, and **make the
second delivery cost nothing** instead.

The change is one line: the worker passes the producer's key to Episode 2's
endpoint.

Same twenty-five jobs, same two workers, same two-second lease against the same
slow payments, nothing about the queue touched:

```
 seq | customer_id | deliveries |       outcomes
-----+-------------+------------+-----------------------
  12 |          12 |          2 | charged then replayed
  14 |          14 |          2 | charged then replayed
  16 |          16 |          2 | charged then replayed
  17 |          17 |          2 | charged then replayed
```

**29 deliveries** for 25 jobs. Four still handed to a second worker off an
expired lease, exactly as before. **25 charges. $1,000 owed, $1,000 collected.**

> **The redelivery did not stop. The second charge did.**

And one detail matters enormously: **that key is minted by the producer and
travels inside the message.**

```python
"key": f"k_{uuid.uuid4().hex[:12]}",
```

It cannot come from the worker. **A worker cannot tell delivery two from delivery
one** — that is the entire problem — so a key it invents for itself is a brand
new key every time and protects nothing. Only the thing that decided this work
should happen knows that all these deliveries are one intent.

Three follow-ups the video did not have room for:

**What if the producer does not set one?** Derive it from something stable in the
payload: an order id, an `(entity, operation, version)` triple. Anything that is
a function of the intent rather than of the delivery. Do **not** use the message
id (§below) and do not use a hash of a payload that contains a timestamp.

**Why not the message id?** It is tempting, and it is not enough. It works for
redelivery of the *same* message, and it fails the moment the producer publishes
the same intent twice — two message ids, one intent, and no consumer can tell.
That is [Episode 4](../episode-4-outbox/), and it measures exactly that: a relay
republishing three outbox rows under six message ids.

**Where should the dedupe live?** Here it is the downstream endpoint's
`idempotency_keys` table, which is ideal because it is the same transaction
boundary as the effect. If your side effect has no such table, the consumer needs
its own — a `processed_messages` table with the key as primary key, written **in
the same transaction as the work**. If they are separate transactions you have
recreated the dual write, which is, again, Episode 4.

---

## 14. What to alert on

Queue monitoring is usually wrong in a specific way: people graph depth. Depth is
a capacity signal. None of the three bugs in this episode moves it.

| Signal | What it catches | Why depth does not |
| --- | --- | --- |
| **age of the oldest unacknowledged message** | a stalled consumer, a poison message looping, a dead worker holding a lease | depth is flat while one message is redelivered forever |
| **dead-letter depth, threshold 1** | work accepted and never done | it is zero until somebody sets a delivery limit |
| **redelivery rate** (`times_delivered > 1`) | a visibility timeout set below the work | invisible in every other metric — the lease scenario had 32 deliveries for 25 jobs and looked healthy |
| **lag vs pending, separately** | "work waiting" vs "work claimed by somebody who may never finish" | collapsing them hides a dead worker entirely |

The `job_runs` table in this repo exists for exactly this reason: it records one
row per **delivery**, not one per job. Money alone cannot tell "the queue stopped
redelivering" from "the queue redelivered and it stopped mattering", and those
are very different systems. In the final scenario it redelivered.

If you take one metric from this section, take the first one. **Oldest
unacknowledged age is the number that would have caught every scenario in this
episode**, and almost nobody graphs it.

---

## 15. Exercises

**1. Hide the bug.**

```bash
VISIBILITY_TIMEOUT_MS=5000 docker compose up -d --force-recreate worker-1 worker-2
python3 scripts/enqueue.py --customers $(seq 1 25)
```

Duplicates vanish. Nothing was fixed — §4.

**2. Kill a worker each way.**

```bash
docker compose kill worker-1        # 1.5s into a 3.5s job
python3 scripts/queue-state.py --label after-the-kill
```

Run it with `ACK_MODE=after` and then `ACK_MODE=before`, and compare `pending`
immediately after the kill. Five versus zero.

**3. Watch nothing happen.**

```bash
python3 scripts/enqueue.py --poison
python3 scripts/dlq-watch.py --seconds 30
```

Thirty seconds of a green dashboard. Then set `MAX_DELIVERIES=5` and watch
something finally go off.

**4. Turn on the fix.**

```bash
IDEMPOTENT_CONSUMER=1 docker compose up -d --force-recreate worker-1 worker-2
```

Then check `job_runs` and confirm the redeliveries are *still there*. That is the
point.

**5. Break redrive.** Modify `on_failure` to write only the payload to the DLQ,
dropping the `key` field, then dead-letter a message and redrive it. Watch the
second charge that §10 warns about.

---

## Where to go next

Everything fixed in this episode is on the **consumer** side. The producer in
this repo is one `XADD` and an exit, and it has never been asked what happens if
it crashes between writing to the database and publishing to the stream — or if
it retries the publish and the same intent goes on twice under two different
message ids.

No queue can fix that for you. That is
[Episode 4](../episode-4-outbox/GUIDE.md).

- [Episode 1 — where duplicates come from](../episode-1-duplicates/GUIDE.md)
- [Episode 2 — idempotency keys](../episode-2-keys/GUIDE.md)

---

Part of the **System Sense — Idempotency** mini-series.
