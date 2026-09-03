# The Dual Write, the Transactional Outbox, and Exactly-Once

**A written companion to Episode 4 of System Sense — [Exactly-Once Is a Lie](../).**

The video is 18 minutes and had to leave things out. This does not. It covers
the same ground more slowly, with the code in full, and then keeps going into
the questions the runtime budget cut: running more than one relay, what happens
to the table as it grows, what the pattern says about *ordering*, and why
two-phase commit is not the answer it looks like.

Every figure here comes from `capture/metrics.json`, produced by
`./scripts/capture-demo.sh` in this folder. Nothing is estimated. If you clone
this and run it, the counts reproduce exactly — the kills are deterministic on
the order's sequence number — and only the relay's latency depends on your disk.

**Who this is for:** you have a service that writes to a database and publishes
to a queue, and you have never been entirely sure what happens if it dies in
between. By the end you will know exactly what happens, why no amount of care in
that handler prevents it, and what the fix actually promises — which is less than
it is usually sold as, and enough.

---

## Contents

1. [The failure, in one command](#1-the-failure-in-one-command)
2. [Why it happens: the dual write](#2-why-it-happens-the-dual-write)
3. [Both orderings are wrong, and one is worse](#3-both-orderings-are-wrong-and-one-is-worse)
4. [Why a retry cannot reach it](#4-why-a-retry-cannot-reach-it)
5. [Why no queue can fix it either](#5-why-no-queue-can-fix-it-either)
6. [Delivery versus effects](#6-delivery-versus-effects)
7. [The transactional outbox](#7-the-transactional-outbox)
8. [What the outbox costs](#8-what-the-outbox-costs)
9. [The relay has the same bug, and it does not matter](#9-the-relay-has-the-same-bug-and-it-does-not-matter)
10. [Running more than one relay](#10-running-more-than-one-relay)
11. [The table grows](#11-the-table-grows)
12. [What this says about ordering](#12-what-this-says-about-ordering)
13. [Why not two-phase commit](#13-why-not-two-phase-commit)
14. [Change data capture, as the alternative](#14-change-data-capture-as-the-alternative)
15. [What Kafka's exactly-once semantics covers](#15-what-kafkas-exactly-once-semantics-covers)
16. [When not to use an outbox](#16-when-not-to-use-an-outbox)
17. [What to monitor](#17-what-to-monitor)
18. [The same problem in agent workflows](#18-the-same-problem-in-agent-workflows)
19. [Exercises](#19-exercises)

---

## 1. The failure, in one command

```bash
PUBLISH_MODE=commit_first CRASH_EVERY=4 docker compose up --build
python3 scripts/place-orders.py --customers $(seq 1 12)
python3 scripts/reconcile.py --label the-damage
```

Twelve orders, $40 each. $480 owed.

```
-- the-damage
   orders committed          12
   events on the stream       9   (9 distinct keys)
   events lost                3   <- committed, never published
   phantom events             0   <- published, never committed
   duplicate publishes        0   <- one intent, two message ids
   charges                    9
   owed  $    480.00
   taken $    360.00
   -- orders nothing will ever pay for
      seq=4   customer 4   $  40.00  key=k_a0a816804a27
      seq=8   customer 8   $  40.00  key=k_641880d7b1d1
      seq=12  customer 12  $  40.00  key=k_d29fa7f1150c
```

Three orders are in the database, looking completely normal, and nothing will
ever charge for them. Not late — never. Nothing retries, because nothing knows
there is anything to retry.

Note what the customer saw: the request that died with the process returned no
response at all. From outside, that is indistinguishable from the order never
having been placed. It *was* placed. It just will not be paid for.

**And nothing in this system is misconfigured.** The HTTP endpoint is Episode
2's, which cannot be made to charge twice for one key. The worker is Episode 3's,
with its lease held, a delivery limit set, and the producer's key passed on. Both
are byte-identical to what those episodes shipped:

```bash
diff -r ../episode-3-queues/app app        # no output
diff -r ../episode-3-queues/worker worker  # no output
```

Everything lost here is lost in front of them.

---

## 2. Why it happens: the dual write

Here is the entire handler that lost the money. Nine lines, from
[`producer/main.py`](producer/main.py):

```python
key = f"k_{uuid.uuid4().hex[:12]}"
fields = event_fields(o, key)

if PUBLISH_MODE == "commit_first":
    async with state["db"].acquire() as con:
        order_id = await con.fetchval(INSERT_ORDER, o.seq, o.customer_id,
                                      o.amount_cents, key)
    maybe_crash(o, "after COMMIT, before publish")
    mid = await publish(fields)
    return {"order_id": order_id, "key": key, "message_id": mid}
```

Read it for a bug. There is no race. There is no missing retry. There is no
error being swallowed. It writes the row, then tells the world, which is the
right order — you do not announce an order you have not stored.

The problem is structural. An order becoming real requires two facts to become
true:

| Fact | Where it lives |
| --- | --- |
| a row in `orders` | PostgreSQL |
| an event on `checkouts` | Redis |

There is a transaction around the database write. **There is no transaction that
can span both**, because the second system is a different process on a different
machine that has never heard of your database. So there is a moment, between
line 7 and line 8, in which one fact is true and the other is not.

This is called the **dual write**, and it has nothing to do with Redis. Swap in
Kafka, SQS, RabbitMQ, an HTTP webhook, a Stripe call, an email — anything that is
not the database you just committed to. The moment is still there.

`CRASH_EVERY=4` puts a real `os._exit(1)` in that moment on every fourth order.
No unwinding, no shutdown hook, nothing flushed — which is what a `kill -9`, an
OOM kill, a rolling deploy and a reclaimed spot instance all look like from
inside the process:

```
[orders] seq=3  customer=3  order=3 COMMITTED -> PUBLISHED 1788436266442-0
[orders] seq=4  customer=4  *** KILLED after COMMIT, before publish ***
[orders] seq=5  customer=5  order=5 COMMITTED -> PUBLISHED 1788436266966-0
```

Docker had the service answering again 524 ms later. That is the part that makes
this survive contact with production: the graph shows a blip, the site is up, and
the event is gone.

---

## 3. Both orderings are wrong, and one is worse

The first thing everybody reaches for is to swap the two lines. Publish first,
then commit. It feels safer, and it is worth noticing why it feels safer: we are
all much better at imagining lost work than at imagining work that should never
have happened.

Same twelve orders. Same three kills, at the same three sequence numbers, in the
same place in the code:

| | orders committed | events published | lost | phantom | owed | taken |
| --- | --- | --- | --- | --- | --- | --- |
| `COMMIT`, then publish | 12 | 9 | **3** | 0 | $480 | **$360** |
| publish, then `COMMIT` | **9** | 12 | 0 | **3** | $360 | **$480** |

The second row is worse, and not by a little.

- A **lost event** costs you revenue and gives you a confused customer. Bad.
- A **phantom event** is a payment your own database cannot explain. Three
  customers were charged $40 each for an order that was never written down.
  Support has a payment and nothing to attach it to. That is not a lost sale, it
  is a chargeback, and possibly a regulator.

There is a second hazard in publish-first that the numbers above do not show: the
consumer can pick the event up and act on it *before* the producer's transaction
commits. Downstream then reads its own database and does not find the order it
was just told about — a race that reads as a flaky, unreproducible bug rather
than as the design flaw it is.

**Swapping the lines did not close the window. It moved it.**

---

## 4. Why a retry cannot reach it

The next instinct is to make the publish reliable: wrap it, retry it, put it in a
`finally`, add a shutdown hook, buffer it.

None of those run.

The process is not *handling an error*. The process is **gone**. There is no
stack, no `except`, no `finally`, no atexit hook, no flush. Whatever you wrapped
that publish in, you wrapped it in code that is no longer running.

This is the single most important thing to internalise about the dual write, and
it is why "just add a retry" is not a smaller version of the fix. There is no
version of that handler, written more carefully, that survives being killed
between two lines. The fix cannot live in the handler.

---

## 5. Why no queue can fix it either

Fine — make the *broker* guarantee delivery. Acknowledgements, confirmations, a
publisher-confirms mode. That is what queues are for.

It does not close either, and the reason is a proof rather than an engineering
gap.

Two clerks, in two buildings. One has the order, the other has the money, and
they need to *both* end up certain the payment was made. Between them is a
courier who sometimes loses an envelope.

Clerk one sends the order. He cannot act on it, because he does not know it
arrived. So clerk two sends back a receipt — and now she cannot act either,
because she does not know the receipt arrived, and he will not move without it.
So he confirms the receipt. And now he is waiting again, because that
confirmation could have been the one that was lost.

Add another envelope, and another. It never closes. **Whatever the last message
in your protocol is, the sender of it does not know it arrived, and the whole
scheme was resting on that.**

This is the **two generals problem**. The result dates to 1975 (Akkoyunlu,
Ekanadham and Huber); Jim Gray gave it the name a few years later. It says that
two parties communicating over a channel that can lose messages cannot both
become certain, ever, with any finite number of messages.

So exactly-once *delivery* is not hard. It is impossible. Kafka cannot give it to
you. SQS cannot. Redis cannot. Nothing can, and nothing ever will.

---

## 6. Delivery versus effects

Here is the sentence the whole series turns on, and it turns on one word:

> Exactly-once **delivery** is impossible. Exactly-once **effects** are not.

| | what it counts |
| --- | --- |
| delivery | how many times the message arrives |
| effects | how many times the money moves |

You have never cared about the first one. Nobody has ever been fired because a
message arrived twice. What you care about is that the customer is charged once.

And exactly-once effects has a definition that fits on one line:

> **at-least-once delivery + a consumer that can be called twice without doing
> the work twice**

That is all it has ever been, from anybody, including the systems that market
themselves as exactly-once.

Episodes 1 through 3 of this series built the second half of that sentence:

| Episode | The bug | The fix |
| --- | --- | --- |
| [1](../episode-1-duplicates/) | A timeout is not a failure — the work happened, only the answer was lost | know where duplicates come from |
| [2](../episode-2-keys/) | Check-then-insert is a race | claim the key first; let the database arbitrate; replay the stored response |
| [3](../episode-3-queues/) | A queue redelivers on a schedule nobody wrote down | make the second delivery cost nothing |

Which leaves the first half: **at-least-once delivery**. And sections 1 to 4
showed that we do not have it — the event was never published at all.

---

## 7. The transactional outbox

You have exactly one thing in this system with a real guarantee attached to it,
and it is the database transaction. So put the event inside it.

```sql
CREATE TABLE outbox (
    id               BIGSERIAL PRIMARY KEY,
    order_id         BIGINT      NOT NULL REFERENCES orders(id),
    topic            TEXT        NOT NULL,
    payload          TEXT        NOT NULL,
    publish_attempts INT         NOT NULL DEFAULT 0,
    message_id       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ
);

-- The relay's only query, and the index that makes it cheap. An outbox that is
-- keeping up is nearly empty by this index's reckoning, however large the table.
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE published_at IS NULL;
```

and the handler becomes:

```python
async with state["db"].acquire() as con:
    async with con.transaction():
        order_id = await con.fetchval(INSERT_ORDER, o.seq, o.customer_id,
                                      o.amount_cents, key)
        outbox_id = await con.fetchval(INSERT_OUTBOX, order_id, STREAM,
                                       json.dumps(fields, separators=(",", ":")))
maybe_crash(o, "after COMMIT, before publish")
return {"order_id": order_id, "key": key, "outbox_id": outbox_id}
```

**One commit. Both facts. One system.**

It is not a clever pattern. There is nothing to tune and nothing to get subtly
wrong. It is the observation that if you cannot write to two systems atomically,
you write to one system twice.

Same twelve orders, same kill switch, still firing on orders 4, 8 and 12, in the
same place in the code:

```
   orders committed          12
   events on the stream      12   (12 distinct keys)
   events lost                0
   phantom events             0
   owed  $    480.00
   taken $    480.00
```

The kill still fired three times. There is simply nothing left for it to land in.
Either the transaction committed and both rows are there, or it did not and
neither is. There is no third state — **and that is a property of the
transaction, not of how carefully the handler was written.** That distinction is
the whole reason this works when "add a retry" does not.

`payload` is `TEXT` rather than `JSONB` on purpose, for the same reason Episode
2's stored response body is: JSONB parses and re-serialises, so what comes back
out is the same *values* in a different arrangement. Store the bytes you meant to
send.

---

## 8. What the outbox costs

The event is no longer published inside the request. Something else publishes it
afterwards, from the table. In this repo that is [`relay/main.py`](relay/main.py),
49 lines:

```python
rows = await con.fetch(UNSENT, RELAY_BATCH)     # WHERE published_at IS NULL
for row in rows:
    attempts = await con.fetchval(ATTEMPT, row["id"])          # attempts += 1
    mid = await state["redis"].xadd(row["topic"], fields)      # publish
    await con.execute(SENT, row["id"], mid)                    # mark sent
```

That delay is the honest price of the pattern, so measure it rather than wave at
it. Across the twelve rows here:

| | |
| --- | --- |
| median, commit to publish | **131.3 ms** |
| worst, of twelve rows | **169.6 ms** |

Most of that is the poll interval (`POLL_MS=200` by default), not work. If you
need it lower, the standard move is to have the producer nudge the relay after
commit — a `NOTIFY`, a channel send, anything — while leaving the poll in place
as the thing that actually guarantees delivery. The nudge is an optimisation and
must never be the mechanism, because a lost nudge is exactly the failure this
whole pattern exists to remove.

You also get something back that is easy to miss: the outbox is a durable,
ordered, queryable record of every event your system ever intended to emit,
sitting in your own database with your own tooling pointed at it. "Was the event
sent?" becomes a `SELECT` instead of an argument.

---

## 9. The relay has the same bug, and it does not matter

Look again at what the relay does. It publishes the event, then marks the row
sent. A write to Redis and a write to Postgres, with nothing around them.

That is the same bug. It cannot *not* be the same bug.

So kill it in the same place — publish, then die, before the row is marked:

```
[relay] outbox=1 seq=1 customer=1 key=k_efed0395701d PUBLISHED 1788436366875-0 attempt=1
[relay] outbox=1 *** KILLED after publish, before marking sent ***
[relay] outbox=1 seq=1 customer=1 key=k_efed0395701d PUBLISHED 1788436367105-0 attempt=2
```

When it comes back, that row still says unpublished — because as far as the
database is concerned, it is. So the relay publishes it again.

**One order. One key. Two message ids.**

Hold on to that, because it is why Episode 3's answer was not enough by itself. A
consumer that deduplicates on the message id cannot see this. Those are
genuinely two different messages, and they mean one thing. Only a key that
travels *with the intent* can tell.

Twelve orders, relay killed three times, fifteen events on the stream. The only
difference between these two runs is one line of consumer config:

| twelve orders, relay killed 3× | events | published twice | charges | taken |
| --- | --- | --- | --- | --- |
| Episode 2's key **off** | 15 | **3** | **15** | **$600** |
| Episode 2's key **on** | 15 | **3** | **12** | **$480** |

```
 seq | customer_id | deliveries |       outcomes
-----+-------------+------------+-----------------------
   1 |           1 |          2 | charged then replayed
   2 |           2 |          2 | charged then replayed
   3 |           3 |          2 | charged then replayed
```

**The duplicate publishing did not stop. The second charge did.**

So here is what the outbox actually promises, stated precisely, because the
pattern is routinely oversold:

> **The outbox does not eliminate duplicates. It eliminates loss.**

It takes a failure you cannot recover from — an event that no longer exists
anywhere — and turns it into one you can: the same event arriving twice. That is
only a good trade because the consumer is idempotent. **If your consumer is not
idempotent, an outbox has moved your problem, not solved it.**

Run everything broken at once — producer killed on every fourth order, relay
killed three times before it could mark a row sent, six process deaths in one
run — and the books still balance: 12 orders, 15 events, 12 charges, $480 owed
and $480 taken.

---

## 10. Running more than one relay

The relay in this repo is deliberately a single process, because duplicates are
the point of the demo. In production you will want more than one, for
availability if not for throughput, and two relays reading the same query will
publish the same row at the same instant.

The standard fix is row-level locking with skipping:

```sql
SELECT id, topic, payload
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED;
```

`FOR UPDATE` takes a row lock; `SKIP LOCKED` tells the second relay to step over
rows another transaction already holds rather than block on them. Each relay gets
a disjoint batch, and neither waits.

Two things to be careful about, both of which this repo hit:

- **The rows must be locked and marked in the same transaction that publishes
  them**, or the lock buys you nothing — but the *attempt counter* must commit
  separately, before the publish. This repo does exactly that, and the comment in
  `relay/main.py` says why: if the counter were only committed at the end of the
  pass, a process killed mid-pass would roll it back and the row would come back
  looking untouched. That is the dual write again, one level down, and it would
  make the relay lie about its own history.
- **`SKIP LOCKED` does not make duplicates impossible.** Nothing does. It stops
  them being the ordinary case; the crash window in section 9 is still there.

---

## 11. The table grows

An outbox row is written for every event your system emits, forever, and nothing
in the pattern deletes them. Left alone this table becomes the largest one you
own.

Two workable approaches:

- **Delete after a retention window.** A periodic
  `DELETE FROM outbox WHERE published_at < now() - interval '7 days'` is enough
  for most systems. Keep a window rather than deleting on publish: the ability to
  answer "did we send that event, and when" is half of what the table is worth,
  and you cannot answer it from a table you emptied. Watch for bloat — a
  high-churn table wants an aggressive `autovacuum_vacuum_scale_factor`.
- **Partition by day and drop partitions.** `DROP TABLE` on an old partition is
  instant and produces no bloat, where a large `DELETE` produces plenty. More
  setup, and it is the right answer at high volume.

Either way, the partial index (`WHERE published_at IS NULL`) is what keeps the
relay's query fast regardless of how large the table gets. That index is the one
piece of this schema that is not optional.

---

## 12. What this says about ordering

The outbox gives you a durable, totally-ordered log of intents in one database.
That is a genuine gain, and it is *not* the same as ordered delivery.

What actually holds:

- **A single relay reading `ORDER BY id` and publishing serially preserves commit
  order.** That is the design in this repo.
- **Add a second relay and you lose it.** `SKIP LOCKED` hands out disjoint
  batches with no coordination between them; a later row can be published first.
- **Retries reorder anyway.** Episode 3 measured this on the consumer side: ten
  jobs published in order with one failing once finished `1 2 3 4 6 7 8 9 10 5`.
  The same is true of a republished outbox row.
- **Partitioned brokers order per partition, not globally.** Kafka orders within
  a partition; a Redis stream is a single log but consumer groups hand entries to
  workers concurrently.

There is a subtler trap that this repo's design happens to avoid, and it is worth
knowing because a common variant walks straight into it. If your relay tracks a
cursor — `WHERE id > last_seen_id` — you will eventually skip rows. `BIGSERIAL`
values are handed out by a **non-transactional sequence**, so a transaction can
be assigned id 5 and commit *after* one assigned id 6. A cursor that has seen 6
will never look at 5 again, and that event is lost — silently, and only under
concurrency.

Reading `WHERE published_at IS NULL` instead, as this repo does, is immune: the
row is invisible to the relay until it commits, and once it commits it is
selected on the next pass whatever its id. If you take one implementation detail
from this document, take that one.

**If you need ordering, get it from the consumer**, by keying work so that
related events land on the same partition and the consumer can reason about
sequence itself. Do not get it from the outbox.

---

## 13. Why not two-phase commit

2PC is the textbook answer to "atomically update two systems", and it is real:
a coordinator asks every participant to *prepare*, each votes, and if all vote
yes it tells them to commit.

It is not the answer here, for four reasons:

1. **Your broker probably is not a participant.** 2PC needs every system to
   implement a prepare/commit protocol as an XA resource manager. Postgres has
   `PREPARE TRANSACTION`. Kafka, Redis, SQS and every HTTP API you call do not.
   You cannot enlist Stripe in a two-phase commit.
2. **It holds locks across the network round trip.** A prepared transaction keeps
   its row locks from prepare until commit, so your database's contention now
   depends on how fast a different machine answers.
3. **The coordinator is a new single point of failure.** If it dies between
   prepare and commit, participants sit in-doubt holding locks until someone
   resolves them by hand. In Postgres an abandoned prepared transaction also
   blocks vacuum, which turns a blip into an outage over hours.
4. **It does not actually escape the proof.** 2PC is a blocking protocol
   precisely because the two generals result still applies to the coordinator's
   final message.

The outbox trades all of that away for one property — "the event might go out
twice" — which you have already paid for by making the consumer idempotent.

---

## 14. Change data capture, as the alternative

There is a version of this with no relay to run. Instead of reading a table, you
read the database's own write-ahead log — the sequential record it already keeps
of every change — and publish from that. This is **change data capture**:
Debezium, or Postgres logical decoding directly.

Same guarantee, and the trade cuts both ways:

| | outbox + relay | CDC / log tailing |
| --- | --- | --- |
| extra process to run | yes, ~50 lines | no relay, but a log reader to operate |
| what the consumer sees | an event you designed | a row change, or an event you designed if you still write an outbox table |
| coupling | your event schema | your **table** schema, unless you tail an outbox table |
| ordering | commit order, one relay | commit order from the WAL |
| delivery | at-least-once | at-least-once |

Two things people get wrong about CDC:

- **It is still at-least-once.** Something has to record how far it got — the
  replication slot's confirmed LSN — and that record is not written in the same
  transaction as the send. Same window, same outcome.
- **Tailing your business tables makes your schema a public API.** A column
  rename becomes a downstream incident. The usual fix is CDC *pointed at an
  outbox table*, which gets you the operational benefit while keeping the event
  contract explicit. That is the Debezium outbox event router pattern, and it is
  worth knowing that the two approaches converge.

Also worth budgeting for: a replication slot that no consumer is draining will
retain WAL segments until the disk fills. A stalled CDC pipeline can take the
primary down, which a stalled relay cannot.

---

## 15. What Kafka's exactly-once semantics covers

It is real, it is well built, it is genuinely useful, and it is much narrower
than its name.

Two things are in the box:

- **The idempotent producer.** Every producer gets an id, every message a
  sequence number per partition. If a `produce` call is retried, the broker
  recognises the sequence number and drops the duplicate. This removes duplicates
  caused by *producer retries*, which is a real and annoying class of them.
- **Transactions.** You can consume from one topic, transform, produce to
  another, and commit your read position in the same transaction as the output.
  Either both land or neither does. That is the consume-transform-produce
  pattern, and it is a hard problem solved properly.

**And every one of those guarantees stops at the edge of Kafka.**

Your call to Stripe is not in Kafka's transaction. Your write to Postgres is not
in Kafka's transaction. The email you sent is not in Kafka's transaction. Read a
message, charge a customer, commit the offset: Kafka will make that offset commit
atomic with a write *to Kafka*, and it has nothing whatsoever to say about the
$40.

> **Exactly-once inside the log is not exactly-once in your system.**

If your pipeline is Kafka-in and Kafka-out, EOS genuinely does the job and you
should use it. The moment a side effect leaves Kafka — a payment, a row in
another database, an email, an HTTP call — you are back to at-least-once plus an
idempotent consumer, and the outbox is how you get the first half.

---

## 16. When not to use an outbox

The pattern is cheap but it is not free, and there are cases where it is the
wrong tool:

- **The consumer is not idempotent and cannot be made so.** Then the outbox has
  moved your problem. Fix the consumer first; that is Episodes 2 and 3.
- **You do not actually need durability.** A cache invalidation, a metric, a
  presence ping — losing one costs nothing and the added latency and table are
  pure overhead. Publish directly and move on.
- **The write is not in a transaction at all.** If there is no business row being
  written, there is nothing to be atomic *with*, and an outbox is just a queue in
  front of a queue.
- **You need the event delivered in single-digit milliseconds.** The relay's poll
  interval is a floor. A commit-time nudge lowers it, but if your budget is
  genuinely that tight, this pattern is not the shape of your problem.
- **Your database cannot take the write volume.** Every event is now an insert on
  your primary. At very high fan-out this is a real cost, and it is where CDC or
  a dedicated event store starts to win.

---

## 17. What to monitor

The outbox is unusually easy to observe, which is another quiet benefit. Three
things are worth an alert:

| Signal | Query | Why |
| --- | --- | --- |
| **oldest unpublished row** | `SELECT now() - min(created_at) FROM outbox WHERE published_at IS NULL` | The one number that says the relay has stopped. Depth alone does not — a burst looks the same as a stall. |
| **unpublished depth** | `SELECT count(*) FROM outbox WHERE published_at IS NULL` | Capacity, not correctness. Useful as a trend. |
| **rows published more than once** | `SELECT count(*) FROM outbox WHERE publish_attempts > 1` | Not an error — this is the pattern working. Worth graphing so you know your real duplicate rate, and so a *spike* tells you the relay is crash-looping. |

And a reconciliation, which this repo ships as
[`scripts/reconcile.py`](scripts/reconcile.py): the database's orders beside the
queue's actual contents, counting events lost, phantom events, and one intent
published under two message ids.

Be honest about what that script is, though. **Needing a reconciler is the
diagnosis, not the cure.** You cannot write a reconciler for a failure whose
whole nature is that neither side knows it happened. The only reason this one
works is that the key is on both sides of it — which is the fix.

---

## 18. The same problem in agent workflows

If you are building on agents you have reached for durable execution, or a
framework is quietly doing it for you: steps, checkpointed, so that when
something falls over the run resumes instead of starting again.

Resuming means **replaying** the steps already taken, which means every side
effect in that workflow is about to be attempted a second time. That is this
entire series, wearing a costume.

One thing about it is genuinely new, and it breaks the first fix everybody
reaches for. **The model's output is not deterministic.** So the obvious move —
hash the step's payload, skip it if you have seen that hash — cannot work. Not
works badly: cannot work. The step produces a different string every time, so
the hash is different every time, so every replay looks like a brand new intent.

Four workflow runs, each replayed three times. Twelve attempts, and the stub
model returned twelve different strings:

| twelve attempts, keyed on | charged | replayed | owed | taken |
| --- | --- | --- | --- | --- |
| the payload (content) | **12** | 0 | $160 | **$480** |
| `(run_id, step_index, action_type)` | **4** | **8** | $160 | **$160** |

So do not key on *what* the step produced. Key on **where the step is**:

```
(run_id, step_index, action_type)
```

That triple is fixed by the shape of the workflow before the model is ever
called, and it is identical on every replay — which is exactly what Episode 2
needed a key to be. It is Episode 2's idempotency key, derived from position
instead of from content.

Two practical notes:

- **The key must survive a change to the workflow.** If `step_index` shifts
  because somebody inserted a step, every downstream key changes and the replay
  charges again. Use a stable step *name* rather than an ordinal if your
  framework gives you one.
- **There is no real model in this repository, on purpose.** A real call costs
  money and returns something different every time, and every number in this
  series has to be reproducible by anyone who clones it. The stub in
  [`scripts/agent-run.py`](scripts/agent-run.py) does the one thing that matters:
  it returns a different string on every call.

---

## 19. Exercises

Everything below runs from this folder.

**1. Watch the bug hide.** Set `CRASH_EVERY=0` and run the commit-first load
again. Nothing is lost. Nothing was fixed either — this is where most systems
are, right up until a deploy.

**2. Make the phantom worse.** Run `publish_first` and then look for the three
charged customers in the `orders` table. They are not there. Now imagine the
support ticket.

**3. Break the relay harder.** Raise `CRASH_AFTER_PUBLISH` to 6 with
`IDEMPOTENT_CONSUMER=0` and watch the over-collection scale linearly. Then set
`IDEMPOTENT_CONSUMER=1` and watch the duplicate publishes stay exactly where they
were while the money stops moving.

**4. Add a second relay.** Copy the `relay` service in `docker-compose.yml`,
give it a different `RELAY_NAME`, and run both. Then add `FOR UPDATE SKIP LOCKED`
to the `UNSENT` query in `relay/main.py` and compare the duplicate counts before
and after.

**5. Reproduce the sequence-gap bug from section 12.** Change the relay to track
a cursor (`WHERE id > $1`) instead of `WHERE published_at IS NULL`, then place
orders concurrently rather than one at a time. Watch `reconcile.py` report lost
events with nothing killed at all.

---

## Where to go next

- The other three episodes, each with a runnable demo:
  [1 — duplicates](../episode-1-duplicates/) ·
  [2 — idempotency keys](../episode-2-keys/) ·
  [3 — queues](../episode-3-queues/)
- The video for this episode, and the full playlist, are linked from the
  [root README](../README.md).
- Stripe's idempotency documentation, for the reference implementation of the
  consumer half.
- The Debezium outbox event router, if section 14 sounded like your problem.

---

Part of the **System Sense — Idempotency** mini-series.
