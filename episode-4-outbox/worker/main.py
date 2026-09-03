"""System Sense — Idempotency Ep.3: the consumer, and the two lines of it that
decide what your queue actually guarantees.

Run it:      docker compose up --build
Watch it:    docker compose logs -f worker-1 worker-2

Episode 2 ended with an HTTP endpoint that cannot be made to charge twice. That
endpoint is still here, unchanged, in `app/main.py`. What changed is who decides
when to retry.

A client that retries is making a choice you can see in its code. A queue
redelivers on a schedule nobody wrote down, for reasons that are not failures,
to a worker that has no idea it is the second one. This file is that worker.

Redis Streams consumer groups are used because they have real visibility-timeout
semantics rather than an imitation of them:

    XADD        the producer puts work on the stream
    XREADGROUP  a consumer takes work and it enters that consumer's PEL,
                the Pending Entries List — delivered, not yet acknowledged
    XACK        the consumer says it is done, and the entry leaves the PEL
    XPENDING    everything delivered and not acknowledged, with how long it has
                been idle and how many times it has been delivered
    XAUTOCLAIM  hand any entry idle longer than N milliseconds to somebody else

That N is the visibility timeout. It is the whole episode. Note what it is NOT:
it is not "how long the work takes". Nothing in this protocol knows how long the
work takes. It is a number you guessed at configuration time, and every time you
guess low the same job runs twice.
"""
import asyncio
import os
import socket
import time

import asyncpg
import httpx
from redis.asyncio import Redis
from redis.exceptions import ResponseError

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
APP_URL = os.getenv("APP_URL", "http://app:8000")
DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@postgres:5432/sysense")

NAME = os.getenv("WORKER_NAME", socket.gethostname())

STREAM = "checkouts"
GROUP = "payments"
DLQ = "checkouts:dead"

# ── THE KNOBS ──────────────────────────────────────────────────────────────
# Every one of these is a line in somebody's config file that nobody has looked
# at since the service was written.
#
#   ACK_MODE               where the XACK goes. `after` the work, or `before`
#                          it. There is no third option, and the two of them are
#                          the two delivery guarantees, spelled out.
#   VISIBILITY_TIMEOUT_MS  how long an entry may sit idle in the PEL before
#                          another consumer is allowed to take it. The guess.
#   HEARTBEAT              keep re-claiming the entry while the work is running,
#                          so the lease does not expire underneath it.
#   MAX_DELIVERIES         how many times a message may be delivered before it
#                          goes to the dead-letter stream. 0 means never, which
#                          is to say: forever.
#   IDEMPOTENT_CONSUMER    send the producer's key on to Episode 2's endpoint.
#                          This is the only setting here that makes redelivery
#                          harmless rather than merely less frequent.
#   WORKER_BATCH           how many entries to take per read. Every queue client
#                          does this (SQS MaxNumberOfMessages, Kafka
#                          max.poll.records) and it is where at-most-once does
#                          its real damage.
ACK_MODE = os.getenv("ACK_MODE", "after").strip().lower()
VISIBILITY_TIMEOUT_MS = int(os.getenv("VISIBILITY_TIMEOUT_MS", "2000"))
HEARTBEAT = os.getenv("HEARTBEAT", "0") == "1"
MAX_DELIVERIES = int(os.getenv("MAX_DELIVERIES", "0"))
IDEMPOTENT_CONSUMER = os.getenv("IDEMPOTENT_CONSUMER", "0") == "1"
WORKER_BATCH = int(os.getenv("WORKER_BATCH", "1"))
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))

# How many times we will come back on a 409 before giving up. Episode 2's
# endpoint answers 409 when another request holds the key and has not finished;
# under redelivery that other request is the other worker, running the same job.
MAX_CONFLICT_POLLS = 15

MODES = ("after", "before")

state: dict = {}


def log(msg: str) -> None:
    print(f"[{NAME}] {msg}", flush=True)


# ── The consumer's own book ────────────────────────────────────────────────
# There are now three sets of books in this repository and they are all
# necessary. `processor.ledger` is what happened to money. `public.charges` is
# what the application believes. `public.job_runs` is what the QUEUE did, and it
# is the only one that can tell you a job ran twice while the money was only
# taken once — which is exactly the state the last scenario is trying to reach.
#
# The row is written when the delivery starts and updated when it ends, so a
# worker that is killed mid-job leaves a row that says `started` and nothing
# else. That row is not a bug in the bookkeeping. It is the evidence.
START_RUN = """
INSERT INTO job_runs (message_id, seq, customer_id, worker, delivery, claimed, outcome)
VALUES ($1, $2, $3, $4, $5, $6, 'started') RETURNING id
"""

END_RUN = """
UPDATE job_runs SET outcome = $2, detail = $3, finished_at = now() WHERE id = $1
"""


async def start_run(mid: str, fields: dict, delivery: int, claimed: bool) -> int:
    async with state["db"].acquire() as con:
        return await con.fetchval(
            START_RUN, mid, int(fields.get("seq", 0)), int(fields.get("customer_id", 0)),
            NAME, delivery, claimed,
        )


async def end_run(run_id: int, outcome: str, detail: str | None = None) -> None:
    async with state["db"].acquire() as con:
        await con.execute(END_RUN, run_id, outcome, detail)


# ── Reading work ───────────────────────────────────────────────────────────
async def ensure_group(r: Redis) -> None:
    """Create the consumer group if it is not there.

    Also called after a NOGROUP error, because the capture script deletes the
    stream between scenarios and deleting a stream deletes its groups with it.
    """
    try:
        await r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log(f"created consumer group {GROUP} on {STREAM}")
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def claim_expired(r: Redis) -> list[tuple[str, dict]]:
    """XAUTOCLAIM — the visibility timeout, made visible.

    "Give me anything that has been sitting in somebody's PEL for longer than
    VISIBILITY_TIMEOUT_MS." Redis does not ask whether that somebody is dead. It
    cannot know. All it knows is that the entry has been idle, and idle is not a
    synonym for abandoned — a worker three seconds into a three-and-a-half-second
    payment is idle by this definition, and is about to have its job taken.
    """
    reply = await r.xautoclaim(
        STREAM, GROUP, NAME, min_idle_time=VISIBILITY_TIMEOUT_MS,
        start_id="0-0", count=WORKER_BATCH,
    )
    # Redis 7 returns [cursor, entries, deleted]; Redis 6 returns [cursor, entries].
    entries = reply[1] if isinstance(reply, (list, tuple)) and len(reply) >= 2 else []
    return [(mid, fields) for mid, fields in entries if fields]


async def read_new(r: Redis) -> list[tuple[str, dict]]:
    """XREADGROUP — entries nobody has been given yet."""
    reply = await r.xreadgroup(
        GROUP, NAME, streams={STREAM: ">"}, count=WORKER_BATCH, block=1000
    )
    if not reply:
        return []
    return [(mid, fields) for _stream, entries in reply for mid, fields in entries if fields]


async def delivery_count(r: Redis, mid: str) -> int:
    """How many times this entry has been handed out.

    Redis keeps the count in the PEL and XAUTOCLAIM increments it, so this is
    the queue's own answer rather than a counter the application maintains. It
    is the only thing standing between a poison message and an infinite loop,
    and most consumers never read it.
    """
    rows = await r.xpending_range(STREAM, GROUP, min=mid, max=mid, count=1)
    return int(rows[0]["times_delivered"]) if rows else 1


# ── Doing the work ─────────────────────────────────────────────────────────
async def charge(fields: dict) -> tuple[str, str]:
    """Call Episode 2's endpoint. Unchanged, and still correct.

    The only thing this episode varies is whether the key goes with it.

    The key is minted by the PRODUCER and carried in the message, not generated
    here. A consumer cannot tell delivery two from delivery one — that is the
    entire problem — so a key it generates for itself is a different key every
    time and buys nothing. The message id would work too, and is tempting, and
    is not enough for the reason Episode 4 is about: the producer can publish
    the same job twice, and then there are two message ids for one intent.
    """
    headers = {"Idempotency-Key": fields["key"]} if IDEMPOTENT_CONSUMER else {}
    body = {"customer_id": int(fields["customer_id"]),
            "amount_cents": int(fields["amount_cents"])}

    for _ in range(MAX_CONFLICT_POLLS):
        resp = await state["http"].post("/api/checkout", json=body, headers=headers)

        # 409: another request holds this key and has not finished. Under
        # redelivery that other request is the other worker, doing this same
        # job right now. Come back, as the server asked.
        if resp.status_code == 409:
            await asyncio.sleep(float(resp.headers.get("retry-after", 1)))
            continue

        resp.raise_for_status()
        replayed = resp.headers.get("idempotency-replayed") == "true"
        return ("replayed" if replayed else "charged"), resp.json().get("processor_charge_id", "")

    raise RuntimeError("the other holder of this key never finished")


async def do_work(fields: dict, delivery: int) -> tuple[str, str]:
    amount = int(fields.get("amount_cents", 0))

    # ── The poison message ─────────────────────────────────────────────────
    # A payload this consumer cannot process and never will be able to. It is
    # not a transient failure and no amount of retrying converts it into one.
    # A bad producer shipped it, it is on the stream, and the stream has no
    # opinion about that.
    if amount <= 0:
        raise ValueError(f"amount_cents={amount} is not a payment")

    # A genuinely transient failure: fails on its first N deliveries, then
    # succeeds. This is the kind retries are FOR, and it is also how a message
    # ends up behind messages that were published after it.
    if delivery <= int(fields.get("fail_times", 0)):
        raise RuntimeError(f"transient failure on delivery {delivery}")

    return await charge(fields)


async def hold_lease(r: Redis, mid: str) -> None:
    """The heartbeat: re-claim the entry while the work is still running.

    XCLAIM with a min-idle-time of 0, by the consumer that already owns it,
    resets the idle clock. JUSTID keeps it from counting as another delivery.

    This is the right fix for bug one and it is not a complete one. The lease
    can still be lost — the worker can be killed, its network can partition, its
    process can stop the world for a garbage collection longer than the timeout —
    and when it is lost the job runs twice anyway. Extending the lease lowers the
    rate. Only an idempotent consumer changes the outcome.
    """
    period = max(VISIBILITY_TIMEOUT_MS / 2000, 0.2)
    while True:
        await asyncio.sleep(period)
        await r.xclaim(STREAM, GROUP, NAME, min_idle_time=0, message_ids=[mid], justid=True)
        log(f"LEASE  {mid} extended")


# ── The eight lines the whole episode is about ─────────────────────────────
async def handle(r: Redis, mid: str, fields: dict, claimed: bool) -> None:
    delivery = await delivery_count(r, mid)
    seq, cid = fields.get("seq", "?"), fields.get("customer_id", "?")
    log(f"{'CLAIM ' if claimed else 'NEW   '} {mid} seq={seq} customer={cid} delivery={delivery}"
        + (f"  (idle > {VISIBILITY_TIMEOUT_MS} ms)" if claimed else ""))

    run_id = await start_run(mid, fields, delivery, claimed)
    started = time.perf_counter()

    beat = asyncio.create_task(hold_lease(r, mid)) if HEARTBEAT else None
    try:
        outcome, charge_id = await do_work(fields, delivery)
    except Exception as exc:
        if beat:
            beat.cancel()
        await end_run(run_id, "failed", f"{type(exc).__name__}: {exc}")
        await on_failure(r, mid, fields, delivery, exc)
        return
    finally:
        if beat:
            beat.cancel()

    await end_run(run_id, outcome, charge_id)
    log(f"{outcome.upper():7} {mid} seq={seq} customer={cid} {charge_id} "
        f"in {time.perf_counter() - started:.2f}s")

    # ── at-least-once ──────────────────────────────────────────────────────
    # The entry leaves the PEL only once the work is done. Crash before this
    # line and the entry is still pending, so somebody else will take it and do
    # the work again. Nothing is lost. Things happen twice.
    if ACK_MODE == "after":
        await r.xack(STREAM, GROUP, mid)


async def on_failure(r: Redis, mid: str, fields: dict, delivery: int, exc: Exception) -> None:
    """It threw. Now what?

    Doing nothing is a decision, and it is the default one: no XACK means the
    entry stays in the PEL, its idle clock restarts, and in VISIBILITY_TIMEOUT_MS
    it is claimable again. Forever. A queue in this state has depth, has
    consumers, has throughput on every dashboard you own, and is doing no work.
    """
    if MAX_DELIVERIES and delivery >= MAX_DELIVERIES:
        # ── The dead-letter stream ─────────────────────────────────────────
        # Not a bin. The message is moved somewhere a human can read it, WITH
        # the error and the delivery count that sent it there, and the main
        # stream is released. Depth here is not "failures we are ignoring"; it
        # is "the processing SLO is broken and this is the evidence".
        await r.xadd(DLQ, {**fields, "failed_after_deliveries": delivery,
                           "error": f"{type(exc).__name__}: {exc}", "worker": NAME})
        await r.xack(STREAM, GROUP, mid)
        log(f"DEAD    {mid} seq={fields.get('seq','?')} after {delivery} deliveries "
            f"-> {DLQ}  ({type(exc).__name__}: {exc})")
        return

    log(f"FAILED  {mid} seq={fields.get('seq','?')} delivery={delivery} left pending "
        f"({type(exc).__name__}: {exc})")


async def main() -> None:
    if ACK_MODE not in MODES:
        raise SystemExit(f"ACK_MODE must be one of {MODES}")

    r = Redis.from_url(REDIS_URL, decode_responses=True)
    state["db"] = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    state["http"] = httpx.AsyncClient(base_url=APP_URL, timeout=HTTP_TIMEOUT_S)
    await ensure_group(r)

    log(f"ack={ACK_MODE} visibility_timeout={VISIBILITY_TIMEOUT_MS}ms heartbeat={int(HEARTBEAT)} "
        f"max_deliveries={MAX_DELIVERIES} idempotent_consumer={int(IDEMPOTENT_CONSUMER)} "
        f"batch={WORKER_BATCH}")

    while True:
        try:
            # Expired leases first: work somebody else was given and did not
            # finish is older than work nobody has touched.
            batch, claimed = await claim_expired(r), True
            if not batch:
                batch, claimed = await read_new(r), False
            if not batch:
                continue

            # ── at-most-once ───────────────────────────────────────────────
            # The whole batch leaves the PEL the moment it is handed over, and
            # from here on the queue has no idea any of it is outstanding. Crash
            # halfway through and the rest is not redelivered, not retried, and
            # not reported anywhere: it was acknowledged. Nothing happens twice.
            # Things are lost.
            if ACK_MODE == "before":
                await r.xack(STREAM, GROUP, *[mid for mid, _ in batch])
                log(f"ACKED   {len(batch)} on receipt, before doing any of it")

            for mid, fields in batch:
                await handle(r, mid, fields, claimed)

        except ResponseError as e:
            if "NOGROUP" in str(e):
                await ensure_group(r)
                continue
            raise
        except (ConnectionError, httpx.HTTPError) as e:
            log(f"transport error, retrying: {type(e).__name__}: {e}")
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
