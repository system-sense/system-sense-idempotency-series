"""System Sense — Idempotency Ep.4: the relay. Reads the outbox, publishes, and
may publish the same row twice.

Run it:      docker compose up --build
Watch it:    docker compose logs -f relay

The producer no longer publishes anything. It writes the event into `outbox`,
in the same transaction as the order, and that transaction is the only thing in
this episode with a guarantee attached to it.

Which leaves this process, whose entire job is:

    SELECT the rows that have not been published
    XADD them
    mark them published

and whose entire problem is that the last two of those are, once again, two
systems. Kill it in between and the row is still unpublished as far as the
database is concerned, so the next relay publishes it again.

**That is not a flaw in the pattern, it is the pattern.** The outbox does not
promise the event goes out once. It promises the event is not LOST — because it
was committed with the data, atomically, and nothing after that can lose it. It
converts an unrecoverable failure into a duplicate, and a duplicate is what
Episode 2's key and Episode 3's consumer were built to absorb.

Exactly-once delivery is not achievable here or anywhere. Exactly-once EFFECTS
are, and this is the shape of them: at-least-once delivery, plus a consumer that
can be called twice.

Log tailing (Debezium, or Postgres logical decoding directly) is the same
pattern with this process deleted and the write-ahead log read instead. It has
the same at-least-once property, for the same reason: something has to record
how far it got, and that record is not in the same transaction as the send.
"""
import asyncio
import json
import os

import asyncpg
from redis.asyncio import Redis

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@postgres:5432/sysense")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

NAME = os.getenv("RELAY_NAME", "relay")

# ── THE KNOBS ──────────────────────────────────────────────────────────────
#   POLL_MS               how often to look for unsent rows. This is the
#                         latency the outbox costs you, and it is the honest
#                         price of the pattern.
#   RELAY_BATCH           rows per pass.
#   CRASH_AFTER_PUBLISH   kill the process after the XADD and before the row is
#                         marked sent, until N rows have been published twice.
#                         0 = never.
POLL_MS = int(os.getenv("POLL_MS", "200"))
RELAY_BATCH = int(os.getenv("RELAY_BATCH", "10"))
CRASH_AFTER_PUBLISH = int(os.getenv("CRASH_AFTER_PUBLISH", "0"))

state: dict = {}


def log(msg: str) -> None:
    print(f"[{NAME}] {msg}", flush=True)


# ── The relay's only query ─────────────────────────────────────────────────
# `ORDER BY id` publishes in commit order. One relay runs here, so nothing needs
# locking; a second one would want `FOR UPDATE SKIP LOCKED` and a transaction
# around each row, which stops two relays publishing the same row at the same
# instant and still does not make duplicates impossible. Nothing does.
UNSENT = """
SELECT id, topic, payload, publish_attempts
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
"""

ATTEMPT = "UPDATE outbox SET publish_attempts = publish_attempts + 1 WHERE id = $1 RETURNING publish_attempts"

SENT = "UPDATE outbox SET published_at = now(), message_id = $2 WHERE id = $1"

# How many rows have already been published more than once. Read from the
# database rather than kept in memory, because the process is about to stop
# existing and a counter in memory would reset with it.
CRASHES = "SELECT count(*) FROM outbox WHERE publish_attempts > 1"


async def publish_one(con, row) -> None:
    fields = json.loads(row["payload"])

    # Counted before the send, so that a row published twice says so afterwards.
    # Note what this is: a write to the database, then a write to the queue.
    # The relay has the producer's bug. It cannot not have it. The difference is
    # that here the bug costs a duplicate rather than a loss, and by now the
    # whole system is built to shrug at duplicates.
    attempts = await con.fetchval(ATTEMPT, row["id"])

    mid = await state["redis"].xadd(row["topic"], {k: str(v) for k, v in fields.items()})
    log(f"outbox={row['id']} seq={fields.get('seq')} customer={fields.get('customer_id')} "
        f"key={fields.get('key')} PUBLISHED {mid} attempt={attempts}")

    if CRASH_AFTER_PUBLISH and attempts == 1:
        already = await con.fetchval(CRASHES)
        if already < CRASH_AFTER_PUBLISH:
            log(f"outbox={row['id']} *** KILLED after publish, before marking sent ***")
            os._exit(1)

    await con.execute(SENT, row["id"], mid)


async def pass_once() -> int:
    """One pass, and every statement in it commits on its own.

    Deliberately NOT one transaction around the batch. If the attempt counter
    were only committed at the end of the pass, a process killed mid-pass would
    roll it back, and the row would come back looking untouched — which is the
    same trap as the dual write, one level down, and it would make this file
    lie about how many times it published something.
    """
    async with state["db"].acquire() as con:
        rows = await con.fetch(UNSENT, RELAY_BATCH)
        for row in rows:
            await publish_one(con, row)
        return len(rows)


async def main() -> None:
    state["db"] = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    state["redis"] = Redis.from_url(REDIS_URL, decode_responses=True)
    log(f"poll={POLL_MS}ms batch={RELAY_BATCH} crash_after_publish={CRASH_AFTER_PUBLISH or 'never'}")

    while True:
        try:
            if not await pass_once():
                await asyncio.sleep(POLL_MS / 1000)
        except (asyncpg.PostgresError, OSError) as e:
            log(f"transport error, retrying: {type(e).__name__}: {e}")
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
