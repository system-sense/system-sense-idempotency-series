"""System Sense — Idempotency Ep.4: the producer, and the window inside it.

Run it:      docker compose up --build
Try it:      curl -sS -X POST localhost:8100/api/orders \
               -H 'content-type: application/json' \
               -d '{"seq": 1, "customer_id": 17, "amount_cents": 4000}'

Episode 3's producer was `scripts/enqueue.py`: one XADD and an exit. It had no
database, so it could not have this bug. Nothing that has no database can.

This one does. An order was placed, and two things have to become true because
of it:

    a row in `orders`, in PostgreSQL
    an event on `checkouts`, in Redis

Two systems. There is no transaction that spans them. Whichever you do first,
there is a moment when one is true and the other is not, and if the process
stops in that moment nothing anywhere will ever reconcile the two. That moment
is not a bug in this file. Read the handler: there is nothing in it to fix.

`PUBLISH_MODE` picks between the three arrangements, and the third one is not
a more careful version of the first two. It is a different shape.

    commit_first   COMMIT, then publish.   Kill in between -> the order exists
                                           and nothing will ever pay for it.
    publish_first  Publish, then COMMIT.   Kill in between -> money moves for an
                                           order that does not exist. Worse.
    outbox         COMMIT both, in one     Kill in the same place -> nothing is
                   transaction, to the     lost, because there is no longer a
                   same database.          window to be killed in.

`CRASH_EVERY` is the kill switch. It is a real `os._exit(1)`: the process is
gone, mid-request, with no unwinding and no shutdown hook, which is what a
kill -9, an OOM kill, a rolling deploy and a spot instance reclaim all look
like from in here. Docker restarts it, because that is also what happens.
"""
import json
import os
import time
import uuid
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from redis.asyncio import Redis

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@postgres:5432/sysense")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

STREAM = "checkouts"

# ── THE KNOBS ──────────────────────────────────────────────────────────────
#   PUBLISH_MODE   commit_first | publish_first | outbox
#   CRASH_EVERY    kill the process on every Nth order, in the window. 0 = never.
#   FAIL_TIMES     carried into the message for Episode 3's consumer. Unused here.
PUBLISH_MODE = os.getenv("PUBLISH_MODE", "outbox").strip().lower()
CRASH_EVERY = int(os.getenv("CRASH_EVERY", "0"))

MODES = ("commit_first", "publish_first", "outbox")

state: dict = {}


class Order(BaseModel):
    seq: int
    customer_id: int
    amount_cents: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    if PUBLISH_MODE not in MODES:
        raise SystemExit(f"PUBLISH_MODE must be one of {MODES}")
    state["db"] = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    state["redis"] = Redis.from_url(REDIS_URL, decode_responses=True)
    log(f"mode={PUBLISH_MODE} crash_every={CRASH_EVERY or 'never'}")
    yield
    await state["redis"].aclose()
    await state["db"].close()


app = FastAPI(title="System Sense — Idempotency Ep.4 (producer)", lifespan=lifespan)


def log(msg: str) -> None:
    print(f"[orders] {msg}", flush=True)


@app.get("/health")
async def health():
    return {"ok": True, "mode": PUBLISH_MODE, "crash_every": CRASH_EVERY}


def event_fields(o: Order, key: str) -> dict:
    """The message, in Episode 3's format, unchanged.

    Same fields the consumer already reads, including `key` — Episode 2's
    idempotency key, minted once per ORDER and carried with it. Everything that
    is about to go wrong will go wrong under this one key, which is what makes
    it recoverable at the far end.
    """
    return {
        "seq": o.seq,
        "customer_id": o.customer_id,
        "amount_cents": o.amount_cents,
        "key": key,
        "fail_times": 0,
        "enqueued_at": f"{time.time():.3f}",
    }


# ── The window ─────────────────────────────────────────────────────────────
# Every branch below is somebody's production code and none of them is careless.
# The kill is deterministic on `seq` rather than random so that all three modes
# are killed at the same three orders, and the comparison between them is a
# measurement rather than a coincidence.
def maybe_crash(o: Order, where: str) -> None:
    if CRASH_EVERY and o.seq % CRASH_EVERY == 0:
        log(f"seq={o.seq} customer={o.customer_id} *** KILLED {where} ***")
        # No unwinding, no shutdown hook, no flush. There is no version of this
        # written more carefully that survives it, which is the point.
        os._exit(1)


INSERT_ORDER = """
INSERT INTO orders (seq, customer_id, amount_cents, order_key)
VALUES ($1, $2, $3, $4) RETURNING id
"""

INSERT_OUTBOX = """
INSERT INTO outbox (order_id, topic, payload) VALUES ($1, $2, $3) RETURNING id
"""


async def publish(fields: dict) -> str:
    return await state["redis"].xadd(STREAM, {k: str(v) for k, v in fields.items()})


@app.post("/api/orders")
async def place_order(o: Order):
    """Place an order. Make the money move. Two systems, one intent."""
    key = f"k_{uuid.uuid4().hex[:12]}"
    fields = event_fields(o, key)

    # ── commit_first: the one everybody writes ─────────────────────────────
    # Write the row, then tell the world. It is the right order — you do not
    # announce an order you have not stored — and it is a dual write, and it
    # loses events.
    if PUBLISH_MODE == "commit_first":
        async with state["db"].acquire() as con:
            order_id = await con.fetchval(INSERT_ORDER, o.seq, o.customer_id,
                                          o.amount_cents, key)
        maybe_crash(o, "after COMMIT, before publish")
        mid = await publish(fields)
        log(f"seq={o.seq} customer={o.customer_id} order={order_id} COMMITTED -> PUBLISHED {mid}")
        return {"order_id": order_id, "key": key, "message_id": mid}

    # ── publish_first: the mirror image, and it is worse ───────────────────
    # Swapping the order does not remove the window, it moves it. Now the event
    # is real and the order is not: a consumer charges a customer for something
    # that is not in the database, and support has nothing to look at.
    if PUBLISH_MODE == "publish_first":
        mid = await publish(fields)
        maybe_crash(o, "after publish, before COMMIT")
        async with state["db"].acquire() as con:
            order_id = await con.fetchval(INSERT_ORDER, o.seq, o.customer_id,
                                          o.amount_cents, key)
        log(f"seq={o.seq} customer={o.customer_id} PUBLISHED {mid} -> order={order_id} COMMITTED")
        return {"order_id": order_id, "key": key, "message_id": mid}

    # ── outbox: one commit, both facts ─────────────────────────────────────
    # The event is not published here at all. It is INSERTed, into a table in
    # the same database, inside the same transaction as the order. There is no
    # second system in this code path, so there is no window, so there is
    # nothing for the kill switch to land in — and the kill switch still fires,
    # in the same place, on the same three orders.
    #
    # What this costs: the event is now published by somebody else, later, and
    # possibly twice. That is the trade, and it is a good one, because "twice"
    # is a problem Episodes 2 and 3 already solved and "never" is not.
    async with state["db"].acquire() as con:
        async with con.transaction():
            order_id = await con.fetchval(INSERT_ORDER, o.seq, o.customer_id,
                                          o.amount_cents, key)
            outbox_id = await con.fetchval(INSERT_OUTBOX, order_id, STREAM,
                                           json.dumps(fields, separators=(",", ":")))
    maybe_crash(o, "after COMMIT, before publish")
    log(f"seq={o.seq} customer={o.customer_id} order={order_id} outbox={outbox_id} "
        f"COMMITTED (both)")
    return {"order_id": order_id, "key": key, "outbox_id": outbox_id}


@app.exception_handler(Exception)
async def anything(request, exc):
    log(f"ERROR {type(exc).__name__}: {exc}")
    return JSONResponse({"detail": f"{type(exc).__name__}: {exc}"}, status_code=500)
