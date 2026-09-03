"""The payment processor. A stand-in for Stripe, and the only thing here that
moves money.

It is a separate service on purpose. The bug this episode is about lives in the
gap between two processes: our application calls this one over the network, and
a network call can succeed while its response never arrives.

Two properties make it a faithful stand-in:

  1. **It is sometimes slow, not always slow.** Latency is a deterministic
     function of the customer id (see `latency_ms`), so roughly half the
     checkouts come back inside the client's timeout and roughly half do not.
     A demo where every request times out would be rigged; this one is a tail
     latency problem, which is what these actually are.

  2. **A capture is not abandoned because a socket closed.** The write is
     wrapped in `asyncio.shield`, so once this service has accepted the charge
     it completes it even if the caller has hung up. Real processors behave
     this way, and it is precisely why "the client saw a timeout" tells you
     nothing about whether the money moved.
"""
import asyncio
import os
import uuid

import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@localhost:5432/sysense")

# ── The one knob that shapes the whole demo ────────────────────────────────
# latency = BASE + (customer_id * 137) % SPREAD, in milliseconds.
#
# Deterministic, so the capture reproduces; spread across a range that straddles
# the client's 2000 ms timeout, so the failure is intermittent rather than
# staged. 137 is prime, which keeps consecutive customer ids from landing in a
# tidy ascending line.
LATENCY_BASE_MS = int(os.getenv("LATENCY_BASE_MS", "1200"))
LATENCY_SPREAD_MS = int(os.getenv("LATENCY_SPREAD_MS", "2400"))

state: dict = {}


def latency_ms(customer_id: int) -> int:
    return LATENCY_BASE_MS + (customer_id * 137) % LATENCY_SPREAD_MS


class ChargeRequest(BaseModel):
    customer_id: int
    amount_cents: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["db"] = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=20)
    print(
        f"[processor] latency = {LATENCY_BASE_MS} + (customer_id * 137) % {LATENCY_SPREAD_MS} ms",
        flush=True,
    )
    yield
    await state["db"].close()


app = FastAPI(title="Payment processor (stand-in)", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


async def _capture(charge_id: str, req: ChargeRequest, delay_ms: int) -> None:
    """Take the money. Slow, and deliberately uninterruptible."""
    await asyncio.sleep(delay_ms / 1000)
    async with state["db"].acquire() as con:
        await con.execute(
            "INSERT INTO processor.ledger (id, customer_id, amount_cents) VALUES ($1, $2, $3)",
            charge_id,
            req.customer_id,
            req.amount_cents,
        )
    print(
        f"[processor] CAPTURED {charge_id}  customer={req.customer_id} "
        f"amount={req.amount_cents}  after {delay_ms} ms",
        flush=True,
    )


@app.post("/charges")
async def create_charge(req: ChargeRequest):
    charge_id = f"ch_{uuid.uuid4().hex[:16]}"
    delay_ms = latency_ms(req.customer_id)

    # shield(): once we have accepted the charge, finish it. If the caller
    # disconnects mid-flight the money still moves — which is the entire point.
    await asyncio.shield(asyncio.create_task(_capture(charge_id, req, delay_ms)))

    return {"id": charge_id, "customer_id": req.customer_id,
            "amount_cents": req.amount_cents, "latency_ms": delay_ms}


@app.get("/ledger/summary")
async def ledger_summary():
    """What actually happened to money, from the processor's own books."""
    async with state["db"].acquire() as con:
        row = await con.fetchrow(
            "SELECT count(*) AS charges, coalesce(sum(amount_cents), 0) AS collected_cents,"
            " count(DISTINCT customer_id) AS customers FROM processor.ledger"
        )
    return dict(row)
