"""System Sense — Idempotency Ep.1: the checkout that can charge you twice.

Run it:      docker compose up --build
Try it:      curl -i --max-time 2 -X POST localhost:8000/api/checkout \
               -H 'content-type: application/json' \
               -d '{"customer_id": 7, "amount_cents": 4000}'

There is no bug in this file. Read it twice and you will not find one: it takes
a request, charges the customer once, records it once, returns. Every line of it
is correct.

The bug is that it can be called twice for one press of a Pay button, and
nothing here can tell the difference. That is what Episode 2 fixes.
"""
import asyncio
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import config

state: dict = {}


class CheckoutRequest(BaseModel):
    customer_id: int
    amount_cents: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["db"] = await asyncpg.create_pool(config.DATABASE_URL, min_size=2, max_size=20)
    state["http"] = httpx.AsyncClient(
        base_url=config.PROCESSOR_URL,
        timeout=config.PROCESSOR_TIMEOUT_SECONDS,
    )
    print(f"[app] processor={config.PROCESSOR_URL}", flush=True)
    yield
    await state["http"].aclose()
    await state["db"].close()


app = FastAPI(title="System Sense — Idempotency Ep.1", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


async def _charge_and_record(req: CheckoutRequest) -> dict:
    """Call the processor, then write down what it told us.

    Shielded by the caller: a customer closing their laptop must not leave a
    captured payment unrecorded. Abandoning this half-way through would turn one
    bug (a double charge) into a worse one (money taken and no record of it).
    """
    resp = await state["http"].post(
        "/charges",
        json={"customer_id": req.customer_id, "amount_cents": req.amount_cents},
    )
    resp.raise_for_status()
    charge = resp.json()

    async with state["db"].acquire() as con:
        row = await con.fetchrow(
            "INSERT INTO charges (customer_id, amount_cents, processor_charge_id)"
            " VALUES ($1, $2, $3) RETURNING id, created_at",
            req.customer_id,
            req.amount_cents,
            charge["id"],
        )

    return {
        "charge_id": row["id"],
        "processor_charge_id": charge["id"],
        "customer_id": req.customer_id,
        "amount_cents": req.amount_cents,
        "created_at": row["created_at"].isoformat(),
    }


@app.post("/api/checkout")
async def checkout(req: CheckoutRequest):
    """Charge a customer. Once. Correctly. Every single time it is called."""
    started = time.perf_counter()
    result = await asyncio.shield(asyncio.create_task(_charge_and_record(req)))
    elapsed_ms = (time.perf_counter() - started) * 1000

    print(
        f"POST /api/checkout  customer={req.customer_id} "
        f"amount={req.amount_cents}  {elapsed_ms:8.1f} ms  -> {result['processor_charge_id']}",
        flush=True,
    )

    return JSONResponse(result, headers={"X-Elapsed-Ms": f"{elapsed_ms:.1f}"})


@app.get("/api/customers/{customer_id}")
async def read_customer(customer_id: int):
    """A GET. Call it once or a thousand times; the answer is the same and the
    world is unchanged. That is not politeness, it is the HTTP spec: GET, PUT
    and DELETE are defined as idempotent methods. POST is not."""
    async with state["db"].acquire() as con:
        cust = await con.fetchrow("SELECT id, name, email FROM customers WHERE id = $1", customer_id)
        if cust is None:
            return JSONResponse({"detail": "no such customer"}, status_code=404)
        agg = await con.fetchrow(
            "SELECT count(*) AS charges, coalesce(sum(amount_cents), 0) AS charged_cents"
            " FROM charges WHERE customer_id = $1",
            customer_id,
        )
    return {"id": cust["id"], "name": cust["name"], "email": cust["email"],
            "charges": agg["charges"], "charged_cents": int(agg["charged_cents"])}


class EmailUpdate(BaseModel):
    email: str


@app.put("/api/customers/{customer_id}/email")
async def set_email(customer_id: int, body: EmailUpdate):
    """A PUT. It says "make the email be this", not "add an email". Send it five
    times and the fifth changes nothing the first did not already do."""
    async with state["db"].acquire() as con:
        row = await con.fetchrow(
            "UPDATE customers SET email = $2 WHERE id = $1 RETURNING id, email",
            customer_id,
            body.email,
        )
    if row is None:
        return JSONResponse({"detail": "no such customer"}, status_code=404)
    return {"id": row["id"], "email": row["email"]}


@app.get("/api/ledger")
async def ledger():
    """Our books beside the processor's books. When these two disagree, the
    difference is somebody's money."""
    async with state["db"].acquire() as con:
        ours = await con.fetchrow(
            "SELECT count(*) AS charges, coalesce(sum(amount_cents), 0) AS cents,"
            " count(DISTINCT customer_id) AS customers FROM charges"
        )
        theirs = await con.fetchrow(
            "SELECT count(*) AS charges, coalesce(sum(amount_cents), 0) AS cents,"
            " count(DISTINCT customer_id) AS customers FROM processor.ledger"
        )
    return {"app": {k: int(v) for k, v in dict(ours).items()},
            "processor": {k: int(v) for k, v in dict(theirs).items()}}
