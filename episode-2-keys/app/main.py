"""System Sense — Idempotency Ep.2: the key that has a race condition in it.

Run it:      docker compose up --build
Try it:      curl -i --max-time 2 -X POST localhost:8000/api/checkout \
               -H 'content-type: application/json' \
               -H 'Idempotency-Key: k_demo_1' \
               -d '{"customer_id": 17, "amount_cents": 4000}'

Episode 1 ended with a handler that was correct line by line and charged
fourteen people twice, because it could be called twice for one press of Pay
and nothing could tell the difference. The obvious fix is to give the client a
key and check whether we have seen it before.

This file contains that fix, and the fix that actually works, and they are
fifteen lines apart. `IDEMPOTENCY_MODE` picks between them.
"""
import asyncio
import hashlib
import json
import time
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from . import config

state: dict = {}

ENDPOINT = "POST /api/checkout"


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
    await apply_constraint()
    print(
        f"[app] mode={config.IDEMPOTENCY_MODE}  ttl={config.IDEMPOTENCY_TTL_SECONDS:g}s  "
        f"unique_constraint={config.WANTS_CONSTRAINT.get(config.IDEMPOTENCY_MODE, True)}",
        flush=True,
    )
    yield
    await state["http"].aclose()
    await state["db"].close()


async def apply_constraint() -> None:
    """Put the UNIQUE index in place, or take it away.

    `naive` is not a code path so much as a world: the world where nobody added
    the constraint. Asserting it here rather than in the migration is what lets
    one capture run the identical handler on both sides of that one line, and
    what makes it impossible for a run to be quietly mislabelled.
    """
    if config.IDEMPOTENCY_MODE not in config.MODES:
        raise SystemExit(f"IDEMPOTENCY_MODE must be one of {config.MODES}")
    want = config.WANTS_CONSTRAINT[config.IDEMPOTENCY_MODE]
    ddl = (
        "CREATE UNIQUE INDEX IF NOT EXISTS idempotency_keys_scope_key_uniq"
        " ON idempotency_keys (scope, idempotency_key)"
        if want
        else "DROP INDEX IF EXISTS idempotency_keys_scope_key_uniq"
    )
    async with state["db"].acquire() as con:
        await con.execute(ddl)


app = FastAPI(title="System Sense — Idempotency Ep.2", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True, "mode": config.IDEMPOTENCY_MODE}


# ── The key, and what it is unique within ──────────────────────────────────
def scope_for(req: CheckoutRequest) -> str:
    """One customer, one endpoint.

    Clients generate these keys, and two clients will eventually pick the same
    string. A globally unique key space would let one customer's retry replay
    another customer's response, which is a worse bug than the one being fixed.
    """
    return f"customer:{req.customer_id}|{ENDPOINT}"


def fingerprint(req: CheckoutRequest) -> str:
    """A hash of the body this key was first used with.

    Same key, different body, is a client bug. Stripe answers it with an error
    rather than a second charge, and so does this.
    """
    canonical = json.dumps(req.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


async def charge_customer(req: CheckoutRequest, key: str | None) -> dict:
    """Call the processor, then write down what it told us.

    Unchanged from Episode 1 apart from writing the key alongside the charge.
    Shielded by the caller: a customer closing their laptop must not leave a
    captured payment unrecorded.
    """
    resp = await state["http"].post(
        "/charges",
        json={"customer_id": req.customer_id, "amount_cents": req.amount_cents},
    )
    resp.raise_for_status()
    charge = resp.json()

    async with state["db"].acquire() as con:
        row = await con.fetchrow(
            "INSERT INTO charges (customer_id, amount_cents, processor_charge_id, idempotency_key)"
            " VALUES ($1, $2, $3, $4) RETURNING id",
            req.customer_id, req.amount_cents, charge["id"], key,
        )

    return {"charge_id": row["id"], "processor_charge_id": charge["id"],
            "customer_id": req.customer_id, "amount_cents": req.amount_cents}


# ── Mode 1: off. Episode 1, unchanged. ─────────────────────────────────────
async def checkout_off(req: CheckoutRequest, key: str | None) -> Response:
    result = await asyncio.shield(asyncio.create_task(charge_customer(req, key)))
    log(f"customer={req.customer_id} key={key} NO CHECK -> CHARGED")
    return fresh(serialise(result))


# ── Modes 2 and 3: check, then insert. ─────────────────────────────────────
async def checkout_check_then_insert(req: CheckoutRequest, key: str) -> Response:
    """The obvious fix. Read it and try to find what is wrong with it.

    Both `naive` and `late` run this exact function. The only difference between
    them is whether idempotency_keys has a UNIQUE index on it, and that
    difference decides whether the second charge is silent or merely too late.
    """
    scope = scope_for(req)

    async with state["db"].acquire() as con:
        seen = await con.fetchrow(
            "SELECT response_status, response_body FROM idempotency_keys"
            " WHERE scope = $1 AND idempotency_key = $2"
            "   AND status = 'completed' AND expires_at > now()",
            scope, key,
        )
    checked_at = time.perf_counter()

    if seen is not None:
        log(f"customer={req.customer_id} key={key} CHECK present -> REPLAY {seen['response_status']}")
        return replay(seen)

    # ── the window ──────────────────────────────────────────────────────────
    # Everything from here to the INSERT below is time in which a second
    # request can run the SELECT above and be told, truthfully, that this key
    # has never been seen. It is not a microsecond. It is however long the
    # payment call takes — and the retry was scheduled to arrive right in the
    # middle of it, because that is what a timeout is.
    result = await asyncio.shield(asyncio.create_task(charge_customer(req, key)))
    body = serialise(result)

    async with state["db"].acquire() as con:
        await con.execute(
            "INSERT INTO idempotency_keys (scope, idempotency_key, request_fingerprint,"
            " status, response_status, response_body, completed_at, expires_at)"
            " VALUES ($1, $2, $3, 'completed', 200, $4, now(),"
            "         now() + make_interval(secs => $5))",
            scope, key, fingerprint(req), body, config.IDEMPOTENCY_TTL_SECONDS,
        )

    window_ms = (time.perf_counter() - checked_at) * 1000
    log(f"customer={req.customer_id} key={key} CHECK absent -> CHARGED -> RECORDED "
        f"window_ms={window_ms:.3f}")
    return fresh(body)


# ── Mode 4: claim the key first. The fix. ──────────────────────────────────
CLAIM = """
INSERT INTO idempotency_keys (scope, idempotency_key, request_fingerprint, status, expires_at)
VALUES ($1, $2, $3, 'in_flight', now() + make_interval(secs => $4))
ON CONFLICT (scope, idempotency_key) DO UPDATE
   SET request_fingerprint = EXCLUDED.request_fingerprint,
       status              = 'in_flight',
       response_status     = NULL,
       response_body       = NULL,
       created_at          = now(),
       completed_at        = NULL,
       expires_at          = EXCLUDED.expires_at
 WHERE idempotency_keys.expires_at <= now()
RETURNING id, xmax::text::bigint <> 0 AS revived
"""

COMPLETE = """
UPDATE idempotency_keys
   SET status = 'completed', response_status = $3, response_body = $4, completed_at = now()
 WHERE scope = $1 AND idempotency_key = $2
"""


async def checkout_claim_first(req: CheckoutRequest, key: str) -> Response:
    """Claim the key, then do the work.

    The application never asks "have I seen this?" It asserts "this one is
    mine", and the database tells exactly one of the two callers that it is not.
    `RETURNING` coming back empty is the loser finding out it lost.
    """
    scope, fp = scope_for(req), fingerprint(req)

    async with state["db"].acquire() as con:
        claimed = await con.fetchrow(CLAIM, scope, key, fp, config.IDEMPOTENCY_TTL_SECONDS)
        if claimed is None:
            held = await con.fetchrow(
                "SELECT status, request_fingerprint, response_status, response_body"
                " FROM idempotency_keys WHERE scope = $1 AND idempotency_key = $2",
                scope, key,
            )
        # `xmax <> 0` is true only on the ON CONFLICT DO UPDATE path: the row
        # existed, had expired, and we took it over. Worth logging, because a
        # revived key means a real second charge is about to happen.
        elif claimed["revived"]:
            log(f"customer={req.customer_id} key={key} CLAIMED (expired key revived)")

    if claimed is None:
        return lost_the_race(req, key, held, fp)

    result = await asyncio.shield(asyncio.create_task(charge_customer(req, key)))
    body = serialise(result)

    async with state["db"].acquire() as con:
        await con.execute(COMPLETE, scope, key, 200, body)

    log(f"customer={req.customer_id} key={key} CLAIMED -> CHARGED -> COMPLETED")
    return fresh(body)


def lost_the_race(req: CheckoutRequest, key: str, held, fp: str) -> JSONResponse:
    """Somebody else owns this key. Three things that can mean."""
    if held is None:
        # The key expired between the claim and this read. Vanishingly rare and
        # not worth a branch in production; answered here rather than crashing.
        return conflict(req, key, "the key expired mid-request")

    if held["request_fingerprint"] != fp:
        log(f"customer={req.customer_id} key={key} LOST -> FINGERPRINT MISMATCH 400")
        return JSONResponse(
            {"error": {"type": "idempotency_error",
                       "message": "This key was first used with a different request body."}},
            status_code=400,
            headers={"Idempotency-Replayed": "false"},
        )

    if held["status"] == "completed":
        log(f"customer={req.customer_id} key={key} LOST -> REPLAY {held['response_status']}")
        return replay(held)

    # ── The in-flight case ─────────────────────────────────────────────────
    # There is a row, and there is no response to replay yet, because the first
    # request is still talking to the processor. Two defensible answers: hold
    # this connection open until the first one finishes, or tell the caller to
    # come back. Waiting ties up a worker for as long as the dependency is slow,
    # which under a retry storm is exactly when you have none to spare. So: 409,
    # and a Retry-After. The client already knows how to come back — that is how
    # it got here.
    return conflict(req, key, "the first request with this key is still running")


def conflict(req: CheckoutRequest, key: str, why: str) -> JSONResponse:
    log(f"customer={req.customer_id} key={key} LOST -> IN FLIGHT 409")
    return JSONResponse(
        {"error": {"type": "idempotency_in_flight", "message": why}},
        status_code=409,
        headers={"Retry-After": "1", "Idempotency-Replayed": "false"},
    )


def serialise(result: dict) -> str:
    """The response body, as the bytes that will go on the wire.

    Serialised once, sent to the first caller and stored for the second. Not
    re-encoded on the way back out, because "the same JSON" and "the same
    response" are not the same claim and only one of them survives a hash.
    """
    return json.dumps(result, separators=(",", ":"))


def fresh(body: str) -> Response:
    return Response(body, media_type="application/json",
                    headers={"Idempotency-Replayed": "false"})


def replay(row) -> Response:
    """The half of the pattern people skip.

    Not "a 200 because we already did it" — the same status and the same bytes
    the first request was given. A caller that gets an empty 200 back has still
    been broken, it just cannot tell you where.
    """
    return Response(
        row["response_body"],
        status_code=row["response_status"],
        media_type="application/json",
        headers={"Idempotency-Replayed": "true"},
    )


def log(msg: str) -> None:
    print(f"[{config.IDEMPOTENCY_MODE}] {msg}", flush=True)


@app.exception_handler(asyncpg.exceptions.UniqueViolationError)
async def duplicate_key(request: Request, exc: asyncpg.exceptions.UniqueViolationError):
    """What `late` mode looks like from the outside.

    The check-then-insert handler has no try/except around its INSERT, because
    nobody writes one — you do not guard against a constraint you did not think
    could fire. So the request that lost the race charges the customer, then
    dies on the way out with a 500, and the customer is told their payment
    failed. Again.
    """
    print(f"[{config.IDEMPOTENCY_MODE}] DUPLICATE KEY REFUSED -> 500 "
          f"(the charge had already been captured)", flush=True)
    return JSONResponse({"detail": "duplicate key value violates unique constraint"},
                        status_code=500)


@app.post("/api/checkout")
async def checkout(req: CheckoutRequest, idempotency_key: str | None = Header(default=None)):
    """Charge a customer. Once, if the mode allows it."""
    if config.IDEMPOTENCY_MODE == "off" or idempotency_key is None:
        return await checkout_off(req, idempotency_key)
    if config.IDEMPOTENCY_MODE in ("naive", "late"):
        return await checkout_check_then_insert(req, idempotency_key)
    return await checkout_claim_first(req, idempotency_key)


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
