#!/usr/bin/env python3
"""The client. Episode 1's, plus a key, minus the assumption that requests
arrive one at a time.

    python3 scripts/checkout.py 1 2 3 ...          # one at a time, as Episode 1 did
    python3 scripts/checkout.py --concurrent 1 2 3 # all of them at once

Two things changed since Episode 1, and only two.

**It sends an Idempotency-Key.** One key per press of the Pay button, generated
before the first attempt and reused on every retry of that press. This is the
fix the viewer wrote in their head at the end of Episode 1, and it is the right
fix. The key is not the bug.

**It can fire the whole fleet at once.** Episode 1 ran customers sequentially,
which is why its retries never overlapped anything except their own original.
Real traffic is concurrent, and a race that needs concurrency to appear will
not appear in a demo that has none.

The retry policy is untouched: a two-second timeout and one retry, which is
what an HTTP client library does by default.

One addition, and it is not a retry: a `409` answer means the first request
with this key is still running, and the server sent a `Retry-After` saying come
back. Coming back is not a second attempt at the payment; it is asking again
what happened to the first one. Counted separately for exactly that reason.

Standard library only, on purpose: this must be readable by someone who has
never seen the repository before.
"""
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

APP_URL = "http://localhost:8000/api/checkout"
AMOUNT_CENTS = 4000          # $40.00
CLIENT_TIMEOUT_S = 2.0       # what we are willing to wait
MAX_ATTEMPTS = 2             # the original, plus one retry
MAX_CONFLICT_POLLS = 10      # how many times we will ask "is it done yet?"

T0 = time.perf_counter()
PRINT_LOCK = threading.Lock()


def emit(lines: list[str]) -> None:
    """One checkout's whole story, printed in one piece.

    Under --concurrent these finish interleaved. Holding each checkout's lines
    together keeps the log readable without pretending the requests were.
    """
    with PRINT_LOCK:
        for line in lines:
            print(line, flush=True)


def post(customer_id: int, key: str, amount_cents: int) -> tuple[int, dict, dict]:
    """Returns (status, body, headers). Raises on timeout, as Episode 1 did."""
    payload = json.dumps({"customer_id": customer_id, "amount_cents": amount_cents}).encode()
    req = urllib.request.Request(
        APP_URL,
        data=payload,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=CLIENT_TIMEOUT_S) as resp:
            return resp.status, json.load(resp), headers_of(resp)
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            parsed = json.loads(body)
        except ValueError:
            parsed = {"detail": body.decode(errors="replace")[:120]}
        return e.code, parsed, headers_of(e)


def headers_of(resp) -> dict:
    """Header names, lowercased.

    HTTP/1.1 puts them on the wire in whatever case the server used and uvicorn
    uses lower. Looking for "Idempotency-Replayed" finds nothing, silently, and
    every replay in the capture is then reported as a fresh charge.
    """
    return {k.lower(): v for k, v in dict(resp.headers).items()}


class Result:
    __slots__ = ("requests", "timeouts", "conflict_polls", "replayed",
                 "server_errors", "ok", "charge_id")

    def __init__(self):
        self.requests = self.timeouts = self.conflict_polls = 0
        self.replayed = self.server_errors = 0
        self.ok = False
        self.charge_id = None


def checkout(customer_id: int, amount_cents: int = AMOUNT_CENTS) -> Result:
    """One press of Pay. One key, however many requests it takes."""
    key = f"k_{uuid.uuid4().hex[:12]}"
    r = Result()
    lines = []

    def note(msg: str) -> None:
        lines.append(f"  t=+{time.perf_counter() - T0:5.2f}s  customer {customer_id:<3} {msg}")

    for attempt in range(1, MAX_ATTEMPTS + 1):
        started = time.perf_counter()
        try:
            r.requests += 1
            status, body, headers = post(customer_id, key, amount_cents)
        except (TimeoutError, urllib.error.URLError) as e:
            r.timeouts += 1
            note(f"attempt {attempt}  TIMEOUT after {time.perf_counter() - started:.2f}s"
                 f"  ({type(e).__name__})")
            continue

        took = time.perf_counter() - started

        # The first request with this key is still running. Come back, as the
        # server asked. This is not another attempt at paying.
        while status == 409 and r.conflict_polls < MAX_CONFLICT_POLLS:
            note(f"attempt {attempt}  409 IN FLIGHT  ({body.get('error', {}).get('message', '')})")
            time.sleep(float(headers.get("retry-after", 1)))
            r.conflict_polls += 1
            r.requests += 1
            try:
                status, body, headers = post(customer_id, key, amount_cents)
            except (TimeoutError, urllib.error.URLError):
                r.timeouts += 1
                note(f"poll {r.conflict_polls}  TIMEOUT")
                status = 0
                break

        if status == 200:
            replayed = headers.get("idempotency-replayed") == "true"
            r.replayed += 1 if replayed else 0
            r.ok = True
            r.charge_id = body.get("processor_charge_id")
            note(f"attempt {attempt}  OK in {took:.2f}s  -> {r.charge_id}"
                 f"{'  (REPLAYED)' if replayed else ''}")
            emit(lines)
            return r

        if status >= 500:
            r.server_errors += 1
            note(f"attempt {attempt}  {status} SERVER ERROR  {body.get('detail', '')}")
        elif status:
            note(f"attempt {attempt}  {status}  {body.get('error', {}).get('message', body)}")

    note("FAILED  <- the customer is told the payment did not go through")
    emit(lines)
    return r


def main(argv: list[str]) -> None:
    concurrent = "--concurrent" in argv
    ids = [int(a) for a in argv if not a.startswith("--")] or [7]

    if concurrent:
        with ThreadPoolExecutor(max_workers=len(ids)) as pool:
            results = list(pool.map(checkout, ids))
    else:
        results = [checkout(cid) for cid in ids]

    total = lambda f: sum(getattr(r, f) for r in results)  # noqa: E731
    print(
        f"DRIVER checkouts={len(ids)} requests={total('requests')} "
        f"timeouts={total('timeouts')} conflict_polls={total('conflict_polls')} "
        f"replays={total('replayed')} server_errors={total('server_errors')} "
        f"failed={sum(0 if r.ok else 1 for r in results)} "
        f"owed_cents={len(ids) * AMOUNT_CENTS} "
        f"client_timeout_ms={int(CLIENT_TIMEOUT_S * 1000)} amount_cents={AMOUNT_CENTS} "
        f"concurrent={1 if concurrent else 0}",
        flush=True,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
