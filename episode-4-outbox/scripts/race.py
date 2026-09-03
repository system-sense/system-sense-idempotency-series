#!/usr/bin/env python3
"""Two requests, one key, a measured distance apart.

    python3 scripts/race.py --customer 17 --gap-ms 3      # B arrives mid-flight
    python3 scripts/race.py --customer 18 --gap-ms 2000   # B arrives after A finished
    python3 scripts/race.py --customer 18 --gap-ms 2000 --different-body

The fleet driver measures how much money moved. This measures what the server
said, which is the other half of the pattern and the half people skip: a retry
has to be given the SAME answer as the request it is retrying, not merely
spared the work.

So both responses are printed in full and hashed, and the hashes are compared
byte for byte. "Both returned 200" is not the claim being made here.

Unlike scripts/checkout.py this is not pretending to be a client — it waits as
long as the server needs (30s), because the point is to see both answers rather
than to reproduce a timeout.
"""
import argparse
import hashlib
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

APP_URL = "http://localhost:8000/api/checkout"
PROBE_TIMEOUT_S = 30.0


def headers_of(resp) -> dict:
    """Header names, lowercased — uvicorn sends them that way on HTTP/1.1."""
    return {k.lower(): v for k, v in dict(resp.headers).items()}


def warm_up() -> None:
    """Send one throwaway request before the race starts.

    urllib builds its default opener on first use, and that build is slow enough
    to swallow a three-millisecond head start. Measured: without this, the
    request fired at t=0 lost to the one fired at t=3ms about half the time,
    and the capture recorded the two of them the wrong way round.
    """
    try:
        urllib.request.urlopen("http://localhost:8000/health", timeout=5).read()
    except Exception:
        pass


def post(customer_id: int, key: str, amount_cents: int):
    payload = json.dumps({"customer_id": customer_id, "amount_cents": amount_cents}).encode()
    req = urllib.request.Request(
        APP_URL,
        data=payload,
        headers={"Content-Type": "application/json", "Idempotency-Key": key},
        method="POST",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT_S) as resp:
            raw = resp.read()
            return resp.status, raw, headers_of(resp), time.perf_counter() - started
    except urllib.error.HTTPError as e:
        return e.code, e.read(), headers_of(e), time.perf_counter() - started
    except (TimeoutError, urllib.error.URLError) as e:
        return 0, f'{{"detail":"{type(e).__name__}"}}'.encode(), {}, time.perf_counter() - started


def show(name: str, status: int, raw: bytes, headers: dict, took: float) -> str:
    sha = hashlib.sha256(raw).hexdigest()[:16]
    replayed = headers.get("idempotency-replayed", "-")
    print(f"  {name}  HTTP {status}  in {took:5.2f}s  Idempotency-Replayed: {replayed}")
    print(f"  {name}  body   {raw.decode(errors='replace')}")
    print(f"  {name}  sha256 {sha}")
    return sha


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customer", type=int, default=17)
    ap.add_argument("--gap-ms", type=int, default=3)
    ap.add_argument("--amount-cents", type=int, default=4000)
    ap.add_argument("--different-body", action="store_true",
                    help="B reuses A's key with a different amount — a client bug")
    ap.add_argument("--label", default="race")
    args = ap.parse_args()

    key = f"k_{uuid.uuid4().hex[:12]}"
    print(f"-- {args.label}: customer {args.customer}, one key ({key}), "
          f"two requests {args.gap_ms} ms apart")

    warm_up()
    out: dict = {}

    def fire(name: str, delay_s: float, amount: int) -> None:
        time.sleep(delay_s)
        out[name] = post(args.customer, key, amount)

    b_amount = args.amount_cents + 500 if args.different_body else args.amount_cents
    threads = [
        threading.Thread(target=fire, args=("A", 0.0, args.amount_cents)),
        threading.Thread(target=fire, args=("B", args.gap_ms / 1000, b_amount)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    a_status, a_raw, a_headers, a_took = out["A"]
    b_status, b_raw, b_headers, b_took = out["B"]
    a_sha = show("A", a_status, a_raw, a_headers, a_took)
    b_sha = show("B", b_status, b_raw, b_headers, b_took)

    identical = 1 if a_raw == b_raw else 0
    print(f"  bodies identical: {'YES' if identical else 'no'}")
    print(
        f"RACE {args.label} gap_ms={args.gap_ms} customer={args.customer} "
        f"a_status={a_status} b_status={b_status} "
        f"b_replayed={1 if b_headers.get('idempotency-replayed') == 'true' else 0} "
        f"bodies_identical={identical} a_sha={a_sha} b_sha={b_sha}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
