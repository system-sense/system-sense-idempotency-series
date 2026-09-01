#!/usr/bin/env python3
"""The client. Sixteen lines of it are the entire bug.

    python3 scripts/checkout.py 1 2 3 ...      # one checkout per customer id

Nothing here is a straw man. A two-second timeout with one retry is what an
HTTP client library does by default, what a mobile app does when the user is on
a train, and what a load balancer does when an upstream is slow. The retry is
not a mistake — without it, one dropped packet loses the sale.

The mistake is the assumption underneath it: that a timeout means the request
did not happen. It means the RESPONSE did not arrive. The request may have
completed perfectly, and taken the customer's money on the way.

Standard library only, on purpose: this must be readable by someone who has
never seen the repository before.
"""
import json
import sys
import time
import urllib.error
import urllib.request

APP_URL = "http://localhost:8000/api/checkout"
AMOUNT_CENTS = 4000          # $40.00
CLIENT_TIMEOUT_S = 2.0       # what we are willing to wait
MAX_ATTEMPTS = 2             # the original, plus one retry


def checkout(customer_id: int) -> tuple[int, int, bool]:
    """Returns (attempts_made, timeouts, succeeded)."""
    payload = json.dumps({"customer_id": customer_id, "amount_cents": AMOUNT_CENTS}).encode()
    timeouts = 0

    for attempt in range(1, MAX_ATTEMPTS + 1):
        req = urllib.request.Request(
            APP_URL, data=payload, headers={"Content-Type": "application/json"}, method="POST"
        )
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=CLIENT_TIMEOUT_S) as resp:
                body = json.load(resp)
            took = time.perf_counter() - started
            print(f"  customer {customer_id:<3} attempt {attempt}  OK in {took:.2f}s"
                  f"  -> {body['processor_charge_id']}", flush=True)
            return attempt, timeouts, True
        except (TimeoutError, urllib.error.URLError) as e:
            took = time.perf_counter() - started
            timeouts += 1
            print(f"  customer {customer_id:<3} attempt {attempt}  TIMEOUT after {took:.2f}s"
                  f"  ({type(e).__name__})", flush=True)

    print(f"  customer {customer_id:<3} FAILED  <- the customer is told the payment did not go through",
          flush=True)
    return MAX_ATTEMPTS, timeouts, False


def main(ids: list[int]) -> None:
    requests = timeouts = failed = 0
    for cid in ids:
        attempts, t, ok = checkout(cid)
        requests += attempts
        timeouts += t
        failed += 0 if ok else 1

    print(
        f"DRIVER checkouts={len(ids)} requests={requests} timeouts={timeouts} "
        f"failed={failed} owed_cents={len(ids) * AMOUNT_CENTS} "
        f"client_timeout_ms={int(CLIENT_TIMEOUT_S * 1000)} amount_cents={AMOUNT_CENTS}",
        flush=True,
    )


if __name__ == "__main__":
    main([int(a) for a in sys.argv[1:]] or [7])
