#!/usr/bin/env python3
"""The client. Places orders, one at a time, and never retries one.

    python3 scripts/place-orders.py --customers $(seq 1 12)
    python3 scripts/place-orders.py --customers 1 2 3 --label smoke

This replaces Episode 3's `enqueue.py`, and the replacement is the episode.
`enqueue.py` was the producer: it published the work itself. Here the producer
is a service with a database, and this script is only the thing that asks it for
an order.

**It does not retry.** A request that dies with the process gets counted as a
reset and abandoned, which is not what a real client would do — a real client
would come back, with Episode 2's key, and get the order it already has. That is
the previous episode, and putting it here would hide this one: a retry would
paper over the lost event and the whole point is that nothing papers over it.
Nobody retries. The order is committed. The event is simply gone.

It DOES wait for the producer to come back before sending the next order, which
is not a retry of the failed one. It is the next customer, arriving after the
restart, finding the site up again — because it is, in about two seconds, which
is why nobody notices.

Standard library only, on purpose (see scripts/resp.py).
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request

AMOUNT_CENTS = 4000     # $40.00, as in Episodes 1, 2 and 3


def post(url: str, body: dict, timeout: float = 60.0) -> tuple[int, dict]:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read() or b"{}")


def wait_up(base: str, seconds: float = 30.0) -> bool:
    """Wait for the producer to answer /health again after it was killed."""
    t0 = time.time()
    while time.time() - t0 < seconds:
        try:
            with urllib.request.urlopen(f"{base}/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.25)
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, nargs="*", default=[])
    ap.add_argument("--amount-cents", type=int, default=AMOUNT_CENTS)
    ap.add_argument("--label", default="run")
    ap.add_argument("--base", default="http://localhost:8100")
    args = ap.parse_args()

    accepted = reset = 0
    owed = 0

    for seq, cid in enumerate(args.customers, start=1):
        body = {"seq": seq, "customer_id": cid, "amount_cents": args.amount_cents}
        owed += args.amount_cents
        try:
            status, out = post(f"{args.base}/api/orders", body)
            accepted += 1
            where = out.get("message_id") or f"outbox={out.get('outbox_id')}"
            print(f"  seq={seq:<3} customer {cid:<3} {status}  order={out.get('order_id')}  "
                  f"key={out.get('key')}  {where}")
        except (urllib.error.URLError, urllib.error.HTTPError, ConnectionError, OSError) as e:
            # The process died with the request in it. From out here this is
            # indistinguishable from the order never having been placed — and
            # in one of the three modes the order IS placed, and paid for, and
            # in another it is placed and never paid for. The client cannot
            # tell, and neither can the customer.
            reset += 1
            print(f"  seq={seq:<3} customer {cid:<3} ---  NO RESPONSE "
                  f"({type(e).__name__}) — the process died with this request in it")
            if not wait_up(args.base):
                print("  producer did not come back", file=sys.stderr)
                break

    print(f"PLACED label={args.label} orders_sent={len(args.customers)} accepted={accepted} "
          f"no_response={reset} owed_cents={owed} amount_cents={args.amount_cents}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
