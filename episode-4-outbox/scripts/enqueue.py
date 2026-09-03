#!/usr/bin/env python3
"""The producer. Puts work on the queue and walks away.

    python3 scripts/enqueue.py --customers $(seq 1 25)
    python3 scripts/enqueue.py --customers 17 16 15 14 13     # the slow ones
    python3 scripts/enqueue.py --poison                       # one bad payload
    python3 scripts/enqueue.py --customers 18 19 20 --fail-seq 2 --fail-times 1

This replaces Episode 2's client, and the replacement is the episode. Episode
2's client made a decision to retry: it set a timeout, it caught the failure, it
came back, and it kept its key while it did. Every one of those is a line you
could read in its source.

Nothing here retries. This process XADDs and exits. Whatever happens after that
happens because a queue decided it should, on a schedule nobody in this file
knows about.

**The key is minted here**, one per job, and carried in the message. It has to
be: a consumer cannot tell delivery two from delivery one, so a key generated on
the consumer side is a fresh key every time and protects nothing. This is the
producer saying "these deliveries are all the same intent" — which it is the
only party in the system that knows.

Standard library only, on purpose (see scripts/resp.py).
"""
import argparse
import sys
import time
import uuid

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from resp import Redis  # noqa: E402

STREAM = "checkouts"
AMOUNT_CENTS = 4000     # $40.00, as in Episodes 1 and 2


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, nargs="*", default=[],
                    help="one job per customer id")
    ap.add_argument("--amount-cents", type=int, default=AMOUNT_CENTS)
    ap.add_argument("--poison", action="store_true",
                    help="also enqueue one message this consumer can never process")
    ap.add_argument("--poison-first", action="store_true",
                    help="put the poison message at the head of the queue")
    ap.add_argument("--fail-seq", type=int, default=0,
                    help="make this sequence number fail its first deliveries")
    ap.add_argument("--fail-times", type=int, default=1)
    ap.add_argument("--label", default="run")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    jobs = []
    seq = 0

    def add(customer_id: int, amount: int, fail_times: int = 0) -> None:
        nonlocal seq
        seq += 1
        jobs.append({
            "seq": seq,
            "customer_id": customer_id,
            "amount_cents": amount,
            # One key per job, generated once, here, before anything is sent.
            "key": f"k_{uuid.uuid4().hex[:12]}",
            "fail_times": fail_times,
            "enqueued_at": f"{time.time():.3f}",
        })

    # amount_cents = -1 is the poison: a payload the consumer rejects on sight
    # and will reject identically every time it is redelivered. Not a transient
    # failure. No number of retries converts it into one.
    if args.poison and args.poison_first:
        add(0, -1)
    for cid in args.customers:
        add(cid, args.amount_cents,
            args.fail_times if args.fail_seq and seq + 1 == args.fail_seq else 0)
    if args.poison and not args.poison_first:
        add(0, -1)

    owed = sum(j["amount_cents"] for j in jobs if j["amount_cents"] > 0)

    with Redis(args.host, args.port) as r:
        for j in jobs:
            fields = []
            for k, v in j.items():
                fields += [k, v]
            mid = r.cmd("XADD", STREAM, "*", *fields)
            kind = "POISON" if j["amount_cents"] <= 0 else f"customer {j['customer_id']}"
            fails = f"  fails its first {j['fail_times']} deliveries" if j["fail_times"] else ""
            print(f"  XADD seq={j['seq']:<3} {mid}  {kind}  key={j['key']}{fails}")

    print(
        f"ENQUEUED label={args.label} messages={len(jobs)} "
        f"payable={sum(1 for j in jobs if j['amount_cents'] > 0)} "
        f"poison={sum(1 for j in jobs if j['amount_cents'] <= 0)} "
        f"owed_cents={owed} amount_cents={args.amount_cents}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
