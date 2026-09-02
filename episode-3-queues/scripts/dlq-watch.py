#!/usr/bin/env python3
"""Something that alerts on the dead-letter queue.

    python3 scripts/dlq-watch.py --seconds 40 --label poison

A dead-letter queue nobody watches is a bin. This is the smallest possible
version of not-a-bin: it samples the depth once a second and says something the
first time the depth is non-zero.

The alert is deliberately not "depth > 100". A dead-letter queue is not a
capacity problem, it is a correctness signal: one message in it means one piece
of work this system accepted and will never do. The threshold is one.

It also samples `pending`, because the failure mode this episode is about does
not show up in the DLQ at all until somebody adds a delivery limit. Before that
the poison message is pending, forever, and every dashboard is green.
"""
import argparse
import sys
import time

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from resp import Redis, RedisError, pairs  # noqa: E402


def groups(r):
    """XINFO GROUPS, or nothing. A stream that does not exist yet is a normal
    state between scenarios, not an error worth a traceback."""
    try:
        return r.cmd("XINFO", "GROUPS", STREAM) or []
    except RedisError:
        return []

STREAM, GROUP, DLQ = "checkouts", "payments", "checkouts:dead"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--label", default="watch")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    t0 = time.time()
    first_alert = None
    max_depth = 0
    samples = 0

    print(f"-- dead-letter watch, {args.seconds:.0f}s   (threshold: one message)")
    with Redis(args.host, args.port) as r:
        while time.time() - t0 < args.seconds:
            elapsed = time.time() - t0
            depth = r.cmd("XLEN", DLQ)
            pending = 0
            for g in groups(r):
                info = pairs(g)
                if info.get("name") == GROUP:
                    pending = int(info.get("pending", 0))
            samples += 1
            max_depth = max(max_depth, depth)
            flag = ""
            if depth and first_alert is None:
                first_alert = elapsed
                flag = "   *** ALERT: work this system accepted will never be done ***"
            print(f"   t=+{elapsed:5.1f}s  dead-letter depth {depth}   pending {pending}{flag}",
                  flush=True)
            time.sleep(1.0)

    print(f"DLQWATCH {args.label} seconds={args.seconds:.0f} samples={samples} "
          f"max_depth={max_depth} first_alert_s={-1 if first_alert is None else round(first_alert, 1)}",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
