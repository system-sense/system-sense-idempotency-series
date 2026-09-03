#!/usr/bin/env python3
"""What the queue thinks is going on. XLEN, XINFO GROUPS, XPENDING.

    python3 scripts/queue-state.py --label after-the-kill

This is the instrument panel, and reading it is most of the episode. Three
numbers matter and only one of them is on anybody's dashboard:

  length     entries on the stream. Goes up when work arrives, and NEVER goes
             down on acknowledgement — a Redis Stream is a log. Watching this
             for queue health is watching the wrong number.
  lag        entries the group has never been handed. This is "work waiting".
  pending    entries handed to a consumer and not acknowledged. This is "work
             claimed by somebody who has not finished, or never will".

A worker that died holding ten entries shows lag=0 and pending=10, and a
dashboard plotting length sees a flat line.
"""
import argparse
import sys

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
    ap.add_argument("--label", default="queue")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    with Redis(args.host, args.port) as r:
        length = r.cmd("XLEN", STREAM)
        dlq = r.cmd("XLEN", DLQ)

        pending = lag = consumers = 0
        for g in groups(r):
            info = pairs(g)
            if info.get("name") == GROUP:
                pending = int(info.get("pending", 0))
                lag = int(info.get("lag") or 0)
                consumers = int(info.get("consumers", 0))

        print(f"-- {args.label}")
        print(f"   stream length      {length}   (a log: acknowledging does not shorten it)")
        print(f"   never delivered    {lag}   <- work waiting")
        print(f"   pending            {pending}   <- delivered, not acknowledged")
        print(f"   consumers          {consumers}")
        print(f"   dead-letter depth  {dlq}")

        entries = r.cmd("XPENDING", STREAM, GROUP, "-", "+", 20) or []
        if entries:
            print("   XPENDING:")
            print(f"      {'entry':<20} {'held by':<10} {'idle ms':>9} {'deliveries':>11}")
            for e in entries:
                mid, consumer, idle, delivered = e[0], e[1], int(e[2]), int(e[3])
                print(f"      {mid:<20} {consumer:<10} {idle:>9} {delivered:>11}")

    print(f"QUEUE {args.label} length={length} lag={lag} pending={pending} dlq={dlq} "
          f"consumers={consumers}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
