#!/usr/bin/env python3
"""The two books, side by side: what the database committed, and what the queue
was actually told.

    python3 scripts/reconcile.py --label after-the-kills

Episode 1 put our ledger beside the processor's. Episode 3 added what the queue
did, delivery by delivery. This is the pair the finale is about, and it is the
pair nobody has:

    orders in PostgreSQL      what the business believes happened
    events on the stream      what anything downstream will ever hear about

They are written by two different systems, and there is no transaction that
spans them. Every number below is a way for them to disagree:

    events lost         an order was committed and no event was ever published.
                        Nothing downstream will ever run. Nothing will retry,
                        because nothing knows there is anything to retry. The
                        order sits in the database looking completely normal.

    phantom events      an event was published for an order that was never
                        committed. Money moves for something support cannot
                        find. This is what you get for publishing first, and it
                        is worse than losing the event.

    duplicate publishes one intent, two message ids. The relay's own failure
                        mode, and the harmless one — IF the far end is keyed.

The reconciliation itself is the tell. A system that needs this script has
already lost the argument: you cannot write a reconciler for a failure whose
whole nature is that neither side knows it happened. This one only works because
the key is on both sides of it, which is the fix, not the diagnosis.

Standard library only: Redis over RESP (scripts/resp.py), Postgres through the
psql that is already inside the container.
"""
import argparse
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from resp import Redis, pairs  # noqa: E402

STREAM = "checkouts"


def psql(sql: str) -> list[list[str]]:
    """One query, through the container's own psql. No driver, no pip install."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "sysense", "-d", "sysense", "-At", "-F", "\t", "-c", sql],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise SystemExit(out.stderr.strip() or "psql failed")
    return [line.split("\t") for line in out.stdout.strip().splitlines() if line]


def scalar(sql: str) -> int:
    rows = psql(sql)
    return int(rows[0][0]) if rows and rows[0][0] else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="reconcile")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--show", type=int, default=8, help="how many mismatched rows to print")
    args = ap.parse_args()

    # ── The database's side ────────────────────────────────────────────────
    orders = {k: (int(seq), int(cid), int(cents))
              for k, seq, cid, cents in
              psql("SELECT order_key, seq, customer_id, amount_cents FROM orders ORDER BY seq")}
    owed = scalar("SELECT coalesce(sum(amount_cents), 0) FROM orders")
    collected = scalar("SELECT coalesce(sum(amount_cents), 0) FROM processor.ledger")
    charges = scalar("SELECT count(*) FROM processor.ledger")
    outbox_rows = scalar("SELECT count(*) FROM outbox")
    outbox_unsent = scalar("SELECT count(*) FROM outbox WHERE published_at IS NULL")
    outbox_twice = scalar("SELECT count(*) FROM outbox WHERE publish_attempts > 1")

    # ── The queue's side ───────────────────────────────────────────────────
    # XRANGE over the whole stream. A Redis Stream is a log, so this is every
    # event that was ever published, acknowledged or not.
    seen: dict[str, list[str]] = {}
    with Redis(args.host, args.port) as r:
        for entry in (r.cmd("XRANGE", STREAM, "-", "+") or []):
            mid, fields = entry[0], pairs(entry[1])
            seen.setdefault(fields.get("key", ""), []).append(mid)

    events = sum(len(v) for v in seen.values())
    lost = [k for k in orders if k not in seen]
    phantom = [k for k in seen if k not in orders]
    duplicated = {k: v for k, v in seen.items() if len(v) > 1}
    dup_publishes = sum(len(v) - 1 for v in duplicated.values())

    print(f"-- {args.label}")
    print(f"   orders committed        {len(orders):>4}")
    print(f"   events on the stream    {events:>4}   ({len(seen)} distinct keys)")
    print(f"   events lost             {len(lost):>4}   <- committed, never published")
    print(f"   phantom events          {len(phantom):>4}   <- published, never committed")
    print(f"   duplicate publishes     {dup_publishes:>4}   <- one intent, two message ids")
    print(f"   outbox rows             {outbox_rows:>4}   ({outbox_unsent} unsent, "
          f"{outbox_twice} published more than once)")
    print(f"   charges                 {charges:>4}")
    print(f"   owed  ${owed / 100:>10,.2f}")
    print(f"   taken ${collected / 100:>10,.2f}")

    if lost:
        print("   -- orders nothing will ever pay for")
        for k in lost[:args.show]:
            seq, cid, cents = orders[k]
            print(f"      seq={seq:<3} customer {cid:<3} ${cents / 100:>7,.2f}  key={k}")
    if phantom:
        print("   -- money moved for orders that are not in the database")
        for k in phantom[:args.show]:
            print(f"      key={k}  message={','.join(seen[k])}")
    if duplicated:
        print("   -- one order, published more than once")
        for k, mids in list(duplicated.items())[:args.show]:
            seq, cid, _ = orders.get(k, (0, 0, 0))
            print(f"      seq={seq:<3} customer {cid:<3} key={k}  messages={' + '.join(mids)}")

    print(f"RECONCILE {args.label} orders={len(orders)} events={events} "
          f"distinct_keys={len(seen)} events_lost={len(lost)} phantom_events={len(phantom)} "
          f"duplicate_publishes={dup_publishes} outbox_rows={outbox_rows} "
          f"outbox_unsent={outbox_unsent} outbox_published_twice={outbox_twice} "
          f"charges={charges} owed_cents={owed} collected_cents={collected}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
