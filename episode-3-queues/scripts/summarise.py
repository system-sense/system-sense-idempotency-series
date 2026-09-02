"""Turns the raw capture logs into capture/metrics.json.

Episode 1 measured money owed against money collected. Episode 2 measured the
same pair three times against three handlers. This episode measures it against
one handler — Episode 2's, unchanged and still correct — and varies only the
consumer in front of it.

That is the point, so it is also the shape of this file: every scenario reads
the same three books, and the story is which of them disagree.

    lease        the lease expires mid-job          -> jobs run twice
    heartbeat    the lease is held                  -> they stop
    kill_after   a worker dies, ack after the work  -> one job runs twice
    kill_before  the same death, ack before         -> four jobs vanish
    poison       a message that can never succeed   -> forever, and silently
    dlq          the same, with a delivery limit    -> depth becomes a signal
    ordering     one message fails once             -> it finishes last
    keyed        the lease scenario, key on         -> redelivered, charged once

The last one is the payoff and it is the reason `job_runs` exists. Money alone
cannot tell that scenario from a queue that stopped misbehaving, and the queue
did not stop misbehaving.
"""
import json
import pathlib
import re

OUT = pathlib.Path("capture")


def text(name: str) -> str:
    p = OUT / name
    return p.read_text() if p.exists() else ""


def kv(prefix: str, body: str) -> dict:
    """Parse the last `PREFIX k=v k=v ...` line in a log. Ints and floats."""
    found = {}
    for line in body.splitlines():
        if line.startswith(prefix):
            found = {}
            for k, v in re.findall(r"(\w+)=(-?[\d.]+)", line):
                found[k] = float(v) if "." in v else int(v)
    return found


def dollars(cents: int) -> str:
    return f"{cents / 100:,.2f}"


def run(log: str, name: str) -> dict:
    """One scenario: what was published, what the queue did, what the money did."""
    body = text(log)
    sent = kv(f"ENQUEUED label={name} ", body)
    got = kv(f"RESULT {name} ", body)

    owed = sent.get("owed_cents", 0)
    collected = got.get("collected_cents", 0)
    enqueued = got.get("messages_enqueued", sent.get("messages", 0))

    r = {
        "scenario": name,
        "messages_published": enqueued,
        "payable_messages": sent.get("payable", enqueued),
        "poison_messages": sent.get("poison", 0),

        # What the queue did. `deliveries` counts handovers, not jobs.
        "deliveries": got.get("deliveries", 0),
        "messages_delivered": got.get("messages_delivered", 0),
        "jobs_run_twice": got.get("jobs_run_twice", 0),
        "redeliveries": got.get("redeliveries", 0),
        "jobs_never_attempted": got.get("jobs_never_attempted", 0),
        "unfinished_runs": got.get("unfinished_runs", 0),
        "failed_runs": got.get("failed_runs", 0),
        "replays": got.get("replays", 0),

        # What the money did.
        "app_charges": got.get("app_charges", 0),
        "processor_charges": got.get("processor_charges", 0),
        "double_charged_customers": got.get("double_charged_customers", 0),
        "duplicate_charges": got.get("duplicate_charges", 0),
        "owed_cents": owed,
        "collected_cents": collected,
    }

    # Money moved twice, and money that was published and never moved at all.
    # Both are failures; only one of them has anybody watching for it.
    r["overcharge_cents"] = max(collected - owed, 0)
    r["shortfall_cents"] = max(owed - collected, 0)
    r["owed_dollars"] = dollars(owed)
    r["collected_dollars"] = dollars(collected)
    r["overcharge_dollars"] = dollars(r["overcharge_cents"])
    r["shortfall_dollars"] = dollars(r["shortfall_cents"])
    payable = r["payable_messages"] or 1
    r["jobs_run_twice_pct"] = round(100 * r["jobs_run_twice"] / payable, 1)
    # Whatever the last queue reading of this scenario was called. Two of the
    # scenarios never reach a finished state — that is the finding — so they
    # are read at the moment the watch stopped instead.
    for label in (f"{name}-finished ", f"{name}-after-30s ", f"{name} "):
        r["queue_line"] = kv(f"QUEUE {label}", body)
        if r["queue_line"]:
            break
    return r


lease = run("02-lease.log", "lease")
heartbeat = run("04-heartbeat.log", "heartbeat")
kill_after = run("06-kill-after.log", "kill_after")
kill_before = run("08-kill-before.log", "kill_before")
poison = run("10-poison.log", "poison")
dlq = run("12-dlq.log", "dlq")
ordering = run("14-ordering.log", "ordering")
keyed = run("16-keyed.log", "keyed")

# ── The heartbeat's cost ───────────────────────────────────────────────────
# Extending the lease is not free: it is a round trip per job per half-timeout,
# for the whole duration of every job. Worth counting, because "just heartbeat"
# is the answer everyone reaches for and it has a price and a hole in it.
heartbeat["lease_extensions"] = kv("EXTENSIONS ", text("04-heartbeat.log")).get("count", 0)

# ── The two kills ──────────────────────────────────────────────────────────
for r, log_name in ((kill_after, "06-kill-after.log"), (kill_before, "08-kill-before.log")):
    k = kv(f"KILL {r['scenario']} ", text(log_name))
    r["killed_at_seconds"] = k.get("at_seconds", 0.0)
    r["batch"] = k.get("batch", 0)
    r["pending_after_the_kill"] = kv(
        f"QUEUE {r['scenario']}-after-the-kill ", text(log_name)
    ).get("pending", 0)

# ── The poison message ─────────────────────────────────────────────────────
p = kv("POISON ", text("10-poison.log"))
# Counted with the worker stopped, so the figure holds still. It is not "16
# deliveries and then it settled" — the worker was switched off mid-redelivery
# and the entry was pending when it was.
poison["deliveries_observed"] = p.get("deliveries", 0)
poison["residency_seconds"] = p.get("residency_seconds", 0.0)
poison["dead_lettered"] = p.get("dead_lettered", 0)
w = kv("DLQWATCH poison ", text("10-poison.log"))
poison["watch_seconds"] = w.get("seconds", 0)
poison["max_dlq_depth"] = w.get("max_depth", 0)
poison["first_alert_seconds"] = w.get("first_alert_s", -1)

d = kv("DLQ ", text("12-dlq.log"))
dlq["deliveries_before_dead_letter"] = d.get("deliveries_before_dlq", 0)
dlq["seconds_to_dead_letter"] = d.get("seconds_to_dlq", 0.0)
dlq["dead_letter_depth"] = d.get("depth", 0)
wd = kv("DLQWATCH dlq ", text("12-dlq.log"))
dlq["watch_seconds"] = wd.get("seconds", 0)
dlq["first_alert_seconds"] = wd.get("first_alert_s", -1)


# ── Ordering ───────────────────────────────────────────────────────────────
def completed_order(body: str) -> list[int]:
    m = re.findall(r"^ORDER completed=([\d,]+)", body, re.M)
    return [int(x) for x in m[-1].split(",")] if m else []


def inversions(seq: list[int]) -> int:
    """Pairs finished out of the order they were published in."""
    return sum(1 for i in range(len(seq)) for j in range(i + 1, len(seq)) if seq[i] > seq[j])


order = completed_order(text("14-ordering.log"))
ordering["published_order"] = list(range(1, ordering["messages_published"] + 1))
ordering["completed_order"] = order
ordering["out_of_order_pairs"] = inversions(order)
ordering["retried_message_finished_at_position"] = (
    order.index(5) + 1 if 5 in order else 0
)

# ── The scenario's own settings, read back from the run rather than assumed ──
up = text("01-compose-up.log")
lat = re.findall(r"^(\d+) (\d+)\s*$", up, re.M)
base, spread = (int(lat[-1][0]), int(lat[-1][1])) if lat else (0, 0)
mode = re.search(r'"mode":"(\w+)"', up)
defaults = re.search(
    r"worker defaults = (\w+) (\d+) (\d+) (\d+) (\d+) (\d+)", up
)

metrics = {
    "setup": {
        "amount_cents": kv("ENQUEUED label=lease ", text("02-lease.log")).get("amount_cents", 0),
        "amount_dollars": dollars(kv("ENQUEUED label=lease ", text("02-lease.log")).get("amount_cents", 0)),
        # The endpoint in front of which all of this happens. It is Episode 2's,
        # in the mode that cannot charge twice for one key, and it never changes.
        "app_idempotency_mode": mode.group(1) if mode else "",
        "visibility_timeout_ms": int(defaults.group(2)) if defaults else 0,
        "worker_ack_mode_default": defaults.group(1) if defaults else "",
        "workers": 2,
        "processor_latency_base_ms": base,
        "processor_latency_spread_ms": spread,
        "processor_latency_min_ms": base,
        "processor_latency_max_ms": base + spread - 1 if spread else 0,
        # Jobs whose payment outlives a two-second lease, by construction:
        # 1200 + (id * 137) % 2400 > 2000.
        "customers_slower_than_lease": sum(
            1 for i in range(1, 26) if base + (i * 137) % spread > 2000
        ) if spread else 0,
    },
    "lease": lease,
    "heartbeat": heartbeat,
    "kill_after": kill_after,
    "kill_before": kill_before,
    "poison": poison,
    "dlq": dlq,
    "ordering": ordering,
    "keyed": keyed,
}

(OUT / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
print(json.dumps(metrics, indent=2))
