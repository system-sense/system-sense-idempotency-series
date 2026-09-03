"""Turns the raw capture logs into capture/metrics.json.

Episode 1 measured money owed against money collected. Episode 2 measured the
same pair against three handlers. Episode 3 measured it against one handler and
varied the consumer. This episode holds BOTH of those fixed — Episode 2's
endpoint, Episode 3's finished consumer — and varies only what happens between
the database and the queue, upstream of either of them.

So the pair that matters here is a new one, and it is the pair nobody has:

    orders committed        what the business believes happened
    events on the stream    what anything downstream will ever hear about

    commit_first        COMMIT then publish, killed between  -> events lost
    publish_first       publish then COMMIT, same kill       -> phantom orders
    outbox              one transaction, same kill           -> nothing lost
    relay_crash         the relay dies before marking sent   -> published twice
    relay_crash_keyed   the same, Episode 2's key on         -> charged once
    agent_payload       a replayed run keyed on content      -> charged 3x
    agent_position      keyed on (run_id, step, action)      -> charged once
    everything          all of it, killed everywhere at once

The last two pairs are the argument. `relay_crash` and `relay_crash_keyed`
publish the same number of duplicate events; only one of them takes the money
twice. That is exactly-once EFFECTS, and it is the only kind there is.
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


def scenario(log: str, name: str) -> dict:
    """One scenario: what was asked for, what survived, and what the money did."""
    body = text(log)
    sent = kv(f"PLACED label={name} ", body)
    rec = kv(f"RECONCILE {name} ", body)
    got = kv(f"RESULT {name} ", body)

    owed = rec.get("owed_cents", 0)
    collected = rec.get("collected_cents", 0)

    r = {
        "scenario": name,

        # The client's side. `no_response` is a request that died with the
        # process in the middle of it: from out there, indistinguishable from
        # an order that was never placed.
        "orders_sent": sent.get("orders_sent", 0),
        "accepted": sent.get("accepted", 0),
        "no_response": sent.get("no_response", 0),

        # The two books that cannot be written atomically.
        "orders_committed": rec.get("orders", 0),
        "events_published": rec.get("events", 0),
        "distinct_keys": rec.get("distinct_keys", 0),
        "events_lost": rec.get("events_lost", 0),
        "phantom_events": rec.get("phantom_events", 0),
        "duplicate_publishes": rec.get("duplicate_publishes", 0),

        # The outbox itself.
        "outbox_rows": rec.get("outbox_rows", 0),
        "outbox_unsent": rec.get("outbox_unsent", 0),
        "outbox_published_twice": rec.get("outbox_published_twice", 0),

        # What Episode 3's consumer did with whatever reached it.
        "deliveries": got.get("deliveries", 0),
        "messages_delivered": got.get("messages_delivered", 0),
        "replays": got.get("replays", 0),
        "failed_runs": got.get("failed_runs", 0),

        # What actually happened to money.
        "app_charges": got.get("app_charges", 0),
        "processor_charges": rec.get("charges", 0),
        "double_charged_customers": got.get("double_charged_customers", 0),
        "duplicate_charges": got.get("duplicate_charges", 0),
        "owed_cents": owed,
        "collected_cents": collected,
    }

    # Money taken twice, and money that was committed and never taken at all.
    # Both are failures. Only one of them has anybody watching for it, and it
    # is not the one that loses you the revenue.
    r["overcharge_cents"] = max(collected - owed, 0)
    r["shortfall_cents"] = max(owed - collected, 0)
    r["owed_dollars"] = dollars(owed)
    r["collected_dollars"] = dollars(collected)
    r["overcharge_dollars"] = dollars(r["overcharge_cents"])
    r["shortfall_dollars"] = dollars(r["shortfall_cents"])
    ordered = r["orders_committed"] or 1
    r["events_lost_pct"] = round(100 * r["events_lost"] / ordered, 1)
    return r


def agent(log: str, name: str) -> dict:
    """One agent scenario. Same twelve attempts; only the key is different."""
    body = text(log)
    a = kv(f"AGENT label={name} ", body)
    m = kv(f"AGENTMONEY {name} ", body)
    owed = a.get("owed_cents", 0)
    collected = m.get("collected_cents", 0)
    r = {
        "scenario": name,
        "runs": a.get("runs", 0),
        "replays_each": a.get("replays", 0),
        "attempts": a.get("attempts", 0),
        # The whole reason payload keying cannot work: the stub model returned a
        # different string on every one of these calls, so a hash of the step's
        # output is a different key every time.
        "distinct_model_outputs": a.get("distinct_notes", 0),
        "charged": a.get("charged", 0),
        "replayed": a.get("replayed", 0),
        "conflicts": a.get("conflicts", 0),
        "processor_charges": m.get("charges", 0),
        "double_charged_customers": m.get("double_charged_customers", 0),
        "duplicate_charges": m.get("duplicate_charges", 0),
        "owed_cents": owed,
        "collected_cents": collected,
    }
    r["overcharge_cents"] = max(collected - owed, 0)
    r["owed_dollars"] = dollars(owed)
    r["collected_dollars"] = dollars(collected)
    r["overcharge_dollars"] = dollars(r["overcharge_cents"])
    return r


commit_first = scenario("02-commit-first.log", "commit_first")
publish_first = scenario("04-publish-first.log", "publish_first")
outbox = scenario("06-outbox.log", "outbox")
relay_crash = scenario("08-relay-crash.log", "relay_crash")
relay_crash_keyed = scenario("10-relay-crash-keyed.log", "relay_crash_keyed")
everything = scenario("14-books.log", "everything")
agent_payload = agent("12-agent-payload.log", "agent_payload")
agent_position = agent("13-agent-position.log", "agent_position")

# ── What the outbox costs ──────────────────────────────────────────────────
# The event is no longer published in the request. It is published by somebody
# else, afterwards, and that gap is the price of the pattern. Worth measuring
# rather than waving at: it is the one honest objection to the outbox and it is
# smaller than people expect.
lag = kv("OUTBOX_LAG_MS ", text("06-outbox.log"))
outbox["relay_lag_median_ms"] = lag.get("median", 0.0)
outbox["relay_lag_max_ms"] = lag.get("max", 0.0)

# ── The producer's kills, counted from its own log ─────────────────────────
for r, services_log in ((commit_first, "03-commit-first-services.log"),
                        (publish_first, "05-publish-first-services.log"),
                        (outbox, "07-outbox-services.log")):
    r["producer_kills"] = len(re.findall(r"\*\*\* KILLED", text(services_log)))

for r, services_log in ((relay_crash, "09-relay-crash-services.log"),
                        (relay_crash_keyed, "11-relay-crash-keyed-services.log")):
    r["relay_kills"] = len(re.findall(r"\*\*\* KILLED", text(services_log)))

# ── The settings the run actually used, read back rather than assumed ──────
up = text("01-compose-up.log")
lat = re.findall(r"^(\d+) (\d+)\s*$", up, re.M)
base, spread = (int(lat[-1][0]), int(lat[-1][1])) if lat else (0, 0)
mode = re.search(r'"mode":"(\w+)"', up)
defaults = re.search(r"worker defaults = (\w+) (\d+) (\d+) (\d+) (\d+) (\d+)", up)
setup = kv("SETUP ", up)

metrics = {
    "setup": {
        "orders": setup.get("orders_in_fleet", 0),
        "crash_every": setup.get("crash_every", 0),
        "expected_kills": setup.get("expected_kills", 0),
        "relay_crashes": setup.get("relay_crashes", 0),
        "amount_cents": kv("PLACED label=commit_first ", text("02-commit-first.log")).get("amount_cents", 0),
        "amount_dollars": dollars(kv("PLACED label=commit_first ", text("02-commit-first.log")).get("amount_cents", 0)),

        # The endpoint and the consumer in front of which all of this happens.
        # Episode 2's, in the mode that cannot charge twice for one key, and
        # Episode 3's, with the lease held and the key passed on. Neither one
        # changes anywhere in this episode.
        "app_idempotency_mode": mode.group(1) if mode else "",
        "worker_ack_mode": defaults.group(1) if defaults else "",
        "visibility_timeout_ms": int(defaults.group(2)) if defaults else 0,
        "worker_heartbeat": int(defaults.group(3)) if defaults else 0,
        "worker_max_deliveries": int(defaults.group(4)) if defaults else 0,
        "worker_idempotent_consumer": int(defaults.group(5)) if defaults else 0,
        "workers": 2,
        "processor_latency_base_ms": base,
        "processor_latency_spread_ms": spread,
        "processor_latency_min_ms": base,
        "processor_latency_max_ms": base + spread - 1 if spread else 0,
    },
    "commit_first": commit_first,
    "publish_first": publish_first,
    "outbox": outbox,
    "relay_crash": relay_crash,
    "relay_crash_keyed": relay_crash_keyed,
    "agent_payload": agent_payload,
    "agent_position": agent_position,
    "everything": everything,
}

(OUT / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
print(json.dumps(metrics, indent=2))
