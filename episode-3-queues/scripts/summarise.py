"""Turns the raw capture logs into capture/metrics.json.

Episode 1 measured one thing: money owed against money collected. This episode
measures that same pair three times over, against three handlers that were
given the identical load, and the story is in how little the first two changed
it:

    naive   the obvious fix, no constraint     -> the duplicates survive
    late    the identical handler, constrained -> the duplicates survive, loudly
    claim   the key claimed before the work    -> one charge each

Plus the two things the money cannot tell you: whether the retry got the SAME
answer as the request it was retrying, and what happens when a key expires.
"""
import json
import pathlib
import re

OUT = pathlib.Path("capture")


def text(name: str) -> str:
    p = OUT / name
    return p.read_text() if p.exists() else ""


def kv(line_prefix: str, body: str) -> dict:
    """Parse the last `PREFIX k=v k=v ...` line in a log. Ints and floats."""
    found = {}
    for line in body.splitlines():
        if line.startswith(line_prefix):
            found = {}
            for k, v in re.findall(r"(\w+)=(-?[\d.]+)", line):
                found[k] = float(v) if "." in v else int(v)
    return found


def dollars(cents: int) -> str:
    return f"{cents / 100:,.2f}"


def run(log: str, name: str) -> dict:
    """One fleet run: what was asked for, and what the money actually did."""
    body = text(log)
    driver = kv("DRIVER", body)
    result = kv(f"RESULT {name}", body)
    owed = driver.get("owed_cents", 0)
    collected = result.get("collected_cents", 0)
    r = {
        "mode": name,
        "checkouts": driver.get("checkouts", 0),
        "requests_sent": driver.get("requests", 0),
        "timeouts": driver.get("timeouts", 0),
        "conflict_polls": driver.get("conflict_polls", 0),
        "replays": driver.get("replays", 0),
        "server_errors_seen_by_client": driver.get("server_errors", 0),
        "checkouts_reported_failed": driver.get("failed", 0),
        "app_charges": result.get("app_charges", 0),
        "processor_charges": result.get("processor_charges", 0),
        "owed_cents": owed,
        "collected_cents": collected,
        "overcharge_cents": collected - owed,
        "double_charged_customers": result.get("double_charged_customers", 0),
        "duplicate_charges": result.get("duplicate_charges", 0),
        "duplicated_keys": result.get("duplicated_keys", 0),
    }
    r["duplicate_rate_pct"] = round(100 * r["double_charged_customers"] / r["checkouts"], 1) if r["checkouts"] else 0.0
    r["overcharge_pct"] = round(100 * r["overcharge_cents"] / owed, 1) if owed else 0.0
    r["owed_dollars"] = dollars(r["owed_cents"])
    r["collected_dollars"] = dollars(r["collected_cents"])
    r["overcharge_dollars"] = dollars(r["overcharge_cents"])
    return r


naive = run("02-naive-fleet.log", "naive")
late = run("05-late-fleet.log", "late")
claim = run("07-claim-fleet.log", "claim")

# What the constraint actually bought in `late` mode: a log line, arriving after
# the money had moved and after the client had already given up listening.
late["duplicate_key_refusals"] = kv("REFUSALS late", text("05-late-fleet.log")).get("count", 0)

# ── How wide the window really is ──────────────────────────────────────────
# Measured in the handler: from the SELECT that said "never seen this key" to
# the INSERT that finally recorded it. The interesting part is that it is not a
# microsecond. It is however long the payment call takes, and a retry is
# scheduled to arrive right in the middle of it.
w = kv("WINDOW naive", text("04-naive-keys.log"))
window = {
    "requests": w.get("requests", 0),
    "min_ms": w.get("min_ms", 0.0),
    "median_ms": w.get("median_ms", 0.0),
    "max_ms": w.get("max_ms", 0.0),
}


def race(log: str, label: str) -> dict:
    """One A/B probe: two requests, one key, a measured distance apart."""
    r = kv(f"RACE {label}", text(log))
    return {
        "gap_ms": r.get("gap_ms", 0),
        "customer_id": r.get("customer", 0),
        "a_status": r.get("a_status", 0),
        "b_status": r.get("b_status", 0),
        "b_replayed": bool(r.get("b_replayed", 0)),
        "bodies_identical": bool(r.get("bodies_identical", 0)),
    }


replay = race("10-replay.log", "replay")
fingerprint = race("11-fingerprint.log", "fingerprint")
in_flight = race("12-in-flight.log", "in_flight")

expiry = race("13-expiry.log", "expired")
exp = kv("EXPIRY", text("13-expiry.log"))
expiry["ttl_seconds"] = exp.get("ttl_seconds", 0)
expiry["processor_charges"] = exp.get("processor_charges", 0)
expiry["collected_dollars"] = dollars(expiry["processor_charges"] * 4000)

# ── The scenario's own settings, read back from the run rather than assumed ──
up = text("01-compose-up.log")
lat = re.findall(r"^(\d+) (\d+)\s*$", up, re.M)
base, spread = (int(lat[-1][0]), int(lat[-1][1])) if lat else (0, 0)
driver_any = kv("DRIVER", text("02-naive-fleet.log"))


def from_up(pattern: str, fallback: float = 0.0) -> float:
    m = re.search(pattern, up)
    return float(m.group(1)) if m else fallback


def unique_index_present() -> int:
    """Whether the UNIQUE index existed during the naive run. It must not have.

    Read back from pg_indexes in the run itself, because `naive` is defined by
    the absence of that index and a capture that got this wrong would be
    measuring the wrong thing while looking exactly the same.
    """
    m = re.search(r"-- is there a unique index on idempotency_keys\?\s*\n\s*(\d+)", up)
    return int(m.group(1)) if m else -1


metrics = {
    "scenario": {
        "client_timeout_ms": driver_any.get("client_timeout_ms", 0),
        "app_processor_timeout_s": from_up(r"app processor timeout = ([\d.]+)"),
        "amount_cents": driver_any.get("amount_cents", 0),
        "amount_dollars": dollars(driver_any.get("amount_cents", 0)),
        "max_attempts": 2,
        "concurrent": driver_any.get("concurrent", 0),
        "idempotency_ttl_seconds": from_up(r"idempotency ttl = ([\d.]+)"),
        "unique_index_during_naive_run": unique_index_present(),
        "processor_latency_base_ms": base,
        "processor_latency_spread_ms": spread,
        "processor_latency_min_ms": base,
        "processor_latency_max_ms": base + spread - 1 if spread else 0,
    },
    "naive": naive,
    "late": late,
    "claim": claim,
    "window": window,
    "replay": replay,
    "fingerprint": fingerprint,
    "in_flight": in_flight,
    "expiry": expiry,
}

(OUT / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
print(json.dumps(metrics, indent=2))
