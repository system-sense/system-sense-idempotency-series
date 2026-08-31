"""Turns the raw capture logs into capture/metrics.json.

Two numbers carry this episode and both come from here:

    money owed      — what the customers actually asked to pay. The driver
                      knows this, because the driver is the one pressing Pay.
    money collected — what the processor's books say it took. The server knows
                      this, and it does not match.

Nothing in the application can compute the first number. That is not a gap in
the demo; it is the episode.
"""
import json
import pathlib
import re

OUT = pathlib.Path("capture")


def text(name: str) -> str:
    p = OUT / name
    return p.read_text() if p.exists() else ""


def kv(line_prefix: str, body: str) -> dict:
    """Parse the last `PREFIX k=v k=v ...` line in a log."""
    found = {}
    for line in body.splitlines():
        if line.startswith(line_prefix):
            found = {k: int(v) for k, v in re.findall(r"(\w+)=(-?\d+)", line)}
    return found


def scenario(log: str, name: str) -> dict:
    body = text(log)
    driver = kv("DRIVER", body)
    result = kv(f"RESULT {name}", body)
    owed = driver.get("owed_cents", 0)
    collected = result.get("collected_cents", 0)
    return {
        "checkouts": driver.get("checkouts", 0),
        "requests_sent": driver.get("requests", 0),
        "timeouts": driver.get("timeouts", 0),
        "checkouts_reported_failed": driver.get("failed", 0),
        "app_charges": result.get("app_charges", 0),
        "processor_charges": result.get("processor_charges", 0),
        "owed_cents": owed,
        "collected_cents": collected,
        "overcharge_cents": collected - owed,
        "double_charged_customers": result.get("double_charged_customers", 0),
        "duplicate_charges": result.get("duplicate_charges", 0),
    }


def dollars(cents: int) -> str:
    return f"{cents / 100:,.2f}"


def customer_in(log: str) -> int:
    """Which customer the single-checkout scenarios used.

    Read back from the log rather than hardcoded, so that changing the id in
    capture-demo.sh cannot leave metrics.json quietly describing a different
    person than the one the episode shows.
    """
    m = re.search(r"customer (\d+)", text(log))
    return int(m.group(1)) if m else 0


fast = scenario("02-single-fast.log", "fast")
slow = scenario("03-single-slow.log", "slow")
fleet = scenario("04-fleet.log", "fleet")

fast["customer_id"] = customer_in("02-single-fast.log")
slow["customer_id"] = customer_in("03-single-slow.log")

fleet["duplicate_rate_pct"] = (
    round(100 * fleet["double_charged_customers"] / fleet["checkouts"], 1)
    if fleet["checkouts"] else 0.0
)
fleet["overcharge_pct"] = (
    round(100 * fleet["overcharge_cents"] / fleet["owed_cents"], 1)
    if fleet["owed_cents"] else 0.0
)
for s in (fast, slow, fleet):
    s["owed_dollars"] = dollars(s["owed_cents"])
    s["collected_dollars"] = dollars(s["collected_cents"])
    s["overcharge_dollars"] = dollars(s["overcharge_cents"])

# ── The scenario's own settings, read back from the run rather than assumed ──
driver_any = kv("DRIVER", text("04-fleet.log"))
up = text("01-compose-up.log")
lat = re.findall(r"^(\d+) (\d+)\s*$", up, re.M)
base, spread = (int(lat[-1][0]), int(lat[-1][1])) if lat else (0, 0)

# ── Methods the spec calls idempotent ──────────────────────────────────────
methods = text("06-idempotent-methods.log")


def section(header: str) -> list[str]:
    """The JSON response lines under one `-- header` marker."""
    body = methods.split(header, 1)[1] if header in methods else ""
    lines = []
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("--"):
            break
        if line.startswith("{"):
            lines.append(line)
    return lines


get_bodies = section("-- GET")
put_bodies = section("-- PUT")
rows_after = re.search(r"^(\d+)$", methods.strip().splitlines()[-1]) if methods.strip() else None

def latency_for(customer_id: int) -> int:
    """The processor's own formula, applied to the settings this run used.

    Not a guess: `processor/main.py` computes exactly this, and both inputs are
    read back out of the running container in 01-compose-up.log.
    """
    return base + (customer_id * 137) % spread if spread else 0


fast["processor_latency_ms"] = latency_for(fast["customer_id"])
slow["processor_latency_ms"] = latency_for(slow["customer_id"])

metrics = {
    "scenario": {
        "client_timeout_ms": driver_any.get("client_timeout_ms", 0),
        "amount_cents": driver_any.get("amount_cents", 0),
        "amount_dollars": dollars(driver_any.get("amount_cents", 0)),
        "max_attempts": 2,
        "processor_latency_base_ms": base,
        "processor_latency_spread_ms": spread,
        "processor_latency_min_ms": base,
        "processor_latency_max_ms": base + spread - 1 if spread else 0,
    },
    "single_fast": fast,
    "single_slow": slow,
    "fleet": fleet,
    "idempotent_methods": {
        "get_calls": len(get_bodies),
        "get_distinct_responses": len(set(get_bodies)),
        "put_calls": len(put_bodies),
        "put_distinct_responses": len(set(put_bodies)),
        "customer_rows_after": int(rows_after.group(1)) if rows_after else 0,
    },
}

(OUT / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
print(json.dumps(metrics, indent=2))
