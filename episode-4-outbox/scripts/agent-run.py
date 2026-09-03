#!/usr/bin/env python3
"""The same problem, in the place it is currently being rediscovered.

    python3 scripts/agent-run.py --runs 4 --replays 3 --key payload
    python3 scripts/agent-run.py --runs 4 --replays 3 --key position

An agent workflow is a durable execution: a sequence of steps, checkpointed, so
that a crash resumes rather than restarts. Resuming means REPLAYING the steps
already taken, which means every side effect in the workflow is about to be
attempted again — which is this whole series, in a costume.

There is one genuinely new wrinkle, and it is the reason this is not a rerun.

**The model's output is not deterministic.** A step replayed does not produce
the byte sequence it produced the first time, so the thing everybody reaches for
first — hash the payload, skip it if you have seen that hash — cannot work. Not
"works badly". Cannot work. The hash is different every single time, so every
replay is a new intent, and the customer is charged for every one of them.

So you do not key on WHAT the step produced. You key on WHERE the step is:

    (run_id, step_index, action_type)

That triple is fixed by the workflow's structure before the model is called and
it is identical on every replay, which is exactly what Episode 2 needed a key to
be. It is Episode 2's idempotency key, derived from position instead of from
content.

── About the model ────────────────────────────────────────────────────────
There isn't one, and there must not be one. A real model call costs money and
returns something different every time, and this series' rule is that every
number on screen is reproducible by anybody who clones the repository. So
`draft_note()` below is a stub that does the one thing that matters here: it
returns a different string on every call. That is the entire property under
discussion. A real model would add cost, latency and an API key, and would not
add a single thing to the argument.

Standard library only, on purpose (see scripts/resp.py).
"""
import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.request

AMOUNT_CENTS = 4000

_calls = 0


def draft_note(customer_id: int) -> str:
    """The stub model. Same prompt, different answer, every time.

    Deterministic across a whole capture (it counts calls) and different on
    every call, which is the combination the argument needs and a real model
    cannot give you.
    """
    global _calls
    _calls += 1
    return (f"Thanks for your order, customer {customer_id}. "
            f"Reference {hashlib.sha1(str(_calls).encode()).hexdigest()[:10]}.")


def payload_key(run_id: str, step: int, action: str, body: dict, note: str) -> str:
    """Dedupe on what the step produced. The obvious one. It cannot work here.

    The note is part of the step's payload — it is what the step is FOR — and it
    is different on every replay, so this hash is different on every replay.
    """
    canonical = json.dumps({**body, "note": note}, sort_keys=True, separators=(",", ":"))
    return "k_" + hashlib.sha256(canonical.encode()).hexdigest()[:16]


def position_key(run_id: str, step: int, action: str, body: dict, note: str) -> str:
    """Dedupe on where the step is. Fixed before the model is called."""
    return f"k_{run_id}:{step}:{action}"


KEYS = {"payload": payload_key, "position": position_key}


def post(url: str, body: dict, key: str) -> tuple[int, dict, bool]:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"content-type": "application/json", "Idempotency-Key": key},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}"), \
                r.headers.get("Idempotency-Replayed") == "true"
    except urllib.error.HTTPError as e:
        return e.code, {}, False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=4, help="how many agent runs")
    ap.add_argument("--replays", type=int, default=3,
                    help="how many times each run is replayed from the top")
    ap.add_argument("--key", choices=sorted(KEYS), default="position")
    ap.add_argument("--first-customer", type=int, default=1)
    ap.add_argument("--amount-cents", type=int, default=AMOUNT_CENTS)
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    mint = KEYS[args.key]
    label = args.label or args.key
    charged = replayed = conflicts = attempts = 0
    notes: set[str] = set()

    print(f"-- {args.runs} agent runs, each replayed {args.replays} times, "
          f"keyed on {args.key}")

    for i in range(args.runs):
        run_id = f"run_{i + 1:03d}"
        customer_id = args.first_customer + i
        body = {"customer_id": customer_id, "amount_cents": args.amount_cents}

        for replay in range(1, args.replays + 1):
            # ── The workflow, from the top, as a resume would ──────────────
            # step 0  fetch_customer   no side effect
            # step 1  draft_note       the model. Different answer every time.
            # step 2  charge_customer  the side effect. This is the one.
            note = draft_note(customer_id)
            notes.add(note)

            key = mint(run_id, 2, "charge_customer", body, note)
            status, out, was_replay = post(f"{args.base}/api/checkout", body, key)
            attempts += 1

            if status == 409:
                conflicts += 1
                verdict = "409 in flight"
            elif was_replay:
                replayed += 1
                verdict = "replayed  (the same response, no second charge)"
            else:
                charged += 1
                verdict = f"CHARGED   {out.get('processor_charge_id', '')}"

            print(f"   {run_id} replay {replay}  customer {customer_id:<3} "
                  f"key={key[:34]:<34} {verdict}")

    print(f"AGENT label={label} runs={args.runs} replays={args.replays} "
          f"attempts={attempts} distinct_notes={len(notes)} charged={charged} "
          f"replayed={replayed} conflicts={conflicts} "
          f"owed_cents={args.runs * args.amount_cents} "
          f"amount_cents={args.amount_cents}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
