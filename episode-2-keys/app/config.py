"""Configuration. Only one setting here is the point of the episode."""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@localhost:5432/sysense")
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:9000")

# How long we are willing to wait on the processor before giving up on it.
# Generously longer than the client's own timeout, so that when a checkout
# fails it is the CLIENT that gave up first — the shape of the bug we are after.
PROCESSOR_TIMEOUT_SECONDS = float(os.getenv("PROCESSOR_TIMEOUT_SECONDS", "30"))

# ── THE KNOB ───────────────────────────────────────────────────────────────
# How this service handles the Idempotency-Key header. Four settings, and the
# episode is the walk from the first to the last.
#
#   off     Episode 1 exactly. The header is read and ignored. Every POST that
#           arrives is a new charge.
#
#   naive   SELECT the key; if it is absent, do the work, then INSERT it.
#           This is the code everybody writes. There is no UNIQUE constraint,
#           because if you are writing this you did not think you needed one.
#
#   late    The SAME handler as `naive`, with the UNIQUE constraint in place.
#           One line of DDL apart. The database does catch the duplicate — and
#           it catches it after the customer's money has already moved.
#
#   claim   The fix. INSERT the key FIRST, with ON CONFLICT DO NOTHING. Zero
#           rows back means you lost the race, so you are the retry: replay the
#           stored response, or say the first one is still running.
IDEMPOTENCY_MODE = os.getenv("IDEMPOTENCY_MODE", "claim").strip().lower()

# How long a key is good for. Stripe's is 24 hours; this is the same by
# default. The capture drops it to a couple of seconds to show what a retry
# that arrives one second after expiry actually does, which is charge again.
IDEMPOTENCY_TTL_SECONDS = float(os.getenv("IDEMPOTENCY_TTL_SECONDS", "86400"))

MODES = ("off", "naive", "late", "claim")

# `naive` is defined by the absence of the constraint, so the app asserts the
# index into or out of existence at startup rather than trusting whoever ran
# the migrations. Printed on boot, so every capture records which world it ran in.
WANTS_CONSTRAINT = {"off": False, "naive": False, "late": True, "claim": True}
