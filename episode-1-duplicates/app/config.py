"""Configuration. Nothing here is the point of the episode; it is all plumbing."""
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://sysense:sysense@localhost:5432/sysense")
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:9000")

# How long we are willing to wait on the processor before giving up on it.
# Generously longer than the client's own timeout, so that when a checkout
# fails it is the CLIENT that gave up first — the shape of the bug we are after.
PROCESSOR_TIMEOUT_SECONDS = float(os.getenv("PROCESSOR_TIMEOUT_SECONDS", "30"))
