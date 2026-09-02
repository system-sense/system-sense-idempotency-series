#!/usr/bin/env bash
#
# Runs the Episode 2 demo end to end and records what actually happened.
#
# Everything the episode claims on screen comes out of this script. If a number
# changes when you run it on your machine, the number was real.
#
#   ./scripts/capture-demo.sh
#
# The same load is fired four times, at the same twenty-five customers, with the
# same keys and the same retry policy. The only thing that changes between runs
# is IDEMPOTENCY_MODE:
#
#   naive   check, then work, then record. No constraint.   -> duplicates
#   late    the same handler, with the constraint.          -> duplicates, and a 500
#   claim   claim the key first, ON CONFLICT DO NOTHING.    -> one charge each
#
# Writes:  capture/*.log  and  capture/metrics.json
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=capture
mkdir -p "$OUT"

FLEET="$(seq 1 25)"

log()   { printf '\n\033[1m== %s\033[0m\n' "$*"; }
dc()    { docker compose "$@"; }
psql()  { dc exec -T postgres psql -U sysense -d sysense -At -F' ' "$@"; }   # machine-readable
psqlt() { dc exec -T postgres psql -U sysense -d sysense "$@"; }              # with headers, for the screen

# Episode 1's stack listens on the same ports, and answers /health just as
# cheerfully. Its reply has no "mode" in it; this episode's does. Insisting on
# that field is the difference between measuring this episode and measuring the
# last one while every log looks correct. (Hit for real while building this.)
wait_healthy() {
  printf 'waiting for the stack '
  for _ in $(seq 1 90); do
    if curl -fsS localhost:8000/health 2>/dev/null | grep -q '"mode"' &&
       curl -fsS localhost:9000/health >/dev/null 2>&1; then echo ' ready'; return 0; fi
    printf '.'; sleep 1
  done
  echo ' TIMED OUT (is another episode holding port 8000? try: docker compose ls)'
  dc logs app processor | tail -40; return 1
}

# Restart just the application in a given mode. The database, the processor and
# the load are untouched between runs — the ONLY variable in this whole capture
# is which of the four handlers is serving, and whether the UNIQUE index exists.
restart_app() {
  local mode=$1 ttl=${2:-86400}
  IDEMPOTENCY_MODE="$mode" IDEMPOTENCY_TTL_SECONDS="$ttl" \
    dc up -d --force-recreate --no-deps app >/dev/null 2>&1
  wait_healthy >/dev/null
  # Read the mode back out of the running container rather than trusting the
  # variable we just set. A capture that mislabels which world it ran in is
  # worse than no capture.
  local got
  got=$(curl -fsS localhost:8000/health | sed 's/.*"mode":"\([a-z]*\)".*/\1/')
  [ "$got" = "$mode" ] || { echo "app reports mode=$got, expected $mode"; return 1; }
  echo "-- app restarted in mode=$mode ttl=${ttl}s"
}

# Both ledgers and the key table back to zero, so each run is measured on its own.
reset_all() {
  psql -c 'TRUNCATE charges; TRUNCATE processor.ledger; TRUNCATE idempotency_keys;' >/dev/null
}

# In-flight charges outlive the client that abandoned them — that is the whole
# point — so wait for both books to go quiet rather than guessing at a sleep.
#
# QUIET is 5 seconds because the slowest charge takes 3.6 s and lands in two
# steps: the processor writes its row, then answers the application, which
# writes its own. A shorter window can read the gap between those two writes and
# report books that disagree by one row for reasons that have nothing to do with
# the bug. Measured on Episode 1: a 2-second window did exactly that.
settle() {
  local QUIET=5 last="" now stable=0
  for _ in $(seq 1 60); do
    now="$(psql -c 'SELECT count(*) FROM charges;')/$(psql -c 'SELECT count(*) FROM processor.ledger;')"
    if [ "$now" = "$last" ]; then
      stable=$((stable + 1))
      [ "$stable" -ge "$QUIET" ] && return 0
    else
      stable=0
    fi
    last=$now
    sleep 1
  done
}

# Reads both books and prints one machine-parsable line.
tally() {
  local name=$1
  settle
  local app_charges app_cents proc_charges proc_cents dupes dupe_charges dupe_keys
  app_charges=$(psql -c 'SELECT count(*) FROM charges;')
  app_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM charges;')
  proc_charges=$(psql -c 'SELECT count(*) FROM processor.ledger;')
  proc_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM processor.ledger;')
  dupes=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1) d;')
  dupe_charges=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM processor.ledger GROUP BY customer_id) d;')
  # How many keys the table holds more than once. Under `naive` this is the
  # constraint that was not there, counted.
  dupe_keys=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM idempotency_keys GROUP BY scope, idempotency_key HAVING count(*) > 1) d;')
  echo "RESULT $name app_charges=$app_charges app_cents=$app_cents" \
       "processor_charges=$proc_charges collected_cents=$proc_cents" \
       "double_charged_customers=$dupes duplicate_charges=$dupe_charges" \
       "duplicated_keys=$dupe_keys"
}

# The server's own account of the run, kept per mode. --force-recreate means
# each container's log covers exactly one mode and nothing else.
app_log() { dc logs --no-log-prefix app > "$OUT/$1"; }

log "1/9  Tearing down any previous run"
dc down -v --remove-orphans >/dev/null 2>&1 || true

log "2/9  Starting the stack with the naive key check"
IDEMPOTENCY_MODE=naive dc up --build -d 2>&1 | tee "$OUT/01-compose-up.log"
wait_healthy
{
  echo "processor latency = LATENCY_BASE_MS + (customer_id * 137) % LATENCY_SPREAD_MS"
  dc exec -T processor printenv LATENCY_BASE_MS LATENCY_SPREAD_MS | tr '\n' ' '
  echo
  printf 'app processor timeout = '
  dc exec -T app printenv PROCESSOR_TIMEOUT_SECONDS | tr -d '\r'
  echo
  printf 'idempotency ttl = '
  dc exec -T app printenv IDEMPOTENCY_TTL_SECONDS | tr -d '\r'
  echo
  echo "-- is there a unique index on idempotency_keys?"
  psql -c "SELECT count(*) FROM pg_indexes WHERE indexname = 'idempotency_keys_scope_key_uniq';"
} >> "$OUT/01-compose-up.log"

# ── The naive fix, under concurrency ────────────────────────────────────────
log "3/9  Twenty-five customers, all at once, each sending an idempotency key"
reset_all
{ python3 scripts/checkout.py --concurrent $FLEET; tally naive; } 2>&1 | tee "$OUT/02-naive-fleet.log"
app_log 03-naive-app.log
{
  echo "-- who paid twice, WITH the idempotency key in place"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id LIMIT 10;"
  echo
  echo "-- keys the table accepted more than once"
  psqlt -c "SELECT idempotency_key, count(*) AS rows, min(scope) AS scope FROM idempotency_keys GROUP BY idempotency_key HAVING count(*) > 1 ORDER BY idempotency_key LIMIT 8;"
  echo
  echo "-- the window between one request's check and its insert, in milliseconds"
  grep -o 'window_ms=[0-9.]*' "$OUT/03-naive-app.log" | sed 's/window_ms=//' | sort -n |
    awk '{v[NR]=$1}
         END {if (NR == 0) exit;
              printf "  requests=%d  min=%.1f ms  median=%.1f ms  max=%.1f ms\n", NR, v[1], v[int((NR+1)/2)], v[NR];
              printf "WINDOW naive requests=%d min_ms=%.1f median_ms=%.1f max_ms=%.1f\n", NR, v[1], v[int((NR+1)/2)], v[NR]}'
} 2>&1 | tee "$OUT/04-naive-keys.log"

# ── The same handler, with the constraint ───────────────────────────────────
log "4/9  The identical handler, now with the UNIQUE constraint in place"
reset_all
restart_app late | tee "$OUT/05-late-fleet.log"
{ python3 scripts/checkout.py --concurrent $FLEET; tally late; } 2>&1 | tee -a "$OUT/05-late-fleet.log"
app_log 06-late-app.log
{
  echo "-- who paid twice, WITH the constraint the database enforced"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id LIMIT 10;"
  echo
  echo "-- duplicate rows in idempotency_keys (the constraint held: there are none)"
  psql -c "SELECT count(*) FROM (SELECT 1 FROM idempotency_keys GROUP BY scope, idempotency_key HAVING count(*) > 1) d;"
  echo
  echo "-- the database refused the second insert this many times"
  echo "REFUSALS late count=$(grep -c 'DUPLICATE KEY REFUSED' "$OUT/06-late-app.log" || true)"
} 2>&1 | tee -a "$OUT/05-late-fleet.log"

# ── Claim the key first ─────────────────────────────────────────────────────
log "5/9  The same load again, claiming the key before doing the work"
reset_all
restart_app claim | tee "$OUT/07-claim-fleet.log"
{ python3 scripts/checkout.py --concurrent $FLEET; tally claim; } 2>&1 | tee -a "$OUT/07-claim-fleet.log"
app_log 08-claim-app.log

log "6/9  The two sets of books, after the fix"
{
  echo "-- our application's books"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM charges;"
  echo
  echo "-- the payment processor's books"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM processor.ledger;"
  echo
  echo "-- who paid twice"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id;"
} 2>&1 | tee "$OUT/09-ledgers.log"

# ── Response replay: the same answer, not merely no second charge ───────────
log "7/9  The retry gets the same answer, byte for byte"
reset_all
{ python3 scripts/race.py --customer 18 --gap-ms 2000 --label replay; } 2>&1 | tee "$OUT/10-replay.log"

log "     The same key with a different body is a client bug, not a second charge"
reset_all
{ python3 scripts/race.py --customer 18 --gap-ms 2000 --different-body --label fingerprint; } 2>&1 | tee "$OUT/11-fingerprint.log"

# ── In flight: a row, and no response to replay yet ─────────────────────────
log "8/9  Two requests three milliseconds apart, while the first is still running"
reset_all
{ python3 scripts/race.py --customer 17 --gap-ms 3 --label in_flight; } 2>&1 | tee "$OUT/12-in-flight.log"

# ── Expiry: correct behaviour, and it charges again ─────────────────────────
log "9/9  A retry that arrives one second after the key expired"
reset_all
restart_app claim 2 | tee "$OUT/13-expiry.log"
{ python3 scripts/race.py --customer 18 --gap-ms 3000 --label expired; } 2>&1 | tee -a "$OUT/13-expiry.log"
{
  settle
  echo
  echo "-- what the processor's books say about that customer"
  psqlt -c "SELECT id, customer_id, amount_cents, to_char(captured_at, 'HH24:MI:SS.MS') AS captured_at FROM processor.ledger WHERE customer_id = 18 ORDER BY captured_at;"
  echo "EXPIRY ttl_seconds=2 processor_charges=$(psql -c 'SELECT count(*) FROM processor.ledger;')"
} 2>&1 | tee -a "$OUT/13-expiry.log"

# Leave the stack in the fixed mode, so that a viewer who runs this script and
# then pokes at the app by hand is poking at the version that works.
restart_app claim >/dev/null

log "Summarising"
python3 scripts/summarise.py

echo
echo "Done. Real numbers are in $OUT/metrics.json"
