#!/usr/bin/env bash
#
# Runs the Episode 1 demo end to end and records what actually happened.
#
# Everything the episode claims on screen comes out of this script. If a number
# changes when you run it on your machine, the number was real.
#
#   ./scripts/capture-demo.sh
#
# Writes:  capture/*.log  and  capture/metrics.json
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=capture
mkdir -p "$OUT"

log()   { printf '\n\033[1m== %s\033[0m\n' "$*"; }
dc()    { docker compose "$@"; }
psql()  { dc exec -T postgres psql -U sysense -d sysense -At -F' ' "$@"; }   # machine-readable
psqlt() { dc exec -T postgres psql -U sysense -d sysense "$@"; }              # with headers, for the screen

wait_healthy() {
  printf 'waiting for the stack '
  for _ in $(seq 1 90); do
    if curl -fsS localhost:8000/health >/dev/null 2>&1 &&
       curl -fsS localhost:9000/health >/dev/null 2>&1; then echo ' ready'; return 0; fi
    printf '.'; sleep 1
  done
  echo ' TIMED OUT'; dc logs app processor | tail -40; return 1
}

# Both ledgers back to zero, so each scenario is measured on its own.
reset_ledgers() {
  psql -c 'TRUNCATE charges; TRUNCATE processor.ledger;' >/dev/null
}

# In-flight charges outlive the client that abandoned them — that is the whole
# point — so wait for both books to go quiet rather than guessing at a sleep.
#
# QUIET is 5 seconds because the slowest charge takes 3.6 s and lands in two
# steps: the processor writes its row, then answers the application, which
# writes its own. A shorter window can read the gap between those two writes and
# report books that disagree by one row for reasons that have nothing to do with
# the bug. Measured: a 2-second window did exactly that.
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
#   RESULT <name> app_charges=.. processor_charges=.. collected_cents=.. ...
tally() {
  local name=$1
  settle
  local app_charges app_cents proc_charges proc_cents dupes dupe_charges
  app_charges=$(psql -c 'SELECT count(*) FROM charges;')
  app_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM charges;')
  proc_charges=$(psql -c 'SELECT count(*) FROM processor.ledger;')
  proc_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM processor.ledger;')
  dupes=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1) d;')
  dupe_charges=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM processor.ledger GROUP BY customer_id) d;')
  echo "RESULT $name app_charges=$app_charges app_cents=$app_cents" \
       "processor_charges=$proc_charges collected_cents=$proc_cents" \
       "double_charged_customers=$dupes duplicate_charges=$dupe_charges"
}

log "1/7  Tearing down any previous run"
dc down -v --remove-orphans >/dev/null 2>&1 || true

log "2/7  Starting the stack"
dc up --build -d 2>&1 | tee "$OUT/01-compose-up.log"
wait_healthy
{
  echo "processor latency = LATENCY_BASE_MS + (customer_id * 137) % LATENCY_SPREAD_MS"
  dc exec -T processor printenv LATENCY_BASE_MS LATENCY_SPREAD_MS | tr '\n' ' '
  echo
} >> "$OUT/01-compose-up.log"

log "3/7  One customer, one press of Pay, and the processor answers in time"
reset_ledgers
{ python3 scripts/checkout.py 18; tally fast; } 2>&1 | tee "$OUT/02-single-fast.log"

log "4/7  The same code, one customer, and the processor is a little too slow"
reset_ledgers
{ python3 scripts/checkout.py 17; tally slow; } 2>&1 | tee "$OUT/03-single-slow.log"
{
  echo
  echo "-- what the processor's books say about customer 17"
  psqlt -c "SELECT id, customer_id, amount_cents, to_char(captured_at, 'HH24:MI:SS.MS') AS captured_at FROM processor.ledger WHERE customer_id = 17 ORDER BY captured_at;"
} 2>&1 | tee -a "$OUT/03-single-slow.log"

log "5/7  Twenty-five customers, the same retry policy, one checkout each"
reset_ledgers
{ python3 scripts/checkout.py $(seq 1 25); tally fleet; } 2>&1 | tee "$OUT/04-fleet.log"

log "6/7  The two sets of books, side by side"
{
  echo "-- our application's books"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM charges;"
  echo
  echo "-- the payment processor's books"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM processor.ledger;"
  echo
  echo "-- who paid twice"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id;"
} 2>&1 | tee "$OUT/05-ledgers.log"

log "7/7  The same retry against methods the spec calls idempotent"
{
  echo "-- GET /api/customers/18, five times"
  for _ in 1 2 3 4 5; do curl -fsS localhost:8000/api/customers/18; echo; done
  echo
  echo "-- PUT /api/customers/18/email, three times, same body"
  for _ in 1 2 3; do
    curl -fsS -X PUT localhost:8000/api/customers/18/email \
      -H 'content-type: application/json' -d '{"email":"eighteen@example.com"}'
    echo
  done
  echo
  echo "-- rows in customers for id 18 afterwards"
  psql -c "SELECT count(*) FROM customers WHERE id = 18;"
} 2>&1 | tee "$OUT/06-idempotent-methods.log"

log "Summarising"
python3 scripts/summarise.py

echo
echo "Done. Real numbers are in $OUT/metrics.json"
