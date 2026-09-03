#!/usr/bin/env bash
#
# Runs the Episode 4 demo end to end and records what actually happened.
#
# Everything the episode claims on screen comes out of this script. If a number
# changes when you run it on your machine, the number was real.
#
#   ./scripts/capture-demo.sh
#
# The same twelve orders, six times. The consumer is Episode 3's, finished:
# the lease is held, there is a delivery limit, and the producer's key is passed
# on. The endpoint is Episode 2's, which cannot charge twice for one key.
# Nothing below is a bug in either of them.
#
#   commit_first       COMMIT, then publish, killed in between  -> events lost
#   publish_first      publish, then COMMIT, same kill          -> phantom orders
#   outbox             both in one transaction, same kill       -> nothing lost
#   relay_crash        the relay dies before marking sent       -> published twice
#   relay_crash_keyed  the same, with Episode 2's key on        -> charged once
#   agent_payload      an agent run replayed, keyed on content  -> charged 3x
#   agent_position     the same, keyed on (run, step, action)   -> charged once
#
# Writes:  capture/*.log  and  capture/metrics.json
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=capture
mkdir -p "$OUT"

# Twelve orders, $40 each: $480 owed. CRASH_EVERY=4 kills the producer on
# seq 4, 8 and 12 — the same three orders in every mode, so the comparison
# between the modes is a measurement and not a coincidence.
FLEET="$(seq 1 12)"
CRASH_AT=4
EXPECT_KILLS=3
RELAY_CRASHES=3

log()   { printf '\n\033[1m== %s\033[0m\n' "$*"; }
dc()    { docker compose "$@"; }
psql()  { dc exec -T postgres psql -U sysense -d sysense -At -F' ' "$@"; }
psqlt() { dc exec -T postgres psql -U sysense -d sysense "$@"; }
rcli()  { dc exec -T redis redis-cli "$@"; }

wait_healthy() {
  printf 'waiting for the stack '
  for _ in $(seq 1 90); do
    if curl -fsS localhost:8000/health 2>/dev/null | grep -q '"mode"' &&
       curl -fsS localhost:8100/health 2>/dev/null | grep -q '"mode"' &&
       curl -fsS localhost:9000/health >/dev/null 2>&1 &&
       rcli ping 2>/dev/null | grep -q PONG; then echo ' ready'; return 0; fi
    printf '.'; sleep 1
  done
  echo ' TIMED OUT (is another episode holding port 8000? try: docker compose ls)'
  dc logs app orders processor redis | tail -40; return 1
}

# ── Between scenarios ──────────────────────────────────────────────────────
# Everything that writes is stopped BEFORE anything is truncated. The producer
# and the relay both restart themselves on exit — that is the point of them —
# so a scenario that tidied up while they were running would be tidying up
# underneath a process that was still working.
reset_all() {
  dc stop worker-1 worker-2 orders relay >/dev/null 2>&1 || true
  psql -c 'TRUNCATE outbox, orders RESTART IDENTITY CASCADE;' >/dev/null
  psql -c 'TRUNCATE charges; TRUNCATE processor.ledger; TRUNCATE idempotency_keys; TRUNCATE job_runs;' >/dev/null
  rcli DEL checkouts checkouts:dead >/dev/null
}

start_producer() {
  dc up -d --force-recreate --no-deps orders >/dev/null 2>&1
  for _ in $(seq 1 60); do
    curl -fsS localhost:8100/health 2>/dev/null | grep -q '"mode"' && {
      echo "-- orders up  (publish_mode=${PUBLISH_MODE:-outbox} crash_every=${CRASH_EVERY:-0})"
      return 0; }
    sleep 0.5
  done
  echo "producer did not come up"; dc logs --tail 30 orders; return 1
}

start_relay() {
  dc up -d --force-recreate --no-deps relay >/dev/null 2>&1
  echo "-- relay up  (crash_after_publish=${CRASH_AFTER_PUBLISH:-0} poll=${POLL_MS:-200}ms)"
}

# Start the named workers with whatever is currently exported, then wait until
# they have actually joined the consumer group. A worker registers as a consumer
# on its first XREADGROUP, so this is the queue's own answer to "are they up",
# not a sleep.
start_workers() {
  local names="$*" n ok
  dc up -d --force-recreate --no-deps $names >/dev/null 2>&1
  for _ in $(seq 1 90); do
    ok=1
    for n in $names; do
      rcli XINFO CONSUMERS checkouts payments 2>/dev/null | grep -qx "$n" || ok=0
    done
    if [ "$ok" = 1 ]; then
      echo "-- $names up  (ack=${ACK_MODE:-after} visibility=${VISIBILITY_TIMEOUT_MS:-15000}ms" \
           "heartbeat=${HEARTBEAT:-1} max_deliveries=${MAX_DELIVERIES:-5}" \
           "key=${IDEMPOTENT_CONSUMER:-1})"
      return 0
    fi
    sleep 0.5
  done
  echo "workers did not join the group"; dc logs --tail 30 $names; return 1
}

# Every row the relay is going to publish, published. Bounded: in the dual-write
# scenarios the outbox is empty and this returns immediately, which is itself
# worth seeing.
settle_outbox() {
  for _ in $(seq 1 90); do
    [ "$(psql -c 'SELECT count(*) FROM outbox WHERE published_at IS NULL;')" = 0 ] && return 0
    sleep 1
  done
}

# Both books quiet. In-flight charges outlive the worker that abandoned them —
# that is the whole point — so wait for the numbers to stop moving rather than
# guessing at a sleep. QUIET is 5 because the slowest charge takes 3.6 s and
# lands in two writes: the processor's row, then the application's.
settle_books() {
  local QUIET=5 last="" now stable=0
  for _ in $(seq 1 60); do
    now="$(psql -c 'SELECT count(*) FROM charges;')/$(psql -c 'SELECT count(*) FROM processor.ledger;')"
    if [ "$now" = "$last" ]; then
      stable=$((stable + 1)); [ "$stable" -ge "$QUIET" ] && return 0
    else stable=0; fi
    last=$now; sleep 1
  done
}

settle_queue() {
  local budget=${1:-90} line lag pend
  settle_outbox
  for _ in $(seq 1 "$budget"); do
    line=$(python3 scripts/queue-state.py --label settle 2>/dev/null | grep '^QUEUE' || true)
    lag=$(echo "$line" | sed -n 's/.*lag=\([0-9]*\).*/\1/p')
    pend=$(echo "$line" | sed -n 's/.*pending=\([0-9]*\).*/\1/p')
    [ "${lag:-1}" = 0 ] && [ "${pend:-1}" = 0 ] && break
    sleep 1
  done
  settle_books
}

# ── The consumer's side of each scenario ───────────────────────────────────
# Reconciliation prints the orders against the stream; this prints what the
# queue and the money did once the events that survived got there.
tally() {
  local name=$1 placed=$2
  local deliveries msgs replays failed charges dupes dupe_charges
  deliveries=$(psql -c 'SELECT count(*) FROM job_runs;')
  msgs=$(psql -c 'SELECT count(DISTINCT message_id) FROM job_runs;')
  replays=$(psql -c "SELECT count(*) FROM job_runs WHERE outcome = 'replayed';")
  failed=$(psql -c "SELECT count(*) FROM job_runs WHERE outcome = 'failed';")
  charges=$(psql -c 'SELECT count(*) FROM charges;')
  dupes=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1) d;')
  dupe_charges=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM processor.ledger GROUP BY customer_id) d;')
  echo "RESULT $name orders_placed=$placed deliveries=$deliveries messages_delivered=$msgs" \
       "replays=$replays failed_runs=$failed app_charges=$charges" \
       "double_charged_customers=$dupes duplicate_charges=$dupe_charges"
}

books() {
  echo "-- what the business committed"
  psqlt -c "SELECT count(*) AS orders, coalesce(sum(amount_cents),0) AS cents, count(DISTINCT customer_id) AS customers FROM orders;"
  echo "-- what the queue was told"
  psqlt -c "SELECT count(*) AS outbox_rows, count(*) FILTER (WHERE published_at IS NULL) AS unsent, count(*) FILTER (WHERE publish_attempts > 1) AS published_twice FROM outbox;"
  echo "-- what actually happened to money"
  psqlt -c "SELECT count(*) AS charges, coalesce(sum(amount_cents),0) AS cents, count(DISTINCT customer_id) AS customers FROM processor.ledger;"
}

# ── 1. Up ──────────────────────────────────────────────────────────────────
log "1/8  Starting the stack: postgres, redis, the processor, Episode 2's app, Episode 3's workers, and two new services"
dc down -v --remove-orphans >/dev/null 2>&1 || true
dc up --build -d 2>&1 | tee "$OUT/01-compose-up.log"
wait_healthy
{
  echo "processor latency = LATENCY_BASE_MS + (customer_id * 137) % LATENCY_SPREAD_MS"
  dc exec -T processor printenv LATENCY_BASE_MS LATENCY_SPREAD_MS | tr '\n' ' '; echo
  printf 'app idempotency mode = '; curl -fsS localhost:8000/health; echo
  printf 'producer = '; curl -fsS localhost:8100/health; echo
  printf 'worker defaults = '
  dc exec -T worker-1 printenv ACK_MODE VISIBILITY_TIMEOUT_MS HEARTBEAT MAX_DELIVERIES IDEMPOTENT_CONSUMER WORKER_BATCH | tr '\n' ' '; echo
  echo "SETUP orders_in_fleet=$(echo $FLEET | wc -w | tr -d ' ') crash_every=$CRASH_AT expected_kills=$EXPECT_KILLS relay_crashes=$RELAY_CRASHES"
} >> "$OUT/01-compose-up.log"
tail -8 "$OUT/01-compose-up.log"

# ── A dual write, killed in the window ─────────────────────────────────────
# Both modes, both kills, same twelve orders, same three seq numbers.
dual_run() {
  local mode=$1 label=$2
  reset_all
  export PUBLISH_MODE=$mode CRASH_EVERY=$CRASH_AT CRASH_AFTER_PUBLISH=0
  export ACK_MODE=after VISIBILITY_TIMEOUT_MS=15000 HEARTBEAT=1 MAX_DELIVERIES=5 \
         IDEMPOTENT_CONSUMER=1 WORKER_BATCH=1
  start_workers worker-1 worker-2
  start_relay
  start_producer
  echo "-- twelve orders, one at a time. The producer dies on every ${CRASH_AT}th."
  python3 scripts/place-orders.py --customers $FLEET --label "$label"
  settle_queue 120
  echo
  python3 scripts/reconcile.py --label "$label"
  echo
  echo "-- the producer's own log, at the moment it stopped existing"
  dc logs --no-log-prefix orders 2>&1 | grep -E 'KILLED|mode=' | head -8
  tally "$label" 12
}

log "2/8  COMMIT, then publish. Killed in between."
dual_run commit_first commit_first 2>&1 | tee "$OUT/02-commit-first.log"
dc logs --no-log-prefix orders relay > "$OUT/03-commit-first-services.log" 2>&1 || true

log "3/8  Publish, then COMMIT. The same kill, and it is worse."
dual_run publish_first publish_first 2>&1 | tee "$OUT/04-publish-first.log"
dc logs --no-log-prefix orders relay > "$OUT/05-publish-first-services.log" 2>&1 || true

# ── The outbox ─────────────────────────────────────────────────────────────
log "4/8  Both facts in one transaction. The same kill, in the same place."
{
  reset_all
  export PUBLISH_MODE=outbox CRASH_EVERY=$CRASH_AT CRASH_AFTER_PUBLISH=0
  export ACK_MODE=after VISIBILITY_TIMEOUT_MS=15000 HEARTBEAT=1 MAX_DELIVERIES=5 \
         IDEMPOTENT_CONSUMER=1 WORKER_BATCH=1
  start_workers worker-1 worker-2
  start_relay
  start_producer
  echo "-- the same twelve orders, and the producer still dies on every ${CRASH_AT}th."
  python3 scripts/place-orders.py --customers $FLEET --label outbox
  settle_queue 120
  echo
  python3 scripts/reconcile.py --label outbox
  echo
  echo "-- the outbox, row by row"
  psqlt -c "SELECT o.id, o.order_id, o.publish_attempts, o.message_id IS NOT NULL AS sent, to_char(o.published_at - o.created_at, 'SS.MS') AS relay_lag_s FROM outbox o ORDER BY o.id;"
  echo "OUTBOX_LAG_MS median=$(psql -c "SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at) * 1000)::numeric, 1) FROM outbox WHERE published_at IS NOT NULL;")" \
       "max=$(psql -c "SELECT round(max(extract(epoch FROM published_at - created_at) * 1000)::numeric, 1) FROM outbox WHERE published_at IS NOT NULL;")"
  tally outbox 12
} 2>&1 | tee "$OUT/06-outbox.log"
dc logs --no-log-prefix orders relay > "$OUT/07-outbox-services.log" 2>&1 || true

# ── The relay's own dual write ─────────────────────────────────────────────
# It publishes, then marks the row sent. Two systems again. Kill it in between
# and the next relay publishes the row a second time.
relay_run() {
  local keyed=$1 label=$2
  reset_all
  export PUBLISH_MODE=outbox CRASH_EVERY=0 CRASH_AFTER_PUBLISH=$RELAY_CRASHES
  export ACK_MODE=after VISIBILITY_TIMEOUT_MS=15000 HEARTBEAT=1 MAX_DELIVERIES=5 \
         IDEMPOTENT_CONSUMER=$keyed WORKER_BATCH=1
  start_workers worker-1 worker-2
  start_producer
  echo "-- twelve orders, no kills in the producer at all."
  python3 scripts/place-orders.py --customers $FLEET --label "$label"
  echo "-- now start the relay, which dies $RELAY_CRASHES times after publishing and"
  echo "   before marking the row sent."
  start_relay
  settle_queue 150
  echo
  python3 scripts/reconcile.py --label "$label"
  echo
  echo "-- the rows the relay published twice"
  psqlt -c "SELECT id, order_id, publish_attempts, message_id FROM outbox WHERE publish_attempts > 1 ORDER BY id;"
  echo "-- what the consumer did with the second copy"
  psqlt -c "SELECT seq, customer_id, count(*) AS deliveries, string_agg(outcome, ' then ' ORDER BY started_at) AS outcomes FROM job_runs GROUP BY seq, customer_id HAVING count(*) > 1 ORDER BY seq;"
  tally "$label" 12
}

log "5/8  The relay dies after publishing and before marking the row sent. Episode 2's key OFF."
relay_run 0 relay_crash 2>&1 | tee "$OUT/08-relay-crash.log"
dc logs --no-log-prefix relay > "$OUT/09-relay-crash-services.log" 2>&1 || true

log "6/8  The identical run, with Episode 2's key back on."
relay_run 1 relay_crash_keyed 2>&1 | tee "$OUT/10-relay-crash-keyed.log"
dc logs --no-log-prefix relay > "$OUT/11-relay-crash-keyed-services.log" 2>&1 || true

# ── The agent segment ──────────────────────────────────────────────────────
# No queue, no producer, no relay. Just Episode 2's endpoint and a workflow
# replayed from the top, keyed two ways. The model is a stub that returns a
# different string every call, which is the only property of a model that
# matters here and the only one that can be measured for free.
agent_run() {
  local keying=$1 label=$2
  reset_all
  export IDEMPOTENT_CONSUMER=1
  python3 scripts/agent-run.py --runs 4 --replays 3 --key "$keying" --label "$label"
  settle_books
  echo
  echo "-- what actually happened to money"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id ORDER BY customer_id;"
  echo "AGENTMONEY $label charges=$(psql -c 'SELECT count(*) FROM processor.ledger;')" \
       "collected_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM processor.ledger;')" \
       "double_charged_customers=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1) d;')" \
       "duplicate_charges=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM processor.ledger GROUP BY customer_id) d;')"
}

log "7/8  Four agent runs, each replayed three times, keyed on what the step produced"
agent_run payload agent_payload 2>&1 | tee "$OUT/12-agent-payload.log"

log "8/8  The same twelve attempts, keyed on where the step is"
agent_run position agent_position 2>&1 | tee "$OUT/13-agent-position.log"

log "The books, after the outbox and the key"
{
  reset_all
  export PUBLISH_MODE=outbox CRASH_EVERY=$CRASH_AT CRASH_AFTER_PUBLISH=$RELAY_CRASHES
  export ACK_MODE=after VISIBILITY_TIMEOUT_MS=15000 HEARTBEAT=1 MAX_DELIVERIES=5 \
         IDEMPOTENT_CONSUMER=1 WORKER_BATCH=1
  start_workers worker-1 worker-2
  start_relay
  start_producer
  echo "-- everything killed at once: the producer on every ${CRASH_AT}th order, the"
  echo "   relay $RELAY_CRASHES times before it could mark a row sent."
  python3 scripts/place-orders.py --customers $FLEET --label everything
  settle_queue 150
  echo
  python3 scripts/reconcile.py --label everything
  books
  tally everything 12
} 2>&1 | tee "$OUT/14-books.log"

log "Summarising"
python3 scripts/summarise.py

echo
echo "Done. Real numbers are in $OUT/metrics.json"
