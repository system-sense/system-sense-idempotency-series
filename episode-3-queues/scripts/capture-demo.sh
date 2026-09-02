#!/usr/bin/env bash
#
# Runs the Episode 3 demo end to end and records what actually happened.
#
# Everything the episode claims on screen comes out of this script. If a number
# changes when you run it on your machine, the number was real.
#
#   ./scripts/capture-demo.sh
#
# Eight scenarios. The application is never touched: `app/` is Episode 2's,
# pinned to the handler that cannot charge twice for one key. Every variable is
# on the consumer, and every one of them is a line in a config file.
#
#   lease       ack after the work, no heartbeat        -> jobs run twice
#   heartbeat   the same, holding the lease             -> they stop
#   kill-after  a worker killed mid-job, ack after      -> one job runs twice
#   kill-before the same kill, ack before               -> four jobs vanish
#   poison      one message that can never succeed      -> forever, silently
#   dlq         the same, with a delivery limit         -> depth becomes a signal
#   ordering    one message fails once                  -> it finishes last
#   keyed       the lease scenario, with Episode 2's key on
#
# Writes:  capture/*.log  and  capture/metrics.json
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=capture
mkdir -p "$OUT"

FLEET="$(seq 1 25)"
# The five slowest customers, by 1200 + (id * 137) % 2400 ms: 3529, 3392, 3255,
# 3118, 2981. Used wherever a worker has to be killed part-way through a payment
# — with jobs this long, "1.5 seconds in" is inside the work by a wide margin
# rather than by a lucky one.
SLOW="17 16 15 14 13"
# Fast customers for the ordering run, so the retry lands after the rest without
# the run taking a minute: 1266, 1403, 1540, 1677, 1814, 1951, 1337, 1474 ms...
FAST="18 19 20 21 22 23 1 2 3 4"

log()   { printf '\n\033[1m== %s\033[0m\n' "$*"; }
dc()    { docker compose "$@"; }
psql()  { dc exec -T postgres psql -U sysense -d sysense -At -F' ' "$@"; }
psqlt() { dc exec -T postgres psql -U sysense -d sysense "$@"; }
rcli()  { dc exec -T redis redis-cli "$@"; }

wait_healthy() {
  printf 'waiting for the stack '
  for _ in $(seq 1 90); do
    if curl -fsS localhost:8000/health 2>/dev/null | grep -q '"mode"' &&
       curl -fsS localhost:9000/health >/dev/null 2>&1 &&
       rcli ping 2>/dev/null | grep -q PONG; then echo ' ready'; return 0; fi
    printf '.'; sleep 1
  done
  echo ' TIMED OUT (is another episode holding port 8000? try: docker compose ls)'
  dc logs app processor redis | tail -40; return 1
}

# ── Between scenarios ──────────────────────────────────────────────────────
# The workers are stopped BEFORE the stream is deleted. Deleting a stream
# deletes its consumer groups, and a worker that is still running will notice
# and helpfully recreate the group — with the consumers from the last scenario
# still registered in it. Measured the hard way: a scenario reported three
# consumers when two containers were running.
reset_all() {
  dc stop worker-1 worker-2 >/dev/null 2>&1 || true
  psql -c 'TRUNCATE charges; TRUNCATE processor.ledger; TRUNCATE idempotency_keys; TRUNCATE job_runs;' >/dev/null
  rcli DEL checkouts checkouts:dead >/dev/null
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
      echo "-- $names up  (ack=${ACK_MODE:-after} visibility=${VISIBILITY_TIMEOUT_MS:-2000}ms" \
           "heartbeat=${HEARTBEAT:-0} max_deliveries=${MAX_DELIVERIES:-0}" \
           "key=${IDEMPOTENT_CONSUMER:-0} batch=${WORKER_BATCH:-1})"
      return 0
    fi
    sleep 0.5
  done
  echo "workers did not join the group"; dc logs --tail 30 $names; return 1
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

# Nothing waiting, nothing outstanding. Bounded, because in two of these
# scenarios `pending` is never going to reach zero — which is the finding.
settle_queue() {
  local budget=${1:-90} line lag pend
  for _ in $(seq 1 "$budget"); do
    line=$(python3 scripts/queue-state.py --label settle 2>/dev/null | grep '^QUEUE' || true)
    lag=$(echo "$line" | sed -n 's/.*lag=\([0-9]*\).*/\1/p')
    pend=$(echo "$line" | sed -n 's/.*pending=\([0-9]*\).*/\1/p')
    [ "${lag:-1}" = 0 ] && [ "${pend:-1}" = 0 ] && break
    sleep 1
  done
  settle_books
}

# ── Reads every book and prints one machine-parsable line ──────────────────
# Three books, and this episode needs all three. The ledgers say how much money
# moved. job_runs says what the QUEUE did, and it is the only one that can show
# a job running twice while the money moves once — which is the state the last
# scenario is trying to reach.
tally() {
  local name=$1 enqueued=$2
  local app_charges app_cents proc_charges proc_cents dupes dupe_charges
  local deliveries msgs redelivered extra unfinished replayed failed
  app_charges=$(psql -c 'SELECT count(*) FROM charges;')
  app_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM charges;')
  proc_charges=$(psql -c 'SELECT count(*) FROM processor.ledger;')
  proc_cents=$(psql -c 'SELECT coalesce(sum(amount_cents),0) FROM processor.ledger;')
  dupes=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1) d;')
  dupe_charges=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM processor.ledger GROUP BY customer_id) d;')
  deliveries=$(psql -c 'SELECT count(*) FROM job_runs;')
  msgs=$(psql -c 'SELECT count(DISTINCT message_id) FROM job_runs;')
  redelivered=$(psql -c 'SELECT count(*) FROM (SELECT 1 FROM job_runs GROUP BY message_id HAVING count(*) > 1) d;')
  extra=$(psql -c 'SELECT coalesce(sum(n - 1),0) FROM (SELECT count(*) AS n FROM job_runs GROUP BY message_id) d;')
  unfinished=$(psql -c "SELECT count(*) FROM job_runs WHERE outcome = 'started' AND finished_at IS NULL;")
  replayed=$(psql -c "SELECT count(*) FROM job_runs WHERE outcome = 'replayed';")
  failed=$(psql -c "SELECT count(*) FROM job_runs WHERE outcome = 'failed';")
  echo "RESULT $name messages_enqueued=$enqueued app_charges=$app_charges app_cents=$app_cents" \
       "processor_charges=$proc_charges collected_cents=$proc_cents" \
       "double_charged_customers=$dupes duplicate_charges=$dupe_charges" \
       "deliveries=$deliveries messages_delivered=$msgs jobs_run_twice=$redelivered" \
       "redeliveries=$extra jobs_never_attempted=$((enqueued - msgs))" \
       "unfinished_runs=$unfinished replays=$replayed failed_runs=$failed"
}

# The order the queue actually completed the work in, beside the order it was
# published in. Asserting that retries break ordering is easy; this prints it.
ordering() {
  echo "-- published order vs completed order"
  psqlt -c "SELECT seq, customer_id, delivery, claimed, outcome, to_char(finished_at, 'MI:SS.MS') AS finished FROM job_runs ORDER BY started_at;"
  echo "ORDER completed=$(psql -c "SELECT string_agg(seq::text, ',' ORDER BY finished_at) FROM job_runs WHERE outcome IN ('charged','replayed');")"
}

worker_logs() { dc logs --no-log-prefix worker-1 worker-2 > "$OUT/$1" 2>&1 || true; }

# ── 1. Up ──────────────────────────────────────────────────────────────────
log "1/9  Starting the stack: postgres, redis, the processor, Episode 2's app, two workers"
dc down -v --remove-orphans >/dev/null 2>&1 || true
dc up --build -d 2>&1 | tee "$OUT/01-compose-up.log"
wait_healthy
{
  echo "processor latency = LATENCY_BASE_MS + (customer_id * 137) % LATENCY_SPREAD_MS"
  dc exec -T processor printenv LATENCY_BASE_MS LATENCY_SPREAD_MS | tr '\n' ' '; echo
  printf 'app idempotency mode = '; curl -fsS localhost:8000/health; echo
  printf 'worker defaults = '
  dc exec -T worker-1 printenv ACK_MODE VISIBILITY_TIMEOUT_MS HEARTBEAT MAX_DELIVERIES IDEMPOTENT_CONSUMER WORKER_BATCH | tr '\n' ' '; echo
  echo "redis = $(rcli INFO server | sed -n 's/^redis_version:\(.*\)/\1/p' | tr -d '\r')"
} >> "$OUT/01-compose-up.log"
cat "$OUT/01-compose-up.log" | tail -8

# ── 2. The visibility timeout, with nothing killed ─────────────────────────
log "2/9  Twenty-five jobs, two workers, a two-second lease. Nothing crashes."
reset_all
export ACK_MODE=after VISIBILITY_TIMEOUT_MS=2000 HEARTBEAT=0 MAX_DELIVERIES=0 IDEMPOTENT_CONSUMER=0 WORKER_BATCH=1
{
  start_workers worker-1 worker-2
  python3 scripts/enqueue.py --customers $FLEET --label lease
} 2>&1 | tee "$OUT/02-lease.log"
settle_queue 120
{
  python3 scripts/queue-state.py --label lease-finished
  echo
  echo "-- messages handed to more than one worker"
  psqlt -c "SELECT seq, customer_id, count(*) AS deliveries, string_agg(DISTINCT worker, ' + ') AS workers FROM job_runs GROUP BY seq, customer_id HAVING count(*) > 1 ORDER BY seq LIMIT 12;"
  echo
  echo "-- who paid twice"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1 ORDER BY customer_id LIMIT 12;"
  tally lease 25
} 2>&1 | tee -a "$OUT/02-lease.log"
worker_logs 03-lease-workers.log

# ── 3. The same, holding the lease ─────────────────────────────────────────
log "3/9  The identical run, with the worker holding its lease while it works"
reset_all
export HEARTBEAT=1
{
  start_workers worker-1 worker-2
  python3 scripts/enqueue.py --customers $FLEET --label heartbeat
} 2>&1 | tee "$OUT/04-heartbeat.log"
settle_queue 120
{
  python3 scripts/queue-state.py --label heartbeat-finished
  echo "-- lease extensions the workers had to send to keep their own jobs"
  echo "EXTENSIONS count=$(dc logs --no-log-prefix worker-1 worker-2 2>/dev/null | grep -c 'LEASE ' || true)"
  tally heartbeat 25
} 2>&1 | tee -a "$OUT/04-heartbeat.log"
worker_logs 05-heartbeat-workers.log

# ── 4 and 5. A worker is killed, once each way ─────────────────────────────
# Same five jobs, same kill, at the same moment. The only difference is which
# side of the work the XACK is on.
kill_run() {
  local ack=$1 label=$2
  reset_all
  export ACK_MODE=$ack VISIBILITY_TIMEOUT_MS=2000 HEARTBEAT=0 MAX_DELIVERIES=0 \
         IDEMPOTENT_CONSUMER=0 WORKER_BATCH=5
  # Publish BEFORE any consumer exists, so that one worker is handed all five
  # in a single read. This is not staging; it is the ordinary state of a queue
  # with a backlog, and every queue client reads in batches (SQS
  # MaxNumberOfMessages, Kafka max.poll.records).
  #
  # It matters because a batch of one cannot show what at-most-once costs.
  # Measured on the first capture of this episode: with the worker already
  # running, the producer's XADDs raced its blocking XREADGROUP, it was handed
  # one message at a time, and ack-before lost nothing at all — the four
  # messages it had not read yet had not been acknowledged either.
  python3 scripts/enqueue.py --customers $SLOW --label "$label"
  echo "-- five jobs waiting, nobody consuming. One worker starts and takes all five."
  start_workers worker-1
  echo "-- it is now 1.5 s into the first of them, a 3.5 s payment."
  sleep 1.5
  dc kill worker-1 2>&1 || true
  echo "KILL $label worker=worker-1 at_seconds=1.5 batch=5"
  sleep 1
  python3 scripts/queue-state.py --label "$label-after-the-kill"
  echo
  echo "-- starting worker-2. Nothing about it knows a worker died."
  start_workers worker-2
  settle_queue 90
  python3 scripts/queue-state.py --label "$label-finished"
  echo
  echo "-- the processor's books"
  psqlt -c "SELECT customer_id, count(*) AS times_charged, sum(amount_cents) AS cents FROM processor.ledger GROUP BY customer_id ORDER BY customer_id;"
  echo "-- what the queue did, delivery by delivery"
  psqlt -c "SELECT seq, customer_id, worker, delivery, claimed, outcome FROM job_runs ORDER BY started_at;"
  tally "$label" 5
}

log "4/9  Ack AFTER the work. Kill the worker mid-job."
kill_run after kill_after 2>&1 | tee "$OUT/06-kill-after.log"
worker_logs 07-kill-after-workers.log

log "5/9  The same five jobs, the same kill. Ack BEFORE the work."
kill_run before kill_before 2>&1 | tee "$OUT/08-kill-before.log"
worker_logs 09-kill-before-workers.log

# ── 6. The poison message ──────────────────────────────────────────────────
log "6/9  One message that can never succeed, and no limit on redelivery"
reset_all
export ACK_MODE=after VISIBILITY_TIMEOUT_MS=2000 HEARTBEAT=0 MAX_DELIVERIES=0 \
       IDEMPOTENT_CONSUMER=0 WORKER_BATCH=1
{
  start_workers worker-1
  python3 scripts/enqueue.py --poison --label poison
  python3 scripts/dlq-watch.py --seconds 30 --label poison
  # Stop the worker before counting. It redelivers every two seconds and would
  # otherwise keep doing so while the tally runs, so "deliveries in 30 seconds"
  # has to actually stop at 30 seconds. (First capture reported 15 and 16 for
  # the same run, measured one second apart.)
  dc stop worker-1 >/dev/null 2>&1
  echo
  python3 scripts/queue-state.py --label poison-after-30s
  echo
  echo "-- every delivery of that one message"
  psqlt -c "SELECT delivery, worker, claimed, outcome, detail, to_char(started_at, 'MI:SS.MS') AS at FROM job_runs ORDER BY started_at LIMIT 20;"
  echo "POISON deliveries=$(psql -c 'SELECT count(*) FROM job_runs;')" \
       "residency_seconds=$(psql -c "SELECT round(extract(epoch FROM max(started_at) - min(started_at))::numeric, 1) FROM job_runs;")" \
       "dead_lettered=$(rcli XLEN checkouts:dead | tr -d '\r')"
  tally poison 1
} 2>&1 | tee "$OUT/10-poison.log"
worker_logs 11-poison-workers.log

# ── 7. The same message, with a delivery limit and somewhere to put it ─────
log "7/9  The same poison message, with a delivery limit and a dead-letter stream"
reset_all
export MAX_DELIVERIES=5
{
  start_workers worker-1
  python3 scripts/enqueue.py --poison --poison-first --customers 18 19 20 --label dlq
  python3 scripts/dlq-watch.py --seconds 30 --label dlq
  echo
  python3 scripts/queue-state.py --label dlq-finished
  echo
  echo "-- what is in the dead-letter stream, and why"
  rcli XRANGE checkouts:dead - + | tr -d '\r'
  echo
  psqlt -c "SELECT seq, customer_id, delivery, outcome, detail FROM job_runs ORDER BY started_at;"
  echo "DLQ deliveries_before_dlq=$(psql -c 'SELECT count(*) FROM job_runs WHERE customer_id = 0;')" \
       "seconds_to_dlq=$(psql -c "SELECT round(extract(epoch FROM max(started_at) - min(started_at))::numeric, 1) FROM job_runs WHERE customer_id = 0;")" \
       "depth=$(rcli XLEN checkouts:dead | tr -d '\r')"
  tally dlq 4
} 2>&1 | tee "$OUT/12-dlq.log"
worker_logs 13-dlq-workers.log

# ── 8. Retries break ordering ──────────────────────────────────────────────
log "8/9  Ten jobs in order. Number five fails once."
reset_all
export ACK_MODE=after VISIBILITY_TIMEOUT_MS=15000 HEARTBEAT=0 MAX_DELIVERIES=0 \
       IDEMPOTENT_CONSUMER=0 WORKER_BATCH=1
{
  start_workers worker-1
  python3 scripts/enqueue.py --customers $FAST --fail-seq 5 --fail-times 1 --label ordering
} 2>&1 | tee "$OUT/14-ordering.log"
settle_queue 120
{ ordering; tally ordering 10; } 2>&1 | tee -a "$OUT/14-ordering.log"
worker_logs 15-ordering-workers.log

# ── 9. Turn Episode 2's key back on ────────────────────────────────────────
log "9/9  Scenario 2 again, with the producer's key passed to Episode 2's endpoint"
reset_all
export ACK_MODE=after VISIBILITY_TIMEOUT_MS=2000 HEARTBEAT=0 MAX_DELIVERIES=0 \
       IDEMPOTENT_CONSUMER=1 WORKER_BATCH=1
{
  start_workers worker-1 worker-2
  python3 scripts/enqueue.py --customers $FLEET --label keyed
} 2>&1 | tee "$OUT/16-keyed.log"
settle_queue 150
{
  python3 scripts/queue-state.py --label keyed-finished
  echo
  echo "-- messages STILL handed to more than one worker. The queue did not improve."
  psqlt -c "SELECT seq, customer_id, count(*) AS deliveries, string_agg(outcome, ' then ' ORDER BY started_at) AS outcomes FROM job_runs GROUP BY seq, customer_id HAVING count(*) > 1 ORDER BY seq LIMIT 12;"
  echo
  echo "-- who paid twice"
  psqlt -c "SELECT customer_id, count(*) AS times_charged FROM processor.ledger GROUP BY customer_id HAVING count(*) > 1;"
  tally keyed 25
} 2>&1 | tee -a "$OUT/16-keyed.log"
worker_logs 17-keyed-workers.log

log "The three sets of books, after the key was turned on"
{
  echo "-- what the queue did"
  psqlt -c "SELECT count(*) AS deliveries, count(DISTINCT message_id) AS messages, count(*) FILTER (WHERE claimed) AS redeliveries, count(*) FILTER (WHERE outcome = 'replayed') AS replays FROM job_runs;"
  echo "-- what our application believes"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM charges;"
  echo "-- what actually happened to money"
  psqlt -c "SELECT count(*) AS charges, sum(amount_cents) AS cents, count(DISTINCT customer_id) AS customers FROM processor.ledger;"
} 2>&1 | tee "$OUT/18-books.log"

# Leave the stack in the state a viewer should be poking at: the lease is held,
# the delivery limit is set, and the consumer sends the key.
export ACK_MODE=after VISIBILITY_TIMEOUT_MS=2000 HEARTBEAT=1 MAX_DELIVERIES=5 \
       IDEMPOTENT_CONSUMER=1 WORKER_BATCH=1
start_workers worker-1 worker-2 >/dev/null

log "Summarising"
python3 scripts/summarise.py

echo
echo "Done. Real numbers are in $OUT/metrics.json"
