# System Sense — Idempotency Mini-Series

The runnable demos for the four-part **"Exactly-Once Is a Lie"** series.

Most engineers believe their queue gives them exactly-once delivery. It does
not, and it cannot. What is achievable is exactly-once *effects* — at-least-once
delivery plus a consumer that can be called twice without doing the work twice.
Each episode is one layer of that sentence, with a demo you can run.

Stack: Python + FastAPI and PostgreSQL 16 in Docker, carried forward from the
Caching series, with Redis added in Episode 3 where it earns its place.

| Episode | Folder | Thesis | State |
| --- | --- | --- | --- |
| 1. The Retry That Charged Your Customer Twice | [`episode-1-duplicates/`](episode-1-duplicates/) | A timeout is not a failure. The request succeeded; only the response was lost. | [**watch**](https://www.youtube.com/watch?v=KE7CCnTfQqk) |
| 2. Your Idempotency Key Has a Race Condition | `episode-2-keys/` | "Check, then insert" is TOCTOU. Let a `UNIQUE` constraint arbitrate, not your application. | not built |
| 3. 3 Ways Your Queue Silently Loses Work | `episode-3-queues/` | Visibility timeouts, poison messages, dead-letter queues. | not built |
| 4. Exactly-Once Delivery Is a Lie | `episode-4-outbox/` | Exactly-once *delivery* is impossible. Exactly-once *effects* are not. | not built |

## Watch

Episode 1 — *The Retry That Charged Your Customer Twice* (https://www.youtube.com/watch?v=KE7CCnTfQqk)

Each episode's folder here is the demo from that video, and every figure quoted
on screen is reproducible from it.

## Start here

```bash
cd episode-1-duplicates
docker compose up --build
./scripts/capture-demo.sh      # in another terminal
```

Twenty-five customers press Pay once each, for $40. Fourteen of them are charged
twice — and those same fourteen are shown an error saying the payment failed.
$1,000 owed, $1,560 collected.

Every number in the videos comes out of that script. `capture/` is committed so
you can check the claims without running anything.

## The previous series

Caching, four episodes, published:
https://github.com/system-sense/system-sense-caching-series
