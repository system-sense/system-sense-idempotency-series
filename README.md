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
| 2. Your Idempotency Key Has a Race Condition | [`episode-2-keys/`](episode-2-keys/) | "Check, then insert" is TOCTOU, and the `UNIQUE` constraint only helps if you claim the key before you do the work. | [**watch**](https://www.youtube.com/watch?v=g19jax6Auxc) |
| 3. 3 Ways Your Queue Silently Loses Work | [`episode-3-queues/`](episode-3-queues/) | Visibility timeouts, poison messages, dead-letter queues. Episode 2's endpoint, unchanged, still charging people twice. | demo built |
| 4. Exactly-Once Delivery Is a Lie | [`episode-4-outbox/`](episode-4-outbox/) | The dual write. Exactly-once *delivery* is impossible; exactly-once *effects* are at-least-once plus an idempotent consumer. | demo built · [**guide**](episode-4-outbox/GUIDE.md) |

## Watch

Full playlist: https://www.youtube.com/playlist?list=PLMlexv0Ndaog

Episode 1 — *The Retry That Charged Your Customer Twice* (https://www.youtube.com/watch?v=KE7CCnTfQqk)

Episode 2 — *Your Idempotency Key Has a Race Condition* (https://www.youtube.com/watch?v=g19jax6Auxc)

Each episode's folder here is the demo from that video, and every figure quoted
on screen is reproducible from it.

## Prefer to read?

**[Episode 4 — The Dual Write, the Transactional Outbox, and Exactly-Once](episode-4-outbox/GUIDE.md)**
is a written companion to that episode: the same ground more slowly, with the
code in full, and then the questions the video's runtime could not fit —
running more than one relay, what happens as the outbox table grows, what the
pattern does and does not say about ordering, and why two-phase commit is not
the answer it looks like.

Guides for episodes 1 to 3 are being written.

The folder `README.md` tells you what a demo is and how to run it. The `GUIDE.md`
teaches the concept, with that demo as the evidence.

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
