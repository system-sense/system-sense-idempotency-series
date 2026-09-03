# A Timeout Is Not a Failure

**A written companion to Episode 1 of System Sense — [Exactly-Once Is a Lie](../).**

The video is about eleven minutes. This covers the same ground more slowly, with
the code in full, and then goes on into what would not fit: the complete list of
places a duplicate can come from and which of them you actually control, what
RFC 9110 does and does not say about `POST`, how to configure a retry so it stops
being the thing that charges people twice, and why cancelling a request does not
cancel the work.

Every figure here comes from `capture/metrics.json`, produced by
`./scripts/capture-demo.sh` in this folder. Nothing is estimated.

**Who this is for:** you have an endpoint that does something with real-world
consequences — takes a payment, sends an email, ships a box — and a client
somewhere that retries. By the end you will know exactly why that is a duplicate
generator, and why none of the obvious readings of a timeout are correct.

---

## Contents

1. [The failure, in one command](#1-the-failure-in-one-command)
2. [What a timeout actually tells you](#2-what-a-timeout-actually-tells-you)
3. [Where the bug is](#3-where-the-bug-is)
4. [Where duplicates actually come from](#4-where-duplicates-actually-come-from)
5. [What RFC 9110 says, precisely](#5-what-rfc-9110-says-precisely)
6. [Cancelling a request does not cancel the work](#6-cancelling-a-request-does-not-cancel-the-work)
7. [How to configure a retry that is not a duplicate generator](#7-how-to-configure-a-retry-that-is-not-a-duplicate-generator)
8. [Why not simply stop retrying](#8-why-not-simply-stop-retrying)
9. [Two sets of books](#9-two-sets-of-books)
10. [What to log](#10-what-to-log)
11. [When you can ignore all of this](#11-when-you-can-ignore-all-of-this)
12. [Exercises](#12-exercises)

---

## 1. The failure, in one command

```bash
docker compose up --build
./scripts/capture-demo.sh
```

Twenty-five customers. Each presses Pay exactly once, for $40.

| | |
| --- | --- |
| Checkouts | 25 |
| HTTP requests those 25 checkouts actually sent | **39** |
| Checkouts the customer was told had **failed** | **14** |
| Customers charged **twice** | 14 (**56%**) |
| Money owed | **$1,000** |
| Money collected | **$1,560** |
| Over-collected | **$560** |

Read the third and fourth rows together, because that is the whole episode: the
fourteen people who were charged twice are *the same fourteen* who were shown an
error saying the payment did not go through.

You can watch it happen to one person:

```bash
python3 scripts/checkout.py 18      # answers in 1.3s — one charge
python3 scripts/checkout.py 17      # takes 3.5s — times out, retries, charged twice
```

Customer 17 owes $40 and was charged $80.

---

## 2. What a timeout actually tells you

A client timeout is one of the most misread signals in software. Here is what it
means, exactly:

> **The response did not arrive within the time I was willing to wait.**

That is all. It says nothing about whether the request arrived, nothing about
whether the server started work, and nothing about whether the work finished.
Every one of the following is consistent with the timeout you just observed:

| What really happened | How often people assume this |
| --- | --- |
| The request never left your machine | often |
| The request arrived; the server has not started | sometimes |
| The server is still working on it | sometimes |
| **The server finished, and the response was lost or slow** | almost never |
| The server finished, responded, and your process gave up 1 ms earlier | never |

The last two are the expensive ones, and they are not rare. In this demo they
are the *majority* case: twenty-eight of the requests timed out, and every single
one of those charges had already been captured.

**A timeout is a statement about your patience, not about the state of the
world.** Once you have internalised that sentence the rest of this series is
mostly bookkeeping.

---

## 3. Where the bug is

Nowhere. That is the point, and it is the reason this episode exists before any
of the fixes.

Here is the handler, from [`app/main.py`](app/main.py):

```python
@app.post("/api/checkout")
async def checkout(req: CheckoutRequest):
    result = await asyncio.shield(asyncio.create_task(charge_customer(req)))
    return result
```

It calls the processor once, records the charge once, returns. No race, no
missing `await`, no swallowed exception.

And here is the client, from [`scripts/checkout.py`](scripts/checkout.py): a
two-second timeout and one retry. That is what an HTTP client library does by
default, what a mobile app does when the user goes through a tunnel, what a load
balancer does when an upstream is slow.

The retry is not a mistake either. Without it, one dropped packet loses the sale.

**The bug is in the assumption underneath the retry** — that a timeout means the
request did not happen. Nothing in the code states that assumption, which is
exactly why it survives code review.

The demo is deliberately built so you cannot dismiss it:

- **The processor is slow for some customers and fast for others**, by
  `1200 + (customer_id * 137) % 2400` ms, a range that straddles the client's
  2000 ms timeout. A processor that is *always* too slow would be a rigged demo.
  This is a tail-latency problem, which is what these actually are.
- **The latency is a deterministic function of the customer id**, so the same
  fourteen customers time out on your machine as on mine. The duplicate rate is
  a measurement, not a setting.

---

## 4. Where duplicates actually come from

The client retry in this demo is one source. It is not the only one, and it is
not even the one you are most likely to be missing. Here is the full list, with
the thing that matters most: **whether you control it.**

| Source | Layer | Do you control it? |
| --- | --- | --- |
| Your HTTP client retrying a timeout | your code | yes — and see §7 |
| A user double-clicking Pay | browser | partly (disable the button, but not reliably) |
| A user hitting refresh on a POST result | browser | no |
| Load balancer / reverse proxy retry | infrastructure | **often not** — and frequently on by default |
| Service mesh retry (Envoy, Linkerd) | infrastructure | configurable, often left at defaults |
| Mobile network handoff (wifi → cellular) | the physical world | no |
| A queue redelivering an unacknowledged message | broker | no — see [Episode 3](../episode-3-queues/) |
| A relay republishing an outbox row | your code | no — see [Episode 4](../episode-4-outbox/) |
| A webhook sender retrying on a non-2xx | somebody else's code | **absolutely not** |
| A batch job re-run after a partial failure | ops | sometimes |
| An agent workflow replaying a step | your framework | see [Episode 4 §18](../episode-4-outbox/GUIDE.md#18-the-same-problem-in-agent-workflows) |

Two observations from that table.

**Most of the sources are not in your code.** You can remove every retry from
your own client and still get duplicates, because a proxy you did not configure
retried a 504 for you. This is why "just don't retry" is not a strategy and why
the fix has to live at the endpoint.

**If you receive webhooks, you are already on the receiving end of this.** Stripe,
GitHub, Shopify and every other sender retries on timeouts and non-2xx responses.
They are all doing the correct thing, and they will all deliver the same event to
you more than once. If your webhook handler is not idempotent, it is not a
question of whether you will double-process — only when.

---

## 5. What RFC 9110 says, precisely

This gets quoted loosely, so here it is exactly. RFC 9110 defines two separate
properties:

- **Safe**: the method is essentially read-only. `GET`, `HEAD`, `OPTIONS`,
  `TRACE`.
- **Idempotent**: *the intended effect on the server of multiple identical
  requests is the same as the effect of a single such request.* `GET`, `HEAD`,
  `OPTIONS`, `TRACE`, **`PUT`** and **`DELETE`**.

`POST` and `PATCH` are neither.

Three things people get wrong about this:

**Idempotent is about the effect, not the response.** `DELETE /orders/7` twice
is idempotent — the order is gone either way — even though the first call
returns `204` and the second returns `404`. Different responses, same end state.
Idempotency does not promise the caller sees the same bytes; that is a stronger
property, and it is the one [Episode 2](../episode-2-keys/) has to build by hand.

**`PUT` is idempotent because of what it means, not because of magic.** `PUT`
says "make the resource be this", so applying it five times leaves the same
resource. If you implement `PUT /balance` as "add this amount", you have written
a non-idempotent `PUT` and the spec will not save you.

**It is a contract, not an enforcement.** Nothing stops you writing a `POST`
handler that is idempotent, and nothing stops you writing a `PUT` that is not.
What the spec buys you is that *everyone else's software* — proxies, meshes,
client libraries — is entitled to assume the standard behaviour and retry
accordingly. Which is exactly why the defaults in §7 are what they are.

You can watch the difference in this repo:

```bash
for i in 1 2 3 4 5; do curl -s localhost:8000/api/customers/18; echo; done
```

Five identical responses, nothing changed. The capture records the same for
`PUT`: three calls, one distinct response, one customer row.

---

## 6. Cancelling a request does not cancel the work

This is the detail that turns the bug from "unlucky" into "guaranteed", and it is
the one most people have not thought about.

When the client's timeout fires, it closes the socket. What happens on the server?

In most stacks: **nothing useful**. The handler is mid-flight in a call to the
payment processor. The processor has already received the charge. Closing a TCP
connection does not reach into another company's datacentre and undo a capture.

Some frameworks go further and *cancel the handler task* when the client
disconnects — which is worse, because now you have a captured payment that your
own database never recorded. You have converted "charged twice" into "charged
once and lost the receipt", which is harder to detect and harder to refund.

This demo deliberately does the right thing:

```python
result = await asyncio.shield(asyncio.create_task(charge_customer(req)))
```

`asyncio.shield` means a client hanging up does not cancel the charge. The work
completes and the row is written even though nobody is listening for the answer.

**That is the correct behaviour and it is why the duplicate happens.** Making the
server honest about what it did is what leaves a captured payment with no
listener — and the client, knowing nothing, retries.

If you take one operational action from this section: check whether your
framework cancels handlers on client disconnect, and decide deliberately. Both
answers are defensible. Not knowing which one you have is not.

---

## 7. How to configure a retry that is not a duplicate generator

Retries are good. Retries configured by default are frequently not. Concretely:

**Do not retry non-idempotent methods by default.** Most sensible clients already
refuse — Go's `net/http` transport only retries requests it can safely replay,
and `urllib3`'s `Retry` defaults to an allowlist that excludes `POST`. But plenty
of wrappers, meshes and gateways will happily retry a `POST` on a 504 or a
connection timeout, and that is a policy decision somebody made for you. Find it
and look at it.

**Retry on the right conditions.** A connection-establishment failure is safe to
retry: nothing was sent. A *read* timeout is not: the request was sent and you
have no idea what happened to it. Many libraries collapse both into one "timeout"
setting, which is how the unsafe case inherits the safe case's policy.

**Backoff with jitter, and a budget.** Fixed-interval retries synchronise across
your fleet and turn a slow dependency into an outage. Exponential backoff with
full jitter, plus a cap on the fraction of traffic that may be retries (a retry
budget), is the standard answer.

**Make the client timeout shorter than the server's.** In this repo the client
gives up at 2 s and the app is willing to wait 30 s on the processor, which is
what makes the failure mode "the client gave up first" rather than "the app gave
up first". If your timeouts are the other way round you get a different bug —
the server abandons work the client is still waiting for.

**And then accept that none of this is sufficient.** Every setting above lowers
the rate. Only an idempotency key changes the outcome, which is
[Episode 2](../episode-2-keys/). The point of tuning retries is to stop making
the problem worse, not to solve it.

---

## 8. Why not simply stop retrying

Because at-most-once is a worse trade than it sounds, and it is worth being
explicit about why rather than treating it as obvious.

If your client never retries, then every transient failure — a dropped packet, a
brief network blip, a pod restarting during a deploy — becomes a lost sale, a
lost signup, a lost order. Those are common. They are far more common than the
duplicate you were trying to avoid.

You are choosing between two failure modes:

| | at-most-once | at-least-once |
| --- | --- | --- |
| what goes wrong | work is silently lost | work happens twice |
| how you find out | you do not | reconciliation, or a customer complaint |
| can you recover | **no** — nothing has a record of it | yes — dedupe, or refund |

**At-least-once is the right default for one reason: duplicates you can solve,
and lost work you cannot.** No code recovers a request that nothing anywhere has
a record of.

[Episode 3](../episode-3-queues/) measures exactly this trade at the queue layer,
where the same choice appears as where you put the acknowledgement.

---

## 9. Two sets of books

This repo keeps two schemas on purpose:

```
public.charges      what OUR application believes it did
processor.ledger    what actually happened to money
```

The application never writes to `processor.ledger`; only the processor service
does. That separation is not decoration — it is the only thing in the repo that
can settle an argument, and every system that touches money has the same shape:
a second set of books, held by somebody else, reachable only across a network
call.

**Every number in this series lives in the gap between those two tables.** When
they agree, nothing interesting happened. When they disagree, the difference is
somebody's money.

Practically: if you take payments and you cannot answer "does our ledger match
the processor's for yesterday" with a query, you cannot detect any of the
failures in this series. Not the duplicates in this episode, not the races in
Episode 2, not the redeliveries in Episode 3, and least of all the silent losses
in Episode 4.

---

## 10. What to log

The reason this bug survives so long in real systems is that nothing in the
default logs says "this was a duplicate". Three cheap additions change that:

- **Log an attempt identifier the client generates**, not one the server mints. A
  server-generated request id is different for each retry by definition, so it
  can never group them. This is the seed of Episode 2's idempotency key, and even
  before you build the full pattern, having the field in your logs tells you your
  real duplicate rate.
- **Log the outcome of a write, not just its start.** "Charged customer 17" is
  useless if it appears twice and you cannot tell whether that was two intents or
  one intent twice.
- **Alert on the ledger gap, not on errors.** In this demo *nothing errored on
  the server*. The application's own logs show 39 successful checkouts, and every
  one of them was correct. The only signal that anything is wrong is $1,000 owed
  against $1,560 collected.

That last point generalises: **the failures in this series are invisible to
error-rate monitoring, because nothing fails.** You have to be watching a
business invariant.

---

## 11. When you can ignore all of this

Not everything needs an idempotency key, and pretending otherwise is how the
pattern gets a reputation for ceremony.

You can skip it when the operation is already idempotent by nature:

- **Setting a value.** `UPDATE users SET email = $1 WHERE id = $2` run five times
  leaves one email. Nothing to protect.
- **Writing with a deterministic primary key.** If the row's identity comes from
  the data (a natural key, a content hash), a second insert is a no-op or a
  conflict you can ignore.
- **Idempotent operations at the far end.** Uploading to a fixed S3 key; setting
  a DNS record; `kubectl apply`. The second call converges to the same state.

You need it when the operation **creates** something, **moves money**, or
**tells a human something**: charges, refunds, orders, emails, SMS, push
notifications, webhooks you send. The rule of thumb: if running it twice
produces two of something a person can see, it needs a key.

---

## 12. Exercises

**1. Hide the bug.** Run with `LATENCY_BASE_MS=100 LATENCY_SPREAD_MS=200`. Every
duplicate disappears. Nothing was fixed — the bug is behind a dependency that
happens to be fast today, which is exactly how it reaches production and exactly
why it surfaces on the day traffic doubles.

**2. Make it worse in the honest direction.** Raise the client's timeout above
the slowest payment. The duplicates vanish for the same non-reason. Now add one
customer slower than the new timeout.

**3. Check the processor's books directly.**

```bash
docker compose exec postgres psql -U sysense -d sysense \
  -c "SELECT * FROM processor.ledger WHERE customer_id = 17;"
```

Two rows, two charge ids, one press of Pay.

**4. Prove the shield matters.** Remove the `asyncio.shield` from the handler and
run the fleet again. Watch a captured payment that `public.charges` has no record
of — a worse bug than the one you started with, and a good argument for reading
§6 carefully.

**5. Find the retries you did not write.** In your own system, go and read the
retry policy of your ingress, your service mesh and your HTTP client wrapper.
Count how many of them will retry a `POST`.

---

## Where to go next

The obvious fix is to give every press of Pay a key and check whether you have
seen it. **That fix has a race condition in it**, and finding it is
[Episode 2](../episode-2-keys/) — with the guide at
[`episode-2-keys/GUIDE.md`](../episode-2-keys/GUIDE.md).

- [Episode 3 — queues](../episode-3-queues/GUIDE.md): the same bug one layer down,
  where nobody chose to retry and it happened anyway.
- [Episode 4 — the outbox](../episode-4-outbox/GUIDE.md): why exactly-once
  delivery is impossible, and what to build instead.

---

Part of the **System Sense — Idempotency** mini-series.
