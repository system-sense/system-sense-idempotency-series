-- System Sense — Idempotency Ep.1: The Retry That Charged Your Customer Twice
--
-- Two ledgers on purpose, in two schemas, because the whole episode turns on
-- the gap between them:
--
--   public.charges       what OUR application believes it did
--   processor.ledger     what the PAYMENT PROCESSOR actually did with money
--
-- They are separated by a network call, and a network call can succeed while
-- its response is lost. When those two tables disagree, a real customer is out
-- real money.

-- ── Customers ──────────────────────────────────────────────────────────────
CREATE TABLE customers (
    id         INT PRIMARY KEY,
    name       TEXT        NOT NULL,
    email      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO customers (id, name, email)
SELECT i,
       'Customer ' || i,
       'customer' || i || '@example.com'
FROM generate_series(1, 25) AS i;

-- ── Our side of the story ──────────────────────────────────────────────────
-- Note what is NOT here: nothing that could tell two requests apart. There is
-- no idempotency key, no unique constraint, nothing the database could refuse.
-- Every POST that arrives is a brand new charge as far as this table knows.
-- That is Episode 2's problem, and it is deliberately unsolved here.
CREATE TABLE charges (
    id                   BIGSERIAL PRIMARY KEY,
    customer_id          INT         NOT NULL REFERENCES customers(id),
    amount_cents         INT         NOT NULL,
    processor_charge_id  TEXT        NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX charges_customer_idx ON charges (customer_id, created_at);

-- ── The processor's side of the story ──────────────────────────────────────
-- A separate schema so that every query against it reads, on screen, as
-- "and this is what the bank saw". The application never writes here; only the
-- processor service does.
CREATE SCHEMA processor;

CREATE TABLE processor.ledger (
    id           TEXT PRIMARY KEY,
    customer_id  INT         NOT NULL,
    amount_cents INT         NOT NULL,
    captured_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ledger_customer_idx ON processor.ledger (customer_id, captured_at);
