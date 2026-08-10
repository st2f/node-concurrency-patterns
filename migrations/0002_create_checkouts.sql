-- Deliberately no uniqueness constraint here: Step 1 needs the race to be
-- reproducible with zero protection. The partial unique index is added by
-- the "unique constraint" strategy (Step 2, item 5) as its own migration.
CREATE TABLE checkouts (
  id SERIAL PRIMARY KEY,
  locker_id INTEGER NOT NULL REFERENCES lockers(id),
  user_id TEXT NOT NULL,
  checked_out_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ
);

CREATE INDEX checkouts_locker_id_idx ON checkouts (locker_id);

-- `version` supports the optimistic-locking strategy (Step 2, item 4):
-- bumped on every checkout/release, compared-and-swapped by the writer.
ALTER TABLE lockers ADD COLUMN version INTEGER NOT NULL DEFAULT 0;
