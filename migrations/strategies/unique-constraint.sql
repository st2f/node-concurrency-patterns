-- This migration is strategy-specific rather than part of the shared base
-- schema. Applying it to every isolated schema would silently protect the
-- baseline and the strategies whose limitations the test suite demonstrates.
CREATE UNIQUE INDEX one_active_checkout_per_locker
ON checkouts (locker_id)
WHERE released_at IS NULL;
