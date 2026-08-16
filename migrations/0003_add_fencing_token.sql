-- PostgreSQL is the fenced resource. It remembers the newest Redis lock owner
-- that reached it so an older, expired owner cannot resume and write later.
ALTER TABLE lockers
ADD COLUMN last_fencing_token BIGINT NOT NULL DEFAULT 0;
