-- followers table — who follows whom
-- follower_id  = the person who will receive notifications
-- following_id = the person being followed (the event producer)
CREATE TABLE IF NOT EXISTS followers (
    id            BIGSERIAL PRIMARY KEY,
    follower_id   TEXT NOT NULL,
    following_id  TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (follower_id, following_id)
);

-- notifications table — one record per notification per recipient
-- delivered = FALSE means the user was offline when it was sent
-- gateway replays all delivered = FALSE records on reconnect
-- primary key is (id, recipient_id) because the same event can have multiple recipients
CREATE TABLE IF NOT EXISTS notifications (
    id            BIGINT NOT NULL,           -- Snowflake ID from Kafka
    recipient_id  TEXT NOT NULL,
    from_user     TEXT NOT NULL,
    type          TEXT NOT NULL,             -- POST, LIKE, COMMENT
    message       TEXT NOT NULL,
    delivered     BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, recipient_id)           -- composite key — one row per event per recipient
);

-- fast lookup of undelivered notifications per user on reconnect
CREATE INDEX IF NOT EXISTS idx_notifications_recipient
    ON notifications(recipient_id, delivered, created_at DESC);

-- events table — used by fan-out on read strategy
-- stores one record per event instead of one per recipient
-- recipients are resolved at read time (for high-follower accounts)
CREATE TABLE IF NOT EXISTS events (
    id            BIGINT PRIMARY KEY,       -- Snowflake ID from Kafka
    from_user     TEXT NOT NULL,
    type          TEXT NOT NULL,
    message       TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- Seed data — follower counts for experiments
-- alice has 10 followers  (baseline)
-- bob   has 100 followers (small scale)
-- carol has 1000 followers (crossover threshold)
-- dave  has 10000 followers (celebrity account)
-- ─────────────────────────────────────────────

-- alice's 10 followers
INSERT INTO followers (follower_id, following_id) VALUES
    ('follower_a_1',  'alice'), ('follower_a_2',  'alice'),
    ('follower_a_3',  'alice'), ('follower_a_4',  'alice'),
    ('follower_a_5',  'alice'), ('follower_a_6',  'alice'),
    ('follower_a_7',  'alice'), ('follower_a_8',  'alice'),
    ('follower_a_9',  'alice'), ('follower_a_10', 'alice')
ON CONFLICT DO NOTHING;

-- bob's 100 followers (generated pattern)
INSERT INTO followers (follower_id, following_id)
SELECT 'follower_b_' || i, 'bob'
FROM generate_series(1, 100) AS i
ON CONFLICT DO NOTHING;

-- carol's 1000 followers
INSERT INTO followers (follower_id, following_id)
SELECT 'follower_c_' || i, 'carol'
FROM generate_series(1, 1000) AS i
ON CONFLICT DO NOTHING;

-- dave's 10000 followers
INSERT INTO followers (follower_id, following_id)
SELECT 'follower_d_' || i, 'dave'
FROM generate_series(1, 10000) AS i
ON CONFLICT DO NOTHING;