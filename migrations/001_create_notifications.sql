-- Notification history persisted by the fan-out worker (Person A).
-- The Read API (Person B) queries this table on Redis cache miss.
CREATE TABLE IF NOT EXISTS notifications (
    id         BIGINT      PRIMARY KEY,                      -- Snowflake ID from the ingestion service
    user_id    TEXT        NOT NULL,
    payload    JSONB       NOT NULL,                         -- Full notification JSON written to Redis
    ts_ms      BIGINT      NOT NULL,                         -- Unix milliseconds; used as the Redis sorted-set score
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Supports the fallback query: WHERE user_id = $1 ORDER BY ts_ms DESC LIMIT $2
CREATE INDEX IF NOT EXISTS idx_notifications_user_ts ON notifications (user_id, ts_ms DESC);
