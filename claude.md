# NotificationSystem — Project Guide (Week 2)

This file is a context document for Claude. Paste it at the start of a new conversation to continue where we left off.

---

## Project Overview

CS6650 Building Scalable Distributed Systems — team of 3 building a distributed real-time notification system inspired by Twitter's architecture. The repo is at github.com/Tanyayya/NotificationSystem.

**Team:** Srimansi Ramesh (you), Tanya Shukla, Mackenzie Veazey

**Stack:** Go, Kafka (MSK), Redis (ElastiCache), PostgreSQL (RDS), WebSockets, AWS ECS Fargate, Terraform

---

## Architecture

```
POST /event → Ingestion API → Kafka (MSK, topic: worker-events)
                                    ↓
                            Fan-out Worker
                         (queries PostgreSQL followers table)
                         (fan-out on write if followers <= 1000)
                         (fan-out on read stub if followers > 1000)
                                    ↓
                    Redis Pub/Sub (notif:{userID})
                                    ↓
              WebSocket Gateway (goroutine per connection)
                                    ↓
                                 Clients
```

---

## Current State (End of Week 2 in progress)

### Completed

- **Week 1:** WebSocket gateway fully working — connection lifecycle, Redis registration, Pub/Sub delivery
- **Week 2 fan-out:** Fan-out on write and fan-out on read stub implemented and tested locally and on AWS
- **PostgreSQL:** Schema created, seed data seeded, RDS running on AWS
- **AWS infra:** Full stack deployed — ECS, MSK, ElastiCache, RDS, ALB, ECR — all via Terraform

### In Progress / Not Yet Done

- **Offline backlog replay** in the gateway — when a user reconnects, replay all `delivered=false` notifications from PostgreSQL
- **Mark delivered** — after replaying, mark notifications as `delivered=true`
- Commit and push Week 2 fanout code to main

---

## PostgreSQL Schema

```sql
-- who follows whom
CREATE TABLE followers (
    id            BIGSERIAL PRIMARY KEY,
    follower_id   TEXT NOT NULL,  -- receives notifications
    following_id  TEXT NOT NULL,  -- being followed
    UNIQUE (follower_id, following_id)
);

-- one record per notification per recipient
-- composite PK because same event can go to multiple recipients
CREATE TABLE notifications (
    id            BIGINT NOT NULL,
    recipient_id  TEXT NOT NULL,
    from_user     TEXT NOT NULL,
    type          TEXT NOT NULL,
    message       TEXT NOT NULL,
    delivered     BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, recipient_id)
);

-- used by fan-out on read — one row per event instead of one per recipient
CREATE TABLE events (
    id            BIGINT PRIMARY KEY,
    from_user     TEXT NOT NULL,
    type          TEXT NOT NULL,
    message       TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
```

**Seed data:** alice=10 followers, bob=100, carol=1000, dave=10000 (for Experiment 1)

---

## Fan-out Logic

**File:** `fanout/internal/fanout/fanout.go`

- Threshold: 1000 followers (set via `FANOUT_THRESHOLD` env var)
- Under threshold: fan-out on write — one `notifications` row per follower + Redis PUBLISH per follower
- Over threshold: fan-out on read stub — one `events` row, log message, no Redis publish yet

**DB layer:** `fanout/internal/db/postgres.go`

- `GetFollowers(ctx, followingID)` — queries followers table
- `InsertNotification(ctx, recipientID, ev)` — writes one row per follower
- `InsertEvent(ctx, ev)` — writes one row for high-follower accounts
- `GetUndeliveredNotifications(ctx, recipientID)` — for gateway backlog replay
- `MarkDelivered(ctx, notificationID, recipientID)` — marks after delivery

---

## Service Contracts

### Kafka (ingestion → fanout)

- Topic: `worker-events`
- Key: `from_user` (sender)
- Value: `{"id":"...","type":"POST","from_user":"alice","detail":"...","timestamp":...}`
- Valid types: `POST`, `LIKE`, `COMMENT`

### Redis Pub/Sub (fanout → gateway)

- Channel: `notif:{userID}`
- Payload: `{"id":...,"type":"...","from_user":"...","message":"...","timestamp":...}`

### Redis Key Schema

- `ws:user:{userID}` → taskID (gateway registration, TTL 120s)

---

## Gateway Files

```
gateway/
├── main.go      — boots HTTP server, initializes Redis
├── handler.go   — WebSocket lifecycle, reader/writer/subscriber goroutines
├── redis.go     — register/deregister user on connect/disconnect
└── pubsub.go    — Redis Pub/Sub subscription, forwards to writeCh
```

**Key patterns:**

- Single writer goroutine + buffered `writeCh` (capacity 64) — prevents concurrent write panics
- `context.WithCancel` for clean goroutine teardown
- 3 goroutines per connection: reader, writer, subscriber

---

## What Needs to Be Built Next

### 1. Offline backlog replay in gateway (priority)

When a user connects, after registering in Redis and before subscribing to Pub/Sub, query PostgreSQL for all undelivered notifications and send them over the WebSocket.

Rough flow:

```
user connects → register in Redis → query GetUndeliveredNotifications →
send each over WebSocket → mark each as delivered → subscribe to Pub/Sub
```

This requires:

- Adding `DB_DSN` env var to the gateway ECS task and docker-compose
- Adding a PostgreSQL client to the gateway (`gateway/db.go` or similar)
- Calling `GetUndeliveredNotifications` in `handler.go` after registration
- Calling `MarkDelivered` after sending each notification

### 2. Commit and push current Week 2 work

```bash
git add fanout/internal/db/postgres.go \
        fanout/internal/fanout/fanout.go \
        fanout/internal/config/config.go \
        fanout/worker/main.go \
        db/init.sql \
        docker-compose.yml \
        go.mod go.sum
git commit -m "feat: week 2 - fan-out on write/read, PostgreSQL persistence"
git push origin main  # or your branch
```

---

## AWS Infrastructure

**Sandbox account:** 885581373325  
**Region:** us-east-1

**Current outputs (from last terraform apply):**

```
alb_dns         = ws://notif-system-alb-1989810179.us-east-1.elb.amazonaws.com:8080
rds_endpoint    = notif-system-postgres.cgnw6a4m2b19.us-east-1.rds.amazonaws.com
redis_endpoint  = notif-system-redis.umdvt3.0001.use1.cache.amazonaws.com:6379
kafka_brokers   = b-1.notifsystemkafka.f4fvbe.c21.kafka.us-east-1.amazonaws.com:9092,...
ecr_gateway     = 885581373325.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway
ecr_ingestion   = 885581373325.dkr.ecr.us-east-1.amazonaws.com/notif-system-ingestion
ecr_fanout      = 885581373325.dkr.ecr.us-east-1.amazonaws.com/notif-system-fanout
```

**DB credentials:** user=notif, password=notif_password_123, db=notifications

**Deploy commands:**

```bash
# build and push (from repo root, Mac Apple Silicon)
docker buildx build --platform linux/amd64 --load -t notif-system-gateway -f gateway/Dockerfile .
docker tag notif-system-gateway:latest 885581373325.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway:latest
docker push 885581373325.dkr.ecr.us-east-1.amazonaws.com/notif-system-gateway:latest

# fanout needs --target worker
docker buildx build --platform linux/amd64 --load --target worker -t notif-system-fanout -f fanout/Dockerfile .

# force new deployments
aws ecs update-service --cluster notif-system-cluster --service notif-system-gateway --force-new-deployment --region us-east-1
aws ecs update-service --cluster notif-system-cluster --service notif-system-ingestion --force-new-deployment --region us-east-1
aws ecs update-service --cluster notif-system-cluster --service notif-system-fanout --force-new-deployment --region us-east-1
```

---

## Local Dev

```bash
# start full stack
docker compose up --build

# test fan-out on write (alice has 10 followers)
wscat -c "ws://localhost:8080/ws?user_id=follower_a_1"
curl -X POST http://localhost:3000/event \
  -H "Content-Type: application/json" \
  -d '{"type":"POST","from_user":"alice","detail":"Alice posted a photo"}'

# verify PostgreSQL
docker exec -it notificationsystem-postgres-1 psql -U notif -d notifications \
  -c "SELECT recipient_id, delivered FROM notifications ORDER BY recipient_id"

# test fan-out on read (dave has 10000 followers)
curl -X POST http://localhost:3000/event \
  -H "Content-Type: application/json" \
  -d '{"type":"POST","from_user":"dave","detail":"Dave posted a photo"}'

# verify event table (should have 1 row, notifications should NOT grow by 10000)
docker exec -it notificationsystem-postgres-1 psql -U notif -d notifications \
  -c "SELECT id, from_user FROM events"
```

---

## Approach and Preferences

- Step-by-step guidance — confirm each step works before moving to the next
- Explain unfamiliar code patterns line by line
- Commit in batches rather than after every file
- Don't mark tasks complete until confirmed working
- Always run from repo root for Docker and Go commands
- Mac Apple Silicon — always use `--platform linux/amd64 --load` for Docker builds targeting ECS
