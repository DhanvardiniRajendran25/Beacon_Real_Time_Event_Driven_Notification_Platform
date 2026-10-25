import json
import time
import threading
from itertools import count

import gevent
import websocket
import requests

from locust import User, task, constant

# ── swap this to the ingestion service's IP or DNS ──────────────────────────
INGESTION_IP = "44.199.215.247"
# ────────────────────────────────────────────────────────────────────────────

GATEWAY_WS    = "ws://notif-system-alb-1110831797.us-east-1.elb.amazonaws.com:8080"
INGESTION_URL = f"http://{INGESTION_IP}:3000/event"

RUN_ID = int(time.time())

_uid_counter = count(1)
_uid_lock    = threading.Lock()


class WSNotifUser(User):
    wait_time = constant(5)  # fire one probe every 5 s (plus HTTP round-trip)

    def on_start(self):
        with _uid_lock:
            self.user_num = next(_uid_counter)
        self.user_id = f"testuser_{RUN_ID}_{self.user_num}"
        self.ws = websocket.create_connection(
            f"{GATEWAY_WS}/ws?user_id={self.user_id}"
        )
        self._listener = gevent.spawn(self._listen)

    def on_stop(self):
        self.ws.close()
        self._listener.kill()

    # ── background greenlet: receive WS messages for the lifetime of the user ──

    def _listen(self):
        while True:
            try:
                raw = self.ws.recv()
            except Exception:
                break
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            # skip history envelopes sent on connect
            if data.get("type") == "history":
                continue
            self._check_probe(data.get("message", ""))

    def _check_probe(self, message):
        if not isinstance(message, str) or not message.startswith("latency_probe:"):
            return
        try:
            sent_at = int(message.split(":", 1)[1])
        except (ValueError, IndexError):
            return
        latency_ms = int(time.time() * 1000) - sent_at
        self.environment.events.request.fire(
            request_type="WS",
            name="latency_probe",
            response_time=latency_ms,
            response_length=0,
            exception=None,
            context={},
        )

    # ── task: fire a probe POST every 5 s ────────────────────────────────────

    @task
    def send_probe(self):
        ts = int(time.time() * 1000)
        payload = {
            "type": "POST",
            "from_user": "alice",
            "detail": f"latency_probe:{ts}",
        }
        start = time.time()
        try:
            resp = requests.post(INGESTION_URL, json=payload, timeout=5)
            elapsed = int((time.time() - start) * 1000)
            self.environment.events.request.fire(
                request_type="POST",
                name="/event (probe)",
                response_time=elapsed,
                response_length=len(resp.content),
                exception=None if resp.ok else Exception(f"HTTP {resp.status_code}"),
                context={},
            )
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            self.environment.events.request.fire(
                request_type="POST",
                name="/event (probe)",
                response_time=elapsed,
                response_length=0,
                exception=e,
                context={},
            )
