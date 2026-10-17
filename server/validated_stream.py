"""Keep a validated, buffered Responses stream alive without exposing actions."""

import json
import time
import threading
import uuid

try:
    from .request_budget import ClientDisconnected
except ImportError:  # direct server entrypoint
    from request_budget import ClientDisconnected


class ValidatedStream:
    def __init__(self, handler, model):
        self.handler = handler
        self.lock = threading.RLock()
        self.response_id = "resp_jev_" + uuid.uuid4().hex
        self.sequence = 0
        self.finished = False
        self.failure_deadline = None
        self.last_write = time.monotonic()
        self.response = {"id": self.response_id, "object": "response",
                         "created_at": int(time.time()), "model": model,
                         "status": "in_progress", "output": []}
        self._bound_write()
        handler.send_response(200)
        handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
        handler.send_header("Connection", "close")
        handler.end_headers()
        self.event({"type": "response.created", "response": self.response})

    def event(self, value):
        with self.lock:
            value = dict(value)
            response = value.get("response")
            if isinstance(response, dict):
                value["response"] = {**response, **{
                    key: self.response[key] for key in ("id", "object", "model", "created_at")}}
            if "response_id" in value:
                value["response_id"] = self.response_id
            value["sequence_number"] = self.sequence
            self.sequence += 1
            self.write(("event: " + value["type"] + "\ndata: " +
                        json.dumps(value, ensure_ascii=False) + "\n\n").encode())

    def _bound_write(self):
        budget = getattr(self.handler, "_budget", None)
        connection = getattr(self.handler, "connection", None)
        if budget is not None and connection is not None:
            remaining = (max(0, self.failure_deadline - time.monotonic())
                         if self.failure_deadline is not None else budget.remaining())
            if not remaining:
                raise ClientDisconnected("downstream write deadline exhausted")
            connection.settimeout(min(remaining, budget.limits["idle_seconds"]))

    def write(self, data):
        with self.lock:
            try:
                self._bound_write()
                self.handler.wfile.write(data)
                self.handler.wfile.flush()
            except OSError:
                # Any relay write may already have exposed content or tool actions.
                # Downstream failure must never be returned as a retryable upstream outage.
                self.finished = True
                raise ClientDisconnected("downstream client disconnected") from None
            self.last_write = time.monotonic()

    def heartbeat(self):
        with self.lock:
            if not self.finished and time.monotonic() - self.last_write >= 10:
                self.event({"type": "response.in_progress", "response": self.response})

    def finish(self, data):
        with self.lock:
            # Only fully validated/normalized output reaches this method. The
            # lifecycle envelope is ours, including across safe provider fallback.
            for block in data.decode("utf-8").replace("\r\n", "\n").split("\n\n"):
                lines = [line[5:].lstrip() for line in block.splitlines() if line.startswith("data:")]
                if not lines:
                    continue
                raw = "\n".join(lines)
                if raw == "[DONE]":
                    continue
                value = json.loads(raw)
                if value.get("type") != "response.created":
                    self.event(value)
            self.write(b"data: [DONE]\n\n")
            self.finished = True

    def fail(self, message, code="upstream_response_failed"):
        with self.lock:
            if self.finished:
                return
            # Only the terminal error may drain after generation's budget is
            # spent. Never leave a slow reader blocking cleanup indefinitely.
            self.failure_deadline = time.monotonic() + 0.25
            self.event({"type": "response.failed", "response": {
                **self.response, "status": "failed",
                "error": {"code": code, "message": message}}})
            self.write(b"data: [DONE]\n\n")
            self.finished = True
