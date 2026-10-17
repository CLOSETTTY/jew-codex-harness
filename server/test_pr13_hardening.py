"""Offline regressions for the PR #13 follow-up; no live model calls."""

from contextlib import ExitStack
import copy
import io
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest import mock

import jev_server as jev
from provider_ladder import Ladder
from request_budget import AttemptClock, ClientDisconnected, ExhaustedRequests, RequestBudget
from validated_stream import ValidatedStream


def handler():
    value = object.__new__(jev.Handler)
    value.wfile = io.BytesIO()
    value._response_started = False
    value.statuses = []
    value.send_response = value.statuses.append
    value.send_header = lambda *_: None
    value.end_headers = lambda: None
    return value


def frame(kind, response):
    return ("data: " + json.dumps({"type": kind, "response": response}) + "\n\n").encode()


class RelayHardeningTests(unittest.TestCase):
    def test_budget_default_remains_180_seconds_and_can_be_overridden(self):
        self.assertEqual(RequestBudget().limits["total_seconds"], 180)
        self.assertEqual(RequestBudget({"total_seconds": 600}).limits["total_seconds"], 600)

    def test_nonladder_last_attempt_blocks_failed_replay_but_not_success(self):
        for success in (False, True):
            with self.subTest(success=success), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                for name in ("OFF_PATH", "SHADOW_PATH", "DEBUG_PATH", "DRY_STATE_PATH", "SIGNATURE_PATH"):
                    stack.enter_context(mock.patch.object(jev, name, str(Path(directory) / name)))
                stack.enter_context(mock.patch.object(jev, "STATE", directory))
                stack.enter_context(mock.patch.dict(os.environ, {"JEV_LADDER_MODE": "off"}))
                for name, value in (("load_key", None), ("native_dry", None),
                                    ("route_lease", None), ("log_line", None),
                                    ("remember_cache_model", None), ("remember_route_lease", None)):
                    stack.enter_context(mock.patch.object(jev, name, return_value=value))
                guard = ExhaustedRequests()
                stack.enter_context(mock.patch.object(jev, "_exhausted_requests", guard))
                stack.enter_context(mock.patch.object(jev, "RequestBudget", side_effect=lambda limits=None, started=None:
                                   RequestBudget({"max_attempts": 1}, started=started)))
                payload = {"model": "jev/auto", "input": "synthetic-nonladder", "stream": False}
                h = handler()
                h.path = "/v1/responses"
                h._body = lambda *_: copy.deepcopy(payload)
                def forward(*args, **kwargs):
                    h._budget.claim()
                    h._attempts.append({"model": args[5], "status": 200 if success else 503,
                                        "terminal_type": "response.completed" if success else None})
                    if success:
                        h.send_response(200)
                    return (200 if success else 503), "json", "application/json", False, (None if success else b'{}'), None
                h._forward = forward
                h._post()
                self.assertEqual(h.statuses, [200 if success else 400])
                if not success:
                    self.assertIn(b'request_budget_exhausted', h.wfile.getvalue())
                retry = handler()
                retry._budget = RequestBudget({"max_attempts": 1})
                self.assertEqual(retry._admit_replay(payload), success)

    def test_completed_metadata_uses_the_logical_envelope_not_the_fallback(self):
        h = handler()
        relay = ValidatedStream(h, "jev/auto")
        relay.finish(frame("response.created", {"id": "provider", "model": "fallback/model"}) +
                     frame("response.completed", {"id": "provider", "model": "resolved/model",
                           "created_at": 1, "status": "completed", "output": [],
                           "usage": {"input_tokens": 123}}))
        snapshots = [json.loads(line[5:])["response"] for line in
                     h.wfile.getvalue().splitlines() if line.startswith(b"data: {")]
        first, final = snapshots[0], snapshots[-1]
        for field in ("id", "object", "model", "created_at"):
            self.assertEqual(first[field], final[field], field)
        self.assertEqual(final["model"], "jev/auto")
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["usage"], {"input_tokens": 123})

    def test_exact_last_failed_attempt_is_terminal_and_cannot_be_replayed(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming), tempfile.TemporaryDirectory() as directory:
                h = handler()
                ladder = Ladder({"providers": [{"id": "only", "model": "gateway/model"}],
                                 "request_budget": {"max_attempts": 1}}, Path(directory) / "state.json")
                h._budget = RequestBudget(ladder.config["request_budget"])
                payload = {"model": "jev/auto", "input": "synthetic", "stream": streaming}
                guard = ExhaustedRequests()
                calls = []
                def forward(*args, **kwargs):
                    calls.append(1)
                    h._budget.claim()
                    h._attempts.append({"model": "gateway/model", "status": 503})
                    if streaming:
                        h._validated_stream = ValidatedStream(h, "jev/auto")
                    return 503, "json", "application/json", False, b'{"error":{}}', None
                h._forward = forward
                with mock.patch.object(jev, "_provider_ladder", return_value=ladder), \
                     mock.patch.object(jev, "_omniroute_key", return_value="synthetic"), \
                     mock.patch.object(jev, "_exhausted_requests", guard), \
                     mock.patch.object(jev, "log_line"):
                    self.assertTrue(h._admit_replay(payload))
                    h._serve_ladder(payload, "scope", jev.SOL, "high", None,
                                    {"step_type": "user_turn"}, None, streaming, False)
                    self.assertEqual(calls, [1])
                    self.assertIn(b'request_budget_exhausted', h.wfile.getvalue())
                    self.assertEqual(h.statuses, [200 if streaming else 400])
                    retry = handler()
                    retry._budget = RequestBudget(ladder.config["request_budget"])
                    self.assertFalse(retry._admit_replay(
                        {"model": "jev/auto", "input": "synthetic", "stream": streaming}))

    def test_history_fallback_does_not_revisit_failed_native_destination(self):
        for second_native in (False, True):
            with self.subTest(second_native=second_native), tempfile.TemporaryDirectory() as directory:
                providers = [{"id": "native", "transport": "native", "model": jev.SOL},
                             {"id": "incompatible", "model": "gateway/no-history"}]
                if second_native:
                    providers.append({"id": "native-backup", "transport": "native", "model": jev.LUNA})
                ladder = Ladder({"providers": providers}, Path(directory) / "state.json")
                h = handler()
                calls = []
                def forward(payload, *args, **kwargs):
                    calls.append(payload["model"])
                    h._budget.claim()
                    h._attempts.append({"model": payload["model"], "status": 503})
                    return 503, "json", "application/json", False, b'{"error":{}}', None
                h._forward = forward
                with mock.patch.object(jev, "_provider_ladder", return_value=ladder), \
                     mock.patch.object(jev, "_omniroute_key", return_value="synthetic"), \
                     mock.patch.object(jev, "log_line"):
                    h._serve_ladder({"model": "jev/auto", "input": [
                        {"type": "tool_search_output", "execution": "client", "call_id": "fixture", "tools": []}]},
                        "scope", jev.SOL, "high", None, {"step_type": "user_turn"}, None, False, False)
                self.assertEqual(calls, [jev.SOL, jev.LUNA] if second_native else [jev.SOL])

    def test_blocked_heartbeat_cannot_hold_the_deadline_watchdog_lock(self):
        entered, release, expired = threading.Event(), threading.Event(), threading.Event()
        def heartbeat():
            entered.set()
            release.wait(2)
        client, peer = socket.socketpair()
        first = second = None
        clock = None
        try:
            with mock.patch("request_budget.time.monotonic", return_value=0) as now, \
                 mock.patch.object(AttemptClock, "_arm"):
                clock = AttemptClock(RequestBudget({"total_seconds": 1}), client, heartbeat)
                upstream = mock.Mock()
                clock.connected(upstream)
                first = threading.Thread(target=clock._interrupt, daemon=True)
                first.start()
                self.assertTrue(entered.wait(1))
                now.return_value = 1
                def interrupt():
                    clock._interrupt()
                    expired.set()
                second = threading.Thread(target=interrupt, daemon=True)
                second.start()
                self.assertTrue(expired.wait(0.5), "heartbeat blocked deadline enforcement")
                upstream.shutdown.assert_called_once_with(socket.SHUT_RDWR)
                self.assertEqual(clock.expired, "total")
        finally:
            release.set()
            for thread in (first, second):
                if thread:
                    thread.join(2)
            if clock:
                clock.close()
            client.close()
            peer.close()

    def test_slow_downstream_write_is_bounded_by_the_request_deadline(self):
        sender, reader = socket.socketpair()
        try:
            sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            h = handler()
            h.connection = sender
            h.wfile = sender.makefile("wb", buffering=4096)
            h._budget = RequestBudget({"total_seconds": 0.15})
            relay = ValidatedStream(h, "jev/auto")
            started = time.monotonic()
            with self.assertRaises(ClientDisconnected):
                # Windows loopback may absorb megabytes despite SO_SNDBUF.
                # Keep writing bounded chunks until the unread peer blocks.
                chunk = b"x" * (1024 * 1024)
                for _ in range(256):
                    relay.write(chunk)
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertTrue(relay.finished)
            h.wfile.close()
        finally:
            sender.close()
            reader.close()
