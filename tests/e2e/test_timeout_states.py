"""AC-30 isolated: slow backend, unavailable backend, empty result, recovery.

A local Unix-socket stub injects slow/empty responses; the real KW service is
used for the recovery path. The Hermes provider must stay bounded, never invent
context, and expose an explicit error/empty state.
"""
from __future__ import annotations

import json
import shutil
import socketserver
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler
from pathlib import Path

import pytest

from dma.integrations.formatting import format_recall_context
from dma.integrations.hermes import DmaMemoryProvider
from dma.integrations.recall_client import RecallClient

from knowledge_weaver import contracts
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.store import Store, receive_batch, register_client
from knowledge_weaver.store.claims import record_claim_revision

WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-timeout"
CLIENT = "client:hermes-timeout"


class _StubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        delay = getattr(self.server, "delay", 0.0)
        payload = getattr(self.server, "payload", None)
        if delay:
            time.sleep(delay)
        body = json.dumps(payload or empty_result()).encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True


class StubRecallServer:
    def __init__(self, *, delay=0.0, payload=None):
        self.directory = Path(tempfile.mkdtemp(prefix="umstub-", dir="/tmp"))
        self.socket_path = self.directory / "kw.sock"
        self.server = socketserver.ThreadingMixIn
        handler = type("_BlockingHandler", (_StubHandler,), {})
        self.httpd = socketserver.ThreadingUnixStreamServer(str(self.socket_path), handler)
        self.httpd.delay = delay
        self.httpd.payload = payload
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc_info):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.directory, ignore_errors=True)


def empty_result() -> dict:
    return {
        "schema_version": "1.0",
        "request_id": "req:stub",
        "status": "empty",
        "scope_candidates": [],
        "watermark": 1,
        "restore_epoch": "epoch:1",
        "items": [],
        "next_cursor": None,
        "total_hits": 0,
        "incomplete_reasons": [],
        "used_tokens": 0,
        "truncated": False,
    }


def _provider(socket_path, *, timeout=0.5, deadline_ms=500):
    return DmaMemoryProvider(
        recall_client=RecallClient(str(socket_path), CLIENT, timeout=timeout),
        client_id=CLIENT,
        deadline_ms=deadline_ms,
    )


def test_slow_backend_is_bounded_and_returns_no_context():
    with StubRecallServer(delay=2.0) as stub:
        provider = _provider(stub.socket_path)
        provider.initialize("session:slow")
        started = time.monotonic()
        result = provider.prefetch("松塔项目端口")
        elapsed = time.monotonic() - started
    assert "历史暂不可用" in result
    assert "4200" not in result
    assert elapsed < 1.5  # deadline/timeout, not the full 2s backend delay
    assert provider.recall_status() is None
    assert provider.last_error_code == "STORE_UNAVAILABLE"


def test_empty_backend_is_empty_not_unavailable():
    with StubRecallServer(payload=empty_result()) as stub:
        provider = _provider(stub.socket_path)
        provider.initialize("session:empty")
        assert "未找到" in provider.prefetch("松塔项目端口")
        status = provider.recall_status()
        assert status is not None and status.count == 0
        assert provider.last_error_code is None
        text, dropped = format_recall_context(empty_result())
        assert text == "" and dropped == 0


def test_unavailable_backend_reports_error_and_recovers(tmp_path, monkeypatch):
    provider = _provider("/tmp/no-such-kw-socket.sock", timeout=0.3)
    provider.initialize("session:down")
    assert "历史暂不可用" in provider.prefetch("松塔项目端口")
    assert provider.last_error_code == "STORE_UNAVAILABLE"

    store = Store(tmp_path / "kw" / "knowledge.db")
    register_client(
        store,
        client_id=CLIENT,
        workspace_id=WORKSPACE,
        host="hermes",
        instance_id=INSTANCE,
        actor_ids=["*"],
        project_ids=["project:songta"],
    )
    event = _event("message:1", "松塔项目端口为4200")
    receive_batch(store, {"schema_version": "1.0", "batch_id": "batch:t", "events": [event]})
    record_claim_revision(
        store,
        workspace_id=WORKSPACE,
        project_id="project:songta",
        subject_id="subject:songta-server",
        predicate="server.port",
        facet="desired",
        qualifiers={},
        value=4200,
        display_text="松塔项目端口为4200",
        authority="user_confirmed",
        assertion_kind="explicit_statement",
        event_time=event["event_time"],
        evidence=[
            {
                "event_id": event["event_id"],
                "event_revision": 1,
                "part_index": 0,
                "start_byte": 0,
                "end_byte": len("松塔项目端口为4200".encode()),
                "body_sha256": event["body"]["sha256"],
                "availability": "full",
            }
        ],
    )
    socket_dir = Path(tempfile.mkdtemp(prefix="umkw-recover-", dir="/tmp"))
    app = ServiceApp(store, socket_dir / "kw.sock").start()
    try:
        recovered = _provider(app.socket_path, timeout=2.0, deadline_ms=2000)
        recovered.initialize("session:recovered")
        text = recovered.prefetch("松塔项目端口")
        assert "4200" in text
        assert recovered.last_error_code is None
        assert recovered.recall_status().count >= 1
    finally:
        app.stop()
        shutil.rmtree(socket_dir, ignore_errors=True)


def _event(native_id: str, text: str) -> dict:
    return {
        "schema_version": "1.0",
        "event_id": contracts.canonical_event_id(WORKSPACE, INSTANCE, native_id),
        "event_revision": 1,
        "workspace_id": WORKSPACE,
        "operation": "create",
        "source": {
            "instance_id": INSTANCE,
            "host": "hermes",
            "actor_id": "jun-test",
            "session_id": "session:timeout",
            "native_event_id": native_id,
            "native_schema": "hermes-snapshot-20261030",
            "role": "user",
            "origin_kind": "user_input",
            "locator": f"hermes://session:timeout/{native_id}",
        },
        "event_time": {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"},
        "body": {
            "text": text,
            "sha256": contracts.body_digest(text),
            "part_index": 0,
            "part_count": 1,
            "content_revision": "native:1",
            "format": "text/plain",
        },
        "redaction": {"policy_version": "redact:1", "status": "passed", "replacements": 0, "credential_refs": []},
        "project_ids": ["project:songta"],
        "parent_revision": None,
        "derived_from": [],
    }
