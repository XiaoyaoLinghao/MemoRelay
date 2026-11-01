"""M1 end-to-end: DMA read-only capture -> redaction -> durable outbox -> KW UDS
service -> durable receipt -> source read -> retract.

Runs entirely against isolated tmp dirs and a Unix socket; no TCP listener, no
production path, no model call, synthetic data only.
"""
from __future__ import annotations

import os
import socket
import time
from pathlib import Path

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events, build_retract_events
from dma.outbox import Outbox
from dma.reader import JsonlReader
from dma.redaction import Redactor
import json

from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, register_client, receive_batch


WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-test"
CLIENT_ID = "client:hermes-main"
EVENT_TIME = {"value": "2026-10-30T01:00:00+00:00", "precision": "instant", "timezone": "UTC"}
SOURCE = {
    "instance_id": INSTANCE,
    "host": "hermes",
    "actor_id": "jun-test",
    "session_id": "session:test",
    "native_event_id": "message:1",
    "native_schema": "hermes-snapshot-20261030",
    "role": "user",
    "origin_kind": "user_input",
    "locator": "hermes://session:test/message:1",
}


@pytest.fixture()
def kw(tmp_path):
    store = Store(tmp_path / "kw" / "knowledge.db")
    register_client(
        store,
        client_id=CLIENT_ID,
        workspace_id=WORKSPACE,
        host="hermes",
        instance_id=INSTANCE,
        actor_ids=["jun-test"],
        project_ids=["project:songta"],
    )
    socket_dir = Path(os.environ.get("E2E_SOCKET_DIR", "/tmp"))
    socket_path = socket_dir / f"umkw-e2e-{os.getpid()}-{int(time.time()*1000) % 100000}.sock"
    app = ServiceApp(store, socket_path).start()
    try:
        yield store, app
    finally:
        app.stop()


@pytest.fixture()
def outbox(tmp_path):
    return Outbox(
        tmp_path / "dma" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=INSTANCE,
        max_attempts=3,
        backoff_delays=(0, 0, 0),
    )


def _events(text, *, native_id="message:1", source=None, **kwargs):
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=dict(source or SOURCE, native_event_id=native_id),
        event_time=EVENT_TIME,
        text=text,
        content_revision="native:1",
        project_ids=("project:songta",),
        **kwargs,
    )


def test_roundtrip_redacted_evidence_reaches_kw(kw, outbox):
    store, app = kw
    received = _events("松塔项目端口改为4200。")
    outbox.capture(received, stream="hermes:main", position="1")
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0)
    result = deliver_once(outbox, client)
    assert result["accepted"] == 1
    assert outbox.stats()["by_state"]["delivered"] == 1
    assert outbox.remote_meta()["backend_instance_id"] == store.meta()["backend_instance_id"]
    assert outbox.remote_meta()["watermark"] == store.watermark()

    reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
    status, source = reader.get_source(received[0]["event_id"], 1)
    assert status == 200
    assert source["text"] == "松塔项目端口改为4200。"
    assert source["availability"] == "full"


def test_secret_is_redacted_before_outbox_and_never_reaches_kw(kw, outbox):
    store, app = kw
    received = _events("数据库密码: CANARY-PW-7f3a，端口改为4200。")
    outbox.capture(received, stream="hermes:main", position="1")
    assert b"CANARY-PW-7f3a" not in outbox.db_path.read_bytes()
    delivered = deliver_once(outbox, DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0))
    assert delivered["accepted"] == 1
    assert b"CANARY-PW-7f3a" not in store.db_path.read_bytes()
    reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
    status, source = reader.get_source(received[0]["event_id"], 1)
    assert status == 200
    assert "CANARY-PW-7f3a" not in (source["text"] or "")
    assert "[REDACTED:PASSWORD_ASSIGNMENT]" in source["text"]


def test_withheld_private_key_is_receipt_only(kw, outbox):
    store, app = kw
    raw = "-----BEGIN PRIVATE KEY-----\nCANARY-PRIVATE\n-----END PRIVATE KEY-----"
    received = _events(raw)
    outbox.capture(received, stream="hermes:main", position="1")
    assert b"CANARY-PRIVATE" not in outbox.db_path.read_bytes()
    deliver_once(outbox, DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0))
    assert b"CANARY-PRIVATE" not in store.db_path.read_bytes()
    reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
    status, source = reader.get_source(received[0]["event_id"], 1)
    assert status == 200
    assert source["availability"] == "withheld"
    assert source["text"] is None


def test_retract_hides_prior_evidence(kw, outbox):
    store, app = kw
    created = _events("端口4200")
    outbox.capture(created, stream="hermes:main", position="1")
    deliver_once(outbox, DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0))
    retract = build_retract_events(
        workspace_id=WORKSPACE,
        source=SOURCE,
        event_time=EVENT_TIME,
        parent_revision=1,
        content_revision="native:1",
        project_ids=("project:songta",),
    )
    outbox.capture(retract, stream="hermes:main", position="2")
    delivered = deliver_once(outbox, DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0))
    assert delivered["accepted"] == 1
    reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
    status, source = reader.get_source(created[0]["event_id"], 1)
    assert status == 200
    assert source["availability"] == "retracted"
    assert source["text"] is None


def test_lost_response_then_replay_is_idempotent(kw, outbox):
    store, app = kw
    received = _events("并发记录")
    outbox.capture(received, stream="hermes:main", position="1")
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0)
    # Deliver once, then replay the exact same outbox batch through a fresh
    # outbox-less path: KW must answer duplicate and not store twice.
    deliver_once(outbox, client)
    batch = {
        "schema_version": "1.0",
        "batch_id": "batch:replay:1",
        "events": received,
    }
    status, result = ServiceClient(app.socket_path, client_id=CLIENT_ID).post_batch(batch)
    assert status == 200
    assert result["receipts"][0]["disposition"] == "duplicate"
    with store.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM event_parts").fetchone()[0] == 1


def test_kw_restart_keeps_durable_evidence(tmp_path):
    db = tmp_path / "kw" / "knowledge.db"
    store = Store(db)
    register_client(
        store,
        client_id=CLIENT_ID,
        workspace_id=WORKSPACE,
        host="hermes",
        instance_id=INSTANCE,
        actor_ids=["jun-test"],
        project_ids=[],
    )
    socket_path = Path("/tmp") / f"umkw-restart-{os.getpid()}.sock"
    event = _events("重启前记录")[0]
    receive_batch(store, {"schema_version": "1.0", "batch_id": "batch:1", "events": [event]})
    watermark_before = store.watermark()
    app = ServiceApp(store, socket_path).start()
    try:
        status, health = ServiceClient(socket_path, client_id=CLIENT_ID).health()
        assert status == 200 and health["watermark"] == watermark_before
        status, source = ServiceClient(socket_path, client_id=CLIENT_ID).get_source(event["event_id"], 1)
        assert status == 200 and source["text"] == "重启前记录"
    finally:
        app.stop()
    # New service instance over the same database: evidence survives.
    store2 = Store(db)
    app2 = ServiceApp(store2, socket_path).start()
    try:
        status, source = ServiceClient(socket_path, client_id=CLIENT_ID).get_source(event["event_id"], 1)
        assert status == 200 and source["text"] == "重启前记录"
    finally:
        app2.stop()


def test_unregistered_client_is_not_acknowledged(kw, outbox):
    store, app = kw
    received = _events("不应被接收")
    outbox.capture(received, stream="hermes:main", position="1")
    client = DeliveryClient(app.socket_path, client_id="client:unknown", timeout=3.0)
    result = deliver_once(outbox, client)
    assert result["failed"] == 1
    assert outbox.stats()["by_state"]["failed"] == 1
    with store.read() as conn:
        assert conn.execute("SELECT COUNT(*) FROM event_parts").fetchone()[0] == 0
