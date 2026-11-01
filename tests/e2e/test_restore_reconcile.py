"""AC-46 e2e: KW restore from an older snapshot -> DMA epoch reconciliation and replay."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events
from dma.outbox import Outbox
from dma.reconcile import reconcile_after_restore
from dma.redaction import Redactor

from knowledge_weaver.portability import export_snapshot, import_snapshot
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, register_client

WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-reconcile"
CLIENT = "client:hermes-reconcile"
EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}


def _source(native_id: str) -> dict:
    return {
        "instance_id": INSTANCE,
        "host": "hermes",
        "actor_id": "jun-test",
        "session_id": "session:reconcile",
        "native_event_id": native_id,
        "native_schema": "hermes-snapshot-20261030",
        "role": "user",
        "origin_kind": "user_input",
        "locator": f"hermes://session:reconcile/{native_id}",
    }


def _events(native_id: str, text: str) -> list[dict]:
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=_source(native_id),
        event_time=EVENT_TIME,
        text=text,
        content_revision="native:1",
        project_ids=("project:songta",),
    )


def _new_store(path: Path) -> Store:
    store = Store(path)
    register_client(
        store,
        client_id=CLIENT,
        workspace_id=WORKSPACE,
        host="hermes",
        instance_id=INSTANCE,
        actor_ids=["*"],
        project_ids=["project:songta"],
    )
    return store


def _socket(name: str) -> Path:
    return Path("/tmp") / f"umkw-{name}-{time.time_ns() % 10**9}.sock"


def test_restore_epoch_change_requeues_and_replays_missing_receipts(tmp_path):
    store1 = _new_store(tmp_path / "kw1" / "knowledge.db")
    app1 = ServiceApp(store1, _socket("reconcile1")).start()
    outbox = Outbox(
        tmp_path / "dma" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=INSTANCE,
    )
    first = _events("message:1", "松塔项目端口为4100")
    second = _events("message:2", "松塔项目端口改为4200")
    try:
        outbox.capture(first, stream="hermes:main", position="1")
        deliver_once(outbox, DeliveryClient(app1.socket_path, client_id=CLIENT, timeout=3.0))
        watermark_at_snapshot = outbox.remote_meta()["watermark"]
        assert watermark_at_snapshot >= 1
        snapshot = tmp_path / "snapshot-old"
        export_snapshot(store1, snapshot)
    finally:
        app1.stop()

    # Continue delivering after the snapshot: these receipts exist only in the
    # live timeline that the restore loses.
    app1b = ServiceApp(store1, _socket("reconcile1b")).start()
    try:
        outbox.capture(second, stream="hermes:main", position="2")
        delivered = deliver_once(outbox, DeliveryClient(app1b.socket_path, client_id=CLIENT, timeout=3.0))
        assert delivered["accepted"] == 1
        assert outbox.remote_meta()["watermark"] > watermark_at_snapshot
    finally:
        app1b.stop()

    # Restore the older snapshot into a fresh backend with a new restore epoch.
    report = import_snapshot(snapshot, tmp_path / "kw-restored")
    restored = Store(report["target_db"])
    assert restored.watermark() == watermark_at_snapshot
    assert restored.meta()["restore_epoch"] == report["restore_epoch"]
    app2 = ServiceApp(restored, _socket("reconcile2")).start()
    try:
        client = DeliveryClient(app2.socket_path, client_id=CLIENT, timeout=3.0)
        reconcile = reconcile_after_restore(outbox, client)
        assert reconcile["changed"] is True
        assert reconcile["current"]["restore_epoch"] == report["restore_epoch"]
        assert reconcile["current"]["watermark"] == watermark_at_snapshot
        assert reconcile["requeued"] == 1
        assert reconcile["gaps"] == []
        assert reconcile["delivery"]["accepted"] == 1
        # The event lost by the snapshot is present again in the restored backend.
        reader = ServiceClient(app2.socket_path, client_id=CLIENT)
        status, source = reader.get_source(second[0]["event_id"], 1)
        assert status == 200
        assert source["text"] == "松塔项目端口改为4200"
        with restored.read() as conn:
            count = conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        assert count == 2
        # Idempotent: a second reconcile sees no epoch change.
        again = reconcile_after_restore(outbox, client)
        assert again["changed"] is False
        assert again["requeued"] == 0
        assert reconcile["limitations"]
    finally:
        app2.stop()
