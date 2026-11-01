"""F15 e2e regression: restore detection must survive an earlier delivery.

Scenario: the adapter acknowledged events past the snapshot watermark, the KW
backend is restored from the older snapshot, and a *new* event is delivered
against the restored backend before reconciliation runs. The old code overwrote
the observed epoch during that delivery, so reconcile saw "no change" and the
lost acknowledgements were never replayed.

Everything is the real product path: real KW UDS service, real DeliveryClient,
real durable outbox, real snapshot export/import. Synthetic data only.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

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
INSTANCE = "instance:hermes-restore"
CLIENT = "client:hermes-restore"
EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}
SOURCE = {
    "instance_id": INSTANCE,
    "host": "hermes",
    "actor_id": "jun-test",
    "session_id": "session:restore",
    "native_event_id": "message:first",
    "native_schema": "hermes-snapshot-20261030",
    "role": "user",
    "origin_kind": "user_input",
    "locator": "hermes://session:restore/message:first",
}


def _events(native_id: str, text: str) -> list[dict]:
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source={**SOURCE, "native_event_id": native_id},
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
    return Path(os.environ.get("E2E_SOCKET_DIR", "/tmp")) / (
        f"umkw-restore-{name}-{os.getpid()}-{int(time.time() * 1000) % 100000}.sock"
    )


def test_restore_detection_survives_new_delivery_and_replays_lost_acks(tmp_path):
    live = _new_store(tmp_path / "kw-live" / "knowledge.db")
    app1 = ServiceApp(live, _socket("live")).start()
    outbox = Outbox(
        tmp_path / "dma" / "outbox.db", workspace_id=WORKSPACE, instance_id=INSTANCE
    )
    client1 = DeliveryClient(app1.socket_path, client_id=CLIENT, timeout=3.0)
    try:
        first = _events("message:first", "松塔项目端口为4100")
        outbox.capture(first, stream="hermes:main", position="1")
        assert deliver_once(outbox, client1)["accepted"] == 1
        snapshot_watermark = outbox.remote_meta()["watermark"]
        assert snapshot_watermark >= 1
        snapshot = tmp_path / "snapshot"
        export_snapshot(live, snapshot)

        # Acknowledged after the snapshot was taken: this receipt exists only in
        # the timeline the restore loses.
        lost = _events("message:lost", "松塔项目端口改为4200")
        outbox.capture(lost, stream="hermes:main", position="2")
        assert deliver_once(outbox, client1)["accepted"] == 1
        assert outbox.remote_meta()["watermark"] > snapshot_watermark
    finally:
        app1.stop()

    report = import_snapshot(snapshot, tmp_path / "kw-restored")
    restored = Store(report["target_db"])
    assert restored.watermark() == snapshot_watermark
    app2 = ServiceApp(restored, _socket("restored")).start()
    client2 = DeliveryClient(app2.socket_path, client_id=CLIENT, timeout=3.0)
    try:
        # A normal new delivery happens BEFORE reconciliation: it observes the
        # new restore epoch and must durably note the change instead of erasing it.
        new = _events("message:new", "恢复后新的记忆")
        outbox.capture(new, stream="hermes:main", position="3")
        assert deliver_once(outbox, client2)["accepted"] == 1
        pending = outbox.pending_restore_change()
        assert pending is not None, "delivery must persist the restore detection"
        assert pending["previous_restore_epoch"] != pending["observed_restore_epoch"]

        # Another ordinary write must not clear the pending marker.
        outbox.capture(
            _events("message:newer", "恢复后另一条记忆"), stream="hermes:main", position="4"
        )
        assert deliver_once(outbox, client2)["accepted"] == 1
        assert outbox.pending_restore_change() is not None

        result = reconcile_after_restore(outbox, client2)
        assert result["changed"] is True
        assert result["changed_source"] == "pending_restore_marker"
        assert result["gaps"] == []
        assert result["requeue_basis"] == "ack_epoch_mismatch"
        requeued_keys = {tuple(key) for key in result["requeued_keys"]}
        lost_key = (lost[0]["event_id"], 1, 0)
        assert lost_key in requeued_keys, {"requeued": requeued_keys, "lost": lost_key}
        # The lost acknowledgement is back in the restored backend.
        with restored.read() as conn:
            row = conn.execute(
                "SELECT body_text FROM event_parts WHERE event_id = ?", (lost[0]["event_id"],)
            ).fetchone()
        assert row is not None and row["body_text"] == "松塔项目端口改为4200"
        assert result["pending_restore_change_after"] is None

        again = reconcile_after_restore(outbox, client2)
        assert again["changed"] is False
        assert again["requeued"] == 0
        reader = ServiceClient(app2.socket_path, client_id=CLIENT)
        assert reader.get_source(lost[0]["event_id"], 1)[0] == 200
    finally:
        app2.stop()


def test_pending_restore_marker_is_durable_across_restart(tmp_path):
    """A crash after detection (before reconcile) must not lose the detection."""
    live = _new_store(tmp_path / "kw-live" / "knowledge.db")
    outbox_path = tmp_path / "dma" / "outbox.db"
    outbox = Outbox(outbox_path, workspace_id=WORKSPACE, instance_id=INSTANCE)
    outbox.set_remote_meta(
        backend_instance_id="kw:old", restore_epoch="epoch:old", watermark=7, source="test"
    )
    outbox.set_remote_meta(
        backend_instance_id="kw:new", restore_epoch="epoch:new", watermark=2, source="delivery"
    )
    pending = outbox.pending_restore_change()
    assert pending is not None
    assert (pending["previous_restore_epoch"], pending["observed_restore_epoch"]) == (
        "epoch:old",
        "epoch:new",
    )
    # Simulate a process restart by re-opening the durable outbox.
    reopened = Outbox(outbox_path, workspace_id=WORKSPACE, instance_id=INSTANCE)
    assert reopened.pending_restore_change()["id"] == pending["id"]
