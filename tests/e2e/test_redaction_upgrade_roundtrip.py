"""F03 e2e regression: REDACTION_REQUIRED re-read must be a legal continuation.

The scenario uses the real product entry points: a real KW service over a Unix
socket, the real ``DeliveryClient``, the real durable outbox and the real
redaction-recovery routine. Only the native reader/adapter is a synthetic
in-memory source (the host side is the fault injector), and every secret is a
synthetic canary.

Old behaviour: KW stored the refused payload fingerprint, so the re-read body
was an EVENT_CONFLICT; the outbox row failed while the recovery report still
claimed ``recovered=1``.
"""
from __future__ import annotations

import copy
import os
import time
from pathlib import Path
from types import SimpleNamespace

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events
from dma.outbox import Outbox
from dma.redaction import Redactor
from dma.redaction_recovery import reread_after_redaction

from knowledge_weaver import contracts
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, register_client

WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-recovery"
CLIENT_ID = "client:hermes-recovery"
EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}
CANARY = "T20_RECOVERY_CANARY_81"
SOURCE = {
    "instance_id": INSTANCE,
    "host": "hermes",
    "actor_id": "jun-test",
    "session_id": "session:recovery",
    "native_event_id": "message:secret",
    "native_schema": "hermes-snapshot-20261030",
    "role": "user",
    "origin_kind": "user_input",
    "locator": "hermes://session:recovery/message:secret",
}


def _redacted_events(text: str) -> list[dict]:
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=SOURCE,
        event_time=EVENT_TIME,
        text=text,
        content_revision="native:1",
        project_ids=("project:songta",),
    )


def _unsanitized_event(text: str) -> dict:
    """Simulate a legacy/older-policy outbox row whose adapter missed the secret.

    The current DMA capture path already rejects such a body, so the fault is
    injected at the durable-outbox seam (a row written by an older build or a
    different policy version). Everything downstream is the real product.
    """
    event = copy.deepcopy(_redacted_events("placeholder")[0])
    event["body"]["text"] = text
    event["body"]["sha256"] = contracts.body_digest(text)
    return event


def _seed_raw_outbox_row(outbox: Outbox, event: dict) -> None:
    import hashlib
    import json

    payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
    with outbox.read() as conn:
        conn.execute(
            "INSERT INTO outbox (event_id, revision, part_index, payload_json, payload_sha256, "
            "state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
            (
                event["event_id"],
                int(event["event_revision"]),
                int(event["body"]["part_index"]),
                payload,
                hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                "2026-10-30T00:00:00Z",
                "2026-10-30T00:00:00Z",
            ),
        )


class _Reader:
    instance_id = INSTANCE
    profile = "test"

    def __init__(self) -> None:
        self.records = [
            SimpleNamespace(native_event_id=SOURCE["native_event_id"]),
        ]

    def read_since(self, stream, position):
        return [(record, None) for record in self.records], None


class _Adapter:
    def __init__(self, *, content_revision="native:1") -> None:
        self.content_revision = content_revision

    def to_events(self, record):
        events = build_message_events(
            redactor=Redactor(),
            workspace_id=WORKSPACE,
            source=SOURCE,
            event_time=EVENT_TIME,
            text=f"密码: {CANARY}",
            content_revision=self.content_revision,
            project_ids=("project:songta",),
        )
        return events


def _socket(name: str) -> Path:
    return Path(os.environ.get("E2E_SOCKET_DIR", "/tmp")) / (
        f"umkw-recov-{name}-{os.getpid()}-{int(time.time() * 1000) % 100000}.sock"
    )


def _setup(tmp_path):
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
    app = ServiceApp(store, _socket("main")).start()
    outbox = Outbox(
        tmp_path / "dma" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=INSTANCE,
        backoff_delays=(0, 0, 0),
    )
    return store, app, outbox


def test_reread_after_redaction_is_accepted_by_real_kw_inbox(tmp_path):
    store, app, outbox = _setup(tmp_path)
    raw = _unsanitized_event(f"密码: {CANARY}")
    _seed_raw_outbox_row(outbox, raw)
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0)
    try:
        first = deliver_once(outbox, client)
        assert first["redaction_required"] == 1
        assert outbox.stats()["by_state"]["redaction_required"] == 1
        assert CANARY.encode() not in outbox.db_path.read_bytes()

        recovery = reread_after_redaction(
            outbox, [(_Reader(), _Adapter())], client=client, deliver=True
        )
        assert recovery["ok"] is True, recovery
        assert recovery["recovered"] == 1, recovery
        assert recovery["failed"] == []
        assert recovery["delivery"]["accepted"] == 1
        assert outbox.stats()["by_state"]["delivered"] == 1
        assert outbox.stats()["by_state"].get("failed", 0) == 0

        reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
        status, source = reader.get_source(raw["event_id"], 1)
        assert status == 200
        assert source["availability"] == "full"
        assert CANARY not in (source["text"] or "")
        assert "[REDACTED:PASSWORD_ASSIGNMENT]" in source["text"]
        # The upgraded event is processed semantically: its job exists.
        with store.read() as conn:
            jobs = conn.execute(
                "SELECT kind, input_key, state FROM jobs WHERE kind = 'extract'"
            ).fetchall()
        assert [(row["input_key"], row["state"]) for row in jobs] == [
            (f"{raw['event_id']}@1", "pending")
        ]
        # Re-delivery of the same sanitized payload is an idempotent duplicate.
        again = deliver_once(outbox, client)
        assert again["batches"] == 0
        duplicate = service_replay(app, raw)
        assert duplicate["disposition"] == "duplicate"
        assert CANARY not in store.db_path.read_bytes().decode("utf-8", "ignore")
    finally:
        app.stop()


def service_replay(app, event):
    """Re-send the exact sanitized event that recovery delivered (real inbox)."""
    from knowledge_weaver.store import receive_batch

    events = _redacted_events(f"密码: {CANARY}")
    result = receive_batch(app.store, {"schema_version": "1.0", "batch_id": "batch:replay", "events": events})
    return result["receipts"][0]


def test_reread_with_changed_content_revision_stays_conflicted(tmp_path):
    """A "re-read" that is a different logical event must not be smuggled in."""
    store, app, outbox = _setup(tmp_path)
    raw = _unsanitized_event(f"密码: {CANARY}")
    _seed_raw_outbox_row(outbox, raw)
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=3.0)
    try:
        assert deliver_once(outbox, client)["redaction_required"] == 1
        recovery = reread_after_redaction(
            outbox,
            [(_Reader(), _Adapter(content_revision="native:2"))],
            client=client,
            deliver=True,
        )
        assert recovery["ok"] is False, recovery
        assert recovery["recovered"] == 0
        assert recovery["failed"] and recovery["failed"][0]["state"] == "failed"
        assert outbox.stats()["by_state"]["failed"] == 1
        with store.read() as conn:
            parts = conn.execute(
                "SELECT body_text FROM event_parts WHERE event_id = ?", (raw["event_id"],)
            ).fetchall()
        assert [row["body_text"] for row in parts] == [None]
    finally:
        app.stop()
