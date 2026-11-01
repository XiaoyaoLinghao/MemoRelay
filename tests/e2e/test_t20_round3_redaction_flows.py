"""T20 round-3 E2E regression: the Codex-corrected F03 redaction flows and the
corrected expiry semantics, through the **real** host-side and service entry
points.

Migrated from ``artifacts/T20-round2/run-20261030-codex/test_F03_normalized.py``
and ``test_variants.py::test_F03_multipart_reread_real_uds`` with the two Codex
corrections applied:

* the refusal body is the *actual* DMA ``Redactor`` output
  ``[REDACTED:PASSWORD_ASSIGNMENT]`` (the old hand-written
  ``密码: [REDACTED:PASSWORD]`` is itself still a secret match and cannot be the
  expected sanitized text);
* a real KW service over a Unix socket, the real ``DeliveryClient`` and the real
  durable outbox, including a multi-part (cross-part) re-read, and the
  changed-``content_revision`` rejection;
* expiry-retains-conclusion: after the source detail expires (real sweep), the
  valid conclusion is still recallable through the real API, its vector is kept,
  and the source/detail is reported as expired instead of readable.

Only the native host reader/adapter is a synthetic in-memory source (the host is
the fault injector). All secrets are synthetic canaries.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events
from dma.outbox import Outbox
from dma.redaction import Redactor
from dma.redaction_recovery import reread_after_redaction

from knowledge_weaver import contracts
from knowledge_weaver.embeddings.index import HashEmbeddingProvider
from knowledge_weaver.lifecycle import sweep
from knowledge_weaver.lifecycle.retention import register_lifecycle
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, receive_batch, register_client
from knowledge_weaver.store.claims import record_claim_revision
from knowledge_weaver.retrieval.embeddings_index import collect_vectors

WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-f03"
CLIENT_ID = "client:hermes-f03"
EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}
CANARY = "T20_R3_RECOVERY_CANARY_6401"
PLACEHOLDER = "[REDACTED:PASSWORD_ASSIGNMENT]"
SOURCE = {
    "instance_id": INSTANCE,
    "host": "hermes",
    "actor_id": "jun-test",
    "session_id": "session:f03",
    "native_event_id": "message:f03-secret",
    "native_schema": "hermes-snapshot-20261030",
    "role": "user",
    "origin_kind": "user_input",
    "locator": "hermes://session:f03/message:f03-secret",
}
RECALL_BODY = {
    "schema_version": "1.0",
    "request_id": "req:e2e:f03:1",
    "session_id": "session:e2e",
    "turn_id": "turn:1",
    "query": "端口4200",
    "project_ids": ["project:songta"],
    "intent": "current",
    "budget_tokens": 2000,
    "limit": 8,
    "deadline_ms": 7000,
    "cursor": None,
}


@pytest.fixture
def socket_dir():
    # Unix-domain socket paths are limited; keep them in a short /tmp directory.
    path = Path(tempfile.mkdtemp(prefix="umf03-", dir="/tmp"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


def _real_events(text: str, *, content_revision="native:1") -> list[dict]:
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=SOURCE,
        event_time=EVENT_TIME,
        text=text,
        content_revision=content_revision,
        project_ids=("project:songta",),
    )


def _seed_raw_outbox_rows(outbox: Outbox, events: list[dict]) -> None:
    now = "2026-10-30T00:00:00Z"
    with outbox.read() as conn:
        for event in events:
            payload = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
            conn.execute(
                "INSERT INTO outbox (event_id, revision, part_index, payload_json, payload_sha256, "
                "state, attempts, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
                (
                    event["event_id"],
                    int(event["event_revision"]),
                    int(event["body"]["part_index"]),
                    payload,
                    hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                    now,
                    now,
                ),
            )


class _Reader:
    instance_id = INSTANCE
    profile = "e2e"

    def __init__(self, native_event_id=SOURCE["native_event_id"]):
        self._native_event_id = native_event_id

    def read_since(self, stream, position):
        return [(SimpleNamespace(native_event_id=self._native_event_id), None)], None


class _Adapter:
    def __init__(self, *, text: str, content_revision="native:1", events_factory=None):
        self.text = text
        self.content_revision = content_revision
        self.events_factory = events_factory

    def to_events(self, record):
        if self.events_factory is not None:
            return self.events_factory()
        return _real_events(self.text, content_revision=self.content_revision)


def _setup_kw(tmp_path, socket_dir, store=None):
    store = store or Store(tmp_path / "kw" / "knowledge.db")
    register_client(
        store,
        client_id=CLIENT_ID,
        workspace_id=WORKSPACE,
        host="hermes",
        instance_id=INSTANCE,
        actor_ids=["jun-test"],
        project_ids=["project:songta"],
    )
    app = ServiceApp(store, socket_dir / "kw.sock").start()
    return store, app


def _setup_outbox(tmp_path):
    return Outbox(
        tmp_path / "dma" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=INSTANCE,
        backoff_delays=(0, 0, 0),
    )


def _long_multipart_text(part_chars: int = 250_000) -> str:
    # Secret at the very start, then enough synthetic Chinese log content to
    # force a real multi-part split of the *already redacted* full message.
    tail = "合成长日志内容。" * ((part_chars // 8) + 2)
    return f"密码: {CANARY}\n{tail}"


def test_actual_redactor_output_is_the_corrected_f03_expectation():
    """Codex correction #1: the expected sanitized text is the real Redactor
    output, not the old hand-written placeholder that is itself a secret match."""
    clean = Redactor().redact(f"密码: {CANARY}").text
    assert clean == PLACEHOLDER, clean
    assert contracts.secret_kinds(clean) == [], clean
    # Why the old assertion was wrong: this placeholder still matches the rule.
    assert contracts.secret_kinds("密码: [REDACTED:PASSWORD]") == ["password_assignment"]


def test_multipart_reread_after_redaction_is_accepted_by_real_kw_uds(tmp_path, socket_dir):
    """F03 + AC-38 '脱敏先于分块': a long message is redacted first and split into
    parts; the refused part is recovered by a real cross-part re-read and the real
    KW UDS delivery, with no secret ever durable."""
    text = _long_multipart_text()
    real_events = _real_events(text)
    assert len(real_events) == 2, len(real_events)
    assert all(event["body"]["text"].find(CANARY) < 0 for event in real_events)
    assert PLACEHOLDER in real_events[0]["body"]["text"] or PLACEHOLDER in real_events[1]["body"]["text"]

    # A legacy/older-policy outbox row missed the redaction for part 0 only:
    # inject the raw canary at the exact redacted position (host fault injector).
    raw_events = copy.deepcopy(real_events)
    target = next(event for event in raw_events if PLACEHOLDER in event["body"]["text"])
    corrupted = target["body"]["text"].replace(PLACEHOLDER, f"密码: {CANARY}")
    # A legacy raw row is bounded by the same part contract; redaction can expand
    # the text, so trim the injected copy back to the part limit.
    corrupted = corrupted[: contracts.MAX_PART_CHARS]
    target["body"]["text"] = corrupted
    target["body"]["sha256"] = contracts.body_digest(corrupted)
    raw_part_index = int(target["body"]["part_index"])

    store, app = _setup_kw(tmp_path, socket_dir)
    outbox = _setup_outbox(tmp_path)
    _seed_raw_outbox_rows(outbox, raw_events)
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=5.0)
    try:
        first = deliver_once(outbox, client)
        assert first["redaction_required"] == 1, first
        assert first["accepted"] == 1, first
        assert outbox.stats()["by_state"]["redaction_required"] == 1
        assert CANARY.encode() not in outbox.db_path.read_bytes()

        def reread_factory():
            return _real_events(text)

        recovery = reread_after_redaction(
            outbox,
            [(_Reader(), _Adapter(text=text, events_factory=reread_factory))],
            client=client,
            deliver=True,
        )
        assert recovery["ok"] is True, recovery
        assert recovery["recovered"] == 1, recovery
        assert recovery["failed"] == [], recovery
        stats = outbox.stats()["by_state"]
        assert stats.get("delivered") == 2, stats
        assert stats.get("redaction_required", 0) == 0 and stats.get("failed", 0) == 0, stats

        reader = ServiceClient(app.socket_path, client_id=CLIENT_ID)
        status, first_part = reader.get_source(raw_events[0]["event_id"], 1, 0)
        assert status == 200, first_part
        status, second_part = reader.get_source(raw_events[0]["event_id"], 1, 1)
        assert status == 200, second_part
        assert first_part["availability"] == "full" and second_part["availability"] == "full"
        by_index = {first_part["part_index"]: first_part, second_part["part_index"]: second_part}
        recovered = by_index[raw_part_index]
        assert recovered["text"] == real_events[raw_part_index]["body"]["text"], recovered
        assert PLACEHOLDER in recovered["text"] and CANARY not in recovered["text"]
        for part in by_index.values():
            assert CANARY not in (part["text"] or "")

        # The group is complete and semantically queued: the recovery was a legal
        # continuation, not just a stored duplicate.
        with store.read() as conn:
            group = conn.execute(
                "SELECT group_state FROM event_revisions WHERE event_id = ?", (raw_events[0]["event_id"],)
            ).fetchone()
            jobs = conn.execute(
                "SELECT input_key, state FROM jobs WHERE kind = 'extract'"
            ).fetchall()
        assert group["group_state"] == "complete", dict(group)
        assert [(row["input_key"], row["state"]) for row in jobs] == [
            (f"{raw_events[0]['event_id']}@1", "pending")
        ], [dict(row) for row in jobs]
        assert CANARY.encode() not in store.db_path.read_bytes()
    finally:
        app.stop()


def test_reread_with_changed_content_revision_stays_rejected(tmp_path, socket_dir):
    """F03 correction #2: a re-read with a different content_revision is not the
    same logical event -> rejected, outbox failed, no body stored."""
    text = f"密码: {CANARY}"
    real_events = _real_events(text)
    raw = copy.deepcopy(real_events[0])
    corrupted = raw["body"]["text"].replace(PLACEHOLDER, f"密码: {CANARY}")
    raw["body"]["text"] = corrupted
    raw["body"]["sha256"] = contracts.body_digest(corrupted)

    store, app = _setup_kw(tmp_path, socket_dir)
    outbox = _setup_outbox(tmp_path)
    _seed_raw_outbox_rows(outbox, [raw])
    client = DeliveryClient(app.socket_path, client_id=CLIENT_ID, timeout=5.0)
    try:
        assert deliver_once(outbox, client)["redaction_required"] == 1
        recovery = reread_after_redaction(
            outbox,
            [(_Reader(), _Adapter(text=text, content_revision="native:2"))],
            client=client,
            deliver=True,
        )
        assert recovery["ok"] is False, recovery
        assert recovery["recovered"] == 0, recovery
        assert recovery["failed"] and recovery["failed"][0]["state"] == "failed", recovery
        assert outbox.stats()["by_state"].get("failed") == 1, outbox.stats()
        with store.read() as conn:
            parts = conn.execute(
                "SELECT body_text FROM event_parts WHERE event_id = ?", (raw["event_id"],)
            ).fetchall()
        assert [row["body_text"] for row in parts] == [None], [dict(row) for row in parts]
    finally:
        app.stop()


def test_expired_source_detail_keeps_conclusion_through_real_api(tmp_path, socket_dir):
    """Corrected expiry semantics (Codex followups-v2): the source detail expires
    and is reported as unreadable, while the valid conclusion K is still recallable
    through the real API and its vector is retained."""
    detail = "T20_R3_E2E_EXPIRY_DETAIL_8812"
    event = build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=SOURCE,
        event_time=EVENT_TIME,
        text=f"{detail} 当前端口4200",
        content_revision="native:1",
        project_ids=("project:songta",),
    )[0]
    store, app = _setup_kw(tmp_path, socket_dir)
    client = ServiceClient(app.socket_path, client_id=CLIENT_ID)
    try:
        notify = ServiceClient(app.socket_path, client_id=CLIENT_ID)
        status, receipt = notify.post_batch(
            {"schema_version": "1.0", "batch_id": "batch:e2e:expiry", "events": [event]}
        )
        assert status == 200 and receipt["receipts"][0]["disposition"] == "accepted", receipt

        conclusion = record_claim_revision(
            store,
            workspace_id=WORKSPACE,
            project_id="project:songta",
            subject_id="subject:e2e-server",
            predicate="server.port",
            facet="desired",
            qualifiers={},
            value=4200,
            display_text="当前端口4200",
            authority="user_confirmed",
            assertion_kind="explicit_statement",
            event_time=EVENT_TIME,
            evidence=[
                {
                    "event_id": event["event_id"],
                    "event_revision": 1,
                    "part_index": 0,
                    "start_byte": len(detail.encode("utf-8")) + 1,
                    "end_byte": len((detail + " 当前端口4200").encode("utf-8")),
                    "body_sha256": event["body"]["sha256"],
                    "availability": "full",
                }
            ],
        )
        collect_vectors(store, {conclusion["revision_id"]}, HashEmbeddingProvider(dimension=2))
        register_lifecycle(
            store,
            subject_kind="event_part",
            subject_id=f"{event['event_id']}#1@0",
            class_="completed_task",
            business_anchor="2020-01-01T00:00:00Z",
        )
        swept = sweep(store, now="2026-10-30T00:00:00Z")
        assert swept["event_parts"] == 1, swept

        status, source = client.get_source(event["event_id"], 1, 0)
        assert status == 200, source
        assert source["availability"] == "expired" and source["text"] is None, source

        status, recall = client.request("POST", "/v1/recall", RECALL_BODY)
        assert status == 200, recall
        assert recall["items"], recall
        hit = next(item for item in recall["items"] if item["revision_id"] == conclusion["revision_id"])
        assert hit["source_detail_expired"] is True, hit
        assert all(link["availability"] == "expired" for link in hit["sources"]), hit
        assert detail not in json.dumps(recall, ensure_ascii=False), "expired detail must not be returned"
        with store.read() as conn:
            vectors = conn.execute(
                "SELECT COUNT(*) AS c FROM claim_vectors WHERE revision_id = ?",
                (conclusion["revision_id"],),
            ).fetchone()["c"]
        assert vectors == 1, "valid conclusion vector must survive source-detail expiry"
    finally:
        app.stop()
