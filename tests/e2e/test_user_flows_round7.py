"""T20 Round-7 — user-visible acceptance flows (isolated synthetic E2E).

This file covers the eight user-facing flows requested for Round-7
(``work/t20-round7/PLAN.md`` §2 "用户实际使用流程验收").  Each test's docstring
names the flow it covers:

1. saved events survive a restart, and duplicate delivery/replay never
   duplicates the stored memory (real KW inbox/store idempotency + real DMA
   durable-outbox replay);
2. a new session recalls previously saved information through the real recall
   entry point (KW UDS + DMA recall client), including scope/identity;
3. different agents keep a clear identity/source (actor/host/source metadata)
   and project knowledge can be shared inside the authorized project scope;
4. after an explicit user correction the new value is current, the old value
   stays traceable, and a late extraction of an older record cannot overwrite
   the correction;
5. completion self-reports and explicit examples / unfinished records are not
   treated as verified completion (F04 semantics: ``reported_done`` +
   downgrade marker + preserved evidence);
6. interruption / temporary unavailability / failed delivery expose an
   explicit state, and processing resumes after recovery (DMA outbox
   state/retry + KW unavailable and restart);
7. expiry cleanup does not delete still-valid conclusions (conclusions are
   retained, the raw source text expires);
8. export/import/restore keeps the necessary state and never clears an
   unrecovered failure (AC52 semantics).

Isolation and honesty
---------------------
* All databases, outboxes and snapshots live under ``pytest``'s ``tmp_path``;
  UDS sockets live in a short private directory under ``/tmp`` that is removed
  after each test; every server thread is stopped in a ``finally`` block.
* Synthetic data only.  No real HOME/Keychain/production history is read, no
  real host adapter is started, no model or embedding endpoint is called: the
  extraction tests use a deterministic in-memory stand-in for the external
  model (the *backend* under test — validation, authority, projection — is the
  real product code), and recall uses keyword-only retrieval.
* This is an isolated, synthetic model/host/history-environment flow check; it
  is **not** evidence that a real Hermes/DSH/OpenClaw host, a real model, real
  imported history or production deployment works.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events
from dma.integrations.hermes import DmaMemoryProvider
from dma.integrations.recall_client import RecallClient
from dma.outbox import Outbox
from dma.redaction import Redactor

from knowledge_weaver import contracts
from knowledge_weaver.extraction import load_event_group, run_worker_once, sanitized_parts
from knowledge_weaver.lifecycle import is_tombstoned, lifecycle_status, register_lifecycle, sweep
from knowledge_weaver.portability import SnapshotError, export_snapshot, import_snapshot
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import (
    Store,
    current_items,
    get_current,
    list_task_views,
    receive_batch,
    register_client,
)
from knowledge_weaver.store.status import OPERATION_LEDGER_VERSION, status as status_view

import knowledge_weaver.portability.export as exporting

WORKSPACE = "owner:demo"
HOST = "hermes"
INSTANCE = "instance:hermes-r7"
CLIENT = "client:hermes-r7"
OTHER_CLIENT = "client:other-r7"
DSH_CLIENT = "client:dsh-r7"
DSH_INSTANCE = "instance:dsh-r7"

EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}
T1 = {"value": "2026-01-01T00:00:00Z", "precision": "instant", "timezone": "UTC"}
T2 = {"value": "2026-06-01T00:00:00Z", "precision": "instant", "timezone": "UTC"}
T_LATE = {"value": "2026-03-01T00:00:00Z", "precision": "instant", "timezone": "UTC"}

ACCEPTANCE_CONDITIONS = ("security_review", "restore_validation")


# ---------------------------------------------------------------------------
# isolation fixtures / helpers (no shared product code is modified)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolated_process_env(tmp_path, monkeypatch):
    """Explicit cache env isolation; HOME/CODEX_HOME are intentionally untouched.

    Every durable path this file uses is passed explicitly under ``tmp_path``.
    """
    cache = tmp_path / "isolated-env"
    cache.mkdir()
    for name in ("TMPDIR", "TMP", "TEMP", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "HF_HOME"):
        monkeypatch.setenv(name, str(cache / name.lower()))
    monkeypatch.setenv("T20_R7_E2E_ISOLATED", "1")
    yield


@pytest.fixture()
def uds_dir():
    """A short private UDS directory, removed even when a test fails."""
    base = os.environ.get("E2E_SOCKET_DIR") or "/tmp"
    directory = Path(tempfile.mkdtemp(prefix="umr7-uds-", dir=base))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def _store(tmp_path: Path, name: str = "kw") -> Store:
    return Store(tmp_path / name / "knowledge.db")


def _register(
    store: Store,
    *,
    client_id: str = CLIENT,
    host: str = HOST,
    instance_id: str = INSTANCE,
    actor_ids=("jun-test",),
    project_ids=("project:songta",),
) -> dict:
    return register_client(
        store,
        client_id=client_id,
        workspace_id=WORKSPACE,
        host=host,
        instance_id=instance_id,
        actor_ids=list(actor_ids),
        project_ids=list(project_ids),
    )


def _socket(uds_dir: Path, name: str = "kw") -> Path:
    return uds_dir / f"{name}.sock"


def _start(store: Store, uds_dir: Path, name: str = "kw") -> ServiceApp:
    return ServiceApp(store, _socket(uds_dir, name)).start()


def _service_client(app: ServiceApp, client_id: str = CLIENT) -> ServiceClient:
    return ServiceClient(app.socket_path, client_id=client_id, timeout=3.0)


def _delivery_client(app: ServiceApp, client_id: str = CLIENT) -> DeliveryClient:
    return DeliveryClient(app.socket_path, client_id=client_id, timeout=3.0)


def _outbox(tmp_path: Path, *, max_attempts: int = 3, backoff_delays=(0, 0, 0)) -> Outbox:
    return Outbox(
        tmp_path / "dma" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=INSTANCE,
        max_attempts=max_attempts,
        backoff_delays=backoff_delays,
    )


def _source(
    native_id: str,
    *,
    actor_id: str = "jun-test",
    host: str = HOST,
    instance_id: str = INSTANCE,
    session_id: str = "session:r7",
    role: str = "user",
    origin_kind: str = "user_input",
) -> dict:
    return {
        "instance_id": instance_id,
        "host": host,
        "actor_id": actor_id,
        "session_id": session_id,
        "native_event_id": native_id,
        "native_schema": "synthetic-r7",
        "role": role,
        "origin_kind": origin_kind,
        "locator": f"synthetic://{session_id}/{native_id}",
    }


def _event(
    native_id: str,
    text: str,
    *,
    event_time: dict | None = None,
    project_ids=("project:songta",),
    **source_kwargs,
) -> dict:
    source = _source(native_id, **source_kwargs)
    return {
        "schema_version": "1.0",
        "event_id": contracts.canonical_event_id(WORKSPACE, source["instance_id"], native_id),
        "event_revision": 1,
        "workspace_id": WORKSPACE,
        "operation": "create",
        "source": source,
        "event_time": event_time or EVENT_TIME,
        "body": {
            "text": text,
            "sha256": contracts.body_digest(text),
            "part_index": 0,
            "part_count": 1,
            "content_revision": "native:1",
            "format": "text/plain",
        },
        "redaction": {
            "policy_version": "redact:1",
            "status": "passed",
            "replacements": 0,
            "credential_refs": [],
        },
        "project_ids": list(project_ids),
        "parent_revision": None,
        "derived_from": [],
    }


def _batch(events, batch_id: str = "batch:r7:1") -> dict:
    return {"schema_version": "1.0", "batch_id": batch_id, "events": list(events)}


def _dma_events(
    text: str,
    native_id: str,
    *,
    actor_id: str = "jun-test",
    host: str = HOST,
    instance_id: str = INSTANCE,
    project_ids=("project:songta",),
) -> list[dict]:
    return build_message_events(
        redactor=Redactor(),
        workspace_id=WORKSPACE,
        source=_source(
            native_id, actor_id=actor_id, host=host, instance_id=instance_id
        ),
        event_time=EVENT_TIME,
        text=text,
        content_revision="native:1",
        project_ids=project_ids,
    )


def _evidence_ref(event: dict, *, quote: str | None = None) -> dict:
    text = quote if quote is not None else event["body"]["text"]
    start = 0
    end = len(text.encode("utf-8"))
    return {
        "event_id": event["event_id"],
        "event_revision": event["event_revision"],
        "part_index": event["body"]["part_index"],
        "start_byte": start,
        "end_byte": end,
        "body_sha256": event["body"]["sha256"],
        "availability": "full",
    }


def _proposal(*claims, warnings=None) -> dict:
    return {
        "schema_version": "1.0",
        "claims": list(claims),
        "excluded_event_ids": [],
        "warnings": list(warnings or []),
    }


def _claim(
    part,
    *,
    predicate: str,
    facet: str,
    value,
    display_text: str,
    assertion_kind: str = "explicit_statement",
    subject_hint: str = "松塔项目服务器",
    project_hint: str = "project:songta",
    qualifiers=None,
    evidence=None,
) -> dict:
    return {
        "subject_hint": subject_hint,
        "project_hint": project_hint,
        "qualifiers": dict(qualifiers or {}),
        "predicate": predicate,
        "facet": facet,
        "value": value,
        "display_text": display_text,
        "assertion_kind": assertion_kind,
        "evidence": evidence
        or [
            {
                "event_id": part.event_id,
                "event_revision": part.event_revision,
                "part_index": part.part_index,
                "support_quote": part.text,
            }
        ],
        "target_claim_hint": None,
        "effective_time_quote": None,
    }


class _MockExtractor:
    """Deterministic stand-in for the external model (no model call is made)."""

    def __init__(self, proposal: dict):
        self.proposal = proposal
        self.calls: list = []

    def extract(self, parts, context):
        self.calls.append((parts, context))
        return self.proposal


class _ScriptedExtractor:
    """Per-event-scripted stand-in for the external model (no model call)."""

    def __init__(self, builders: dict):
        self.builders = dict(builders)
        self.calls: list = []

    def extract(self, parts, context):
        self.calls.append((parts, context))
        part = parts[0]
        builder = self.builders.get(part.event_id)
        if builder is None:
            return _proposal()
        if callable(builder):
            return builder(part)
        return builder


def _drain(store: Store, extractor) -> list[dict]:
    """Run the real extraction worker until the durable job queue is empty."""
    outcomes = []
    for _ in range(30):
        outcome = run_worker_once(store, extractor)
        if not outcome.get("claimed"):
            return outcomes
        outcomes.append(outcome)
    raise AssertionError("extraction jobs did not drain")


def _extract_one(store: Store, event: dict, proposal: dict) -> dict:
    """One real extraction job for the event handed to a deterministic proposal."""
    outcomes = _drain(store, _MockExtractor(proposal))
    assert outcomes, "the real inbox must enqueue an extraction job for the event"
    result = outcomes[-1]
    assert result["state"] == "succeeded", result
    return result


def _extract_after(store: Store, event: dict, claim: dict, *, extractor=None) -> dict:
    """Extract ``event`` with a claim proposal built by ``claim`` (real worker)."""
    return _extract_one(store, event, claim)


def _provider(
    app_or_path,
    *,
    client_id: str = CLIENT,
    project_ids=("project:songta",),
    timeout: float = 3.0,
) -> DmaMemoryProvider:
    socket_path = (
        app_or_path.socket_path
        if isinstance(app_or_path, ServiceApp)
        else str(app_or_path)
    )
    return DmaMemoryProvider(
        recall_client=RecallClient(socket_path, client_id, timeout=timeout),
        client_id=client_id,
        project_ids=tuple(project_ids),
        deadline_ms=3000,
    )


def _recall(
    app: ServiceApp,
    *,
    query: str,
    intent: str = "current",
    project_ids=("project:songta",),
    client_id: str = CLIENT,
    session_id: str = "session:new",
):
    return RecallClient(app.socket_path, client_id, timeout=3.0).recall(
        query=query,
        session_id=session_id,
        turn_id="turn:r7",
        request_id="req:r7:recall",
        project_ids=project_ids,
        intent=intent,
    )


def _items(store: Store, *, predicate: str | None = None, states=None) -> list[dict]:
    kwargs = {"workspace_id": WORKSPACE}
    if states is not None:
        kwargs["include_states"] = states
    items = current_items(store, **kwargs)
    if predicate is None:
        return items
    return [item for item in items if item["predicate"] == predicate]


def _only_current(store: Store, predicate: str) -> dict:
    items = [item for item in _items(store) if item["state"] == "current"]
    matching = [item for item in items if item["predicate"] == predicate]
    assert len(matching) == 1, matching
    return matching[0]


def _pipelines(store: Store) -> dict:
    return {entry["operation"]: entry for entry in status_view(store)["pipelines"]}


def _ledger_rows(store: Store, kind: str) -> list[dict]:
    with store.read() as conn:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM jobs WHERE kind = ? ORDER BY created_at, rowid", (kind,)
            ).fetchall()
        ]


def _operation_rows(store: Store, kind: str) -> list[dict]:
    return [
        row
        for row in _ledger_rows(store, kind)
        if row.get("model_version") == OPERATION_LEDGER_VERSION
    ]


def _raise_synthetic(*args, **kwargs):
    raise RuntimeError("synthetic operation failure")


def _fail_export(store: Store, tmp_path: Path, monkeypatch, name: str) -> dict:
    """Run the real export entry point once and record a real failed ledger row."""
    with monkeypatch.context() as patch:
        patch.setattr(exporting, "_write_jsonl", _raise_synthetic)
        with pytest.raises(RuntimeError):
            export_snapshot(store, tmp_path / name)
    failed = [row for row in _operation_rows(store, "export") if row["state"] == "failed"]
    assert failed, "the real export failure must be recorded in the durable ledger"
    return failed[-1]


def _succeed_export(store: Store, tmp_path: Path, name: str) -> dict:
    export_snapshot(store, tmp_path / name)
    succeeded = [row for row in _operation_rows(store, "export") if row["state"] == "succeeded"]
    assert succeeded
    return succeeded[-1]


def _snapshot_jobs(snapshot: Path) -> list[dict]:
    path = snapshot / "records" / "jobs.jsonl"
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_snapshot_jobs(snapshot: Path, rows: list[dict]) -> None:
    path = snapshot / "records" / "jobs.jsonl"
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    _rehash(snapshot)


def _rehash(snapshot: Path) -> None:
    """Recompute manifest file hashes and SHA256SUMS after a synthetic edit."""
    manifest_path = snapshot / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for name, entry in manifest["files"].items():
        payload = (snapshot / name).read_bytes()
        entry["sha256"] = hashlib.sha256(payload).hexdigest()
        entry["size"] = len(payload)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    sums = "".join(
        hashlib.sha256(path.read_bytes()).hexdigest()
        + "  "
        + path.relative_to(snapshot).as_posix()
        + "\n"
        for path in sorted(snapshot.rglob("*"))
        if path.is_file() and path.name != "SHA256SUMS"
    )
    (snapshot / "SHA256SUMS").write_text(sums, encoding="utf-8")


# ===========================================================================
# Flow 1 — saved events survive a restart; duplicate delivery is idempotent
# ===========================================================================


def test_user_flow_1_saved_events_survive_restart_and_replays_are_idempotent(
    tmp_path, uds_dir
):
    """User flow 1: a saved event is still there after restart, and the same
    delivery (lost response / outbox replay) never duplicates the memory."""
    store = _store(tmp_path)
    _register(store)
    app = _start(store, uds_dir, "kw1")
    outbox = _outbox(tmp_path)
    try:
        events = _dma_events("松塔项目端口改为4200。", "message:flow1:1")
        outbox.capture(events, stream="hermes:main", position="1")
        assert outbox.stats()["by_state"] == {"pending": 1}

        # The KW committed the batch but the response was lost: the durable
        # outbox still holds the row and replays the exact same batch.
        batch, keys = outbox.build_batches()[0]
        assert keys == [(events[0]["event_id"], 1, 0)]
        status, payload = _service_client(app).post_batch(batch)
        assert status == 200
        assert payload["receipts"][0]["disposition"] == "accepted"
        assert outbox.stats()["by_state"] == {"pending": 1}, "the ack was not applied"

        replay = deliver_once(outbox, _delivery_client(app))
        assert replay["duplicate"] == 1, replay
        assert replay["failed"] == 0, replay
        assert outbox.stats()["by_state"] == {"delivered": 1}

        # Semantic memory must also be written exactly once.
        part = sanitized_parts(load_event_group(store, events[0]["event_id"], 1))[0]
        _extract_after(
            store,
            events[0],
            _proposal(
                _claim(
                    part,
                    predicate="server.port",
                    facet="desired",
                    value=4200,
                    display_text="松塔项目端口改为4200",
                )
            ),
        )
        with store.read() as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                for table in ("events", "event_parts", "claim_revisions")
            }
        assert counts == {"events": 1, "event_parts": 1, "claim_revisions": 1}, counts
        watermark = store.watermark()
    finally:
        app.stop()

    # A process restart over the same durable database.
    restarted = Store(store.db_path)
    app2 = _start(restarted, uds_dir, "kw2")
    try:
        assert restarted.watermark() == watermark
        status, source = _service_client(app2).get_source(events[0]["event_id"], 1)
        assert status == 200, (status, source)
        assert source["text"] == "松塔项目端口改为4200。"
        assert source["source"]["host"] == "hermes"
        assert source["source"]["actor_id"] == "jun-test"

        # Replaying the same batch after the restart is answered as duplicate
        # and does not add a second part or a second semantic revision.
        status, replay_after_restart = _service_client(app2).post_batch(batch)
        assert status == 200
        assert replay_after_restart["receipts"][0]["disposition"] == "duplicate"
        with restarted.read() as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
                for table in ("events", "event_parts", "claim_revisions")
            }
        assert counts == {"events": 1, "event_parts": 1, "claim_revisions": 1}, counts
        assert restarted.watermark() == watermark
    finally:
        app2.stop()

    # The durable outbox remembers the delivery across its own restart too.
    reopened_outbox = _outbox(tmp_path)
    assert reopened_outbox.stats()["by_state"] == {"delivered": 1}
    assert deliver_once(reopened_outbox, _delivery_client(app2))["batches"] == 0


# ===========================================================================
# Flow 2 — a new session recalls previously saved information
# ===========================================================================


def test_user_flow_2_new_session_recalls_previously_saved_info(tmp_path, uds_dir):
    """User flow 2: a new session recalls an earlier saved fact through the real
    recall entry, under the client's scope/identity."""
    store = _store(tmp_path)
    _register(store)
    _register(
        store,
        client_id=OTHER_CLIENT,
        actor_ids=("*",),
        project_ids=("project:salt",),
    )
    app = _start(store, uds_dir, "kw")
    outbox = _outbox(tmp_path)
    try:
        events = _dma_events("松塔项目端口为4200。", "message:flow2:1")
        outbox.capture(events, stream="hermes:main", position="1")
        assert deliver_once(outbox, _delivery_client(app))["accepted"] == 1
        part = sanitized_parts(load_event_group(store, events[0]["event_id"], 1))[0]
        _extract_after(
            store,
            events[0],
            _proposal(
                _claim(
                    part,
                    predicate="server.port",
                    facet="desired",
                    value=4200,
                    display_text="松塔项目端口为4200",
                )
            ),
        )
    finally:
        app.stop()

    # New process lifetime, same durable database: this is the "new session".
    restarted = Store(store.db_path)
    app2 = _start(restarted, uds_dir, "kw2")
    try:
        provider = _provider(app2, project_ids=("project:songta",))
        text = provider.prefetch("松塔项目端口", session_id="session:new-dialog")
        assert text, "a new session must recall the previously saved fact"
        assert "4200" in text
        assert "project:songta" in text
        # Provenance/identity labels: which agent/source, which evidence grade.
        assert "来源者=jun-test" in text
        assert "证据等级=user_confirmed" in text
        assert provider.last_error_code is None
        assert provider.recall_status() is not None
        assert provider.recall_status().count >= 1

        item = _only_current(restarted, "server.port")
        assert item["project_id"] == "project:songta"
        assert item["evidence_grade"] == "user_confirmed"
        assert item["sources"][0]["event_id"] == events[0]["event_id"]

        # Identity is required: an unregistered client gets an explicit error,
        # not a silently empty or fabricated memory.
        stranger = _provider(app2, client_id="client:not-registered")
        notice = stranger.prefetch("松塔项目端口")
        assert "历史暂不可用" in notice
        assert not any(value in notice for value in ("4200", "周二", "盐场项目备份周期"))
        assert stranger.last_error_code == "UNREGISTERED_CLIENT"

        # Scope is enforced: a client bound to another project cannot recall it.
        foreign = _provider(app2, client_id=OTHER_CLIENT, project_ids=("project:songta",))
        notice = foreign.prefetch("松塔项目端口")
        assert "历史暂不可用" in notice
        assert not any(value in notice for value in ("4200", "周二", "盐场项目备份周期"))
        assert foreign.last_error_code == "SCOPE_DENIED"
    finally:
        app2.stop()


# ===========================================================================
# Flow 3 — agent identity/source is clear; shared project knowledge is scoped
# ===========================================================================


def test_user_flow_3_agent_identity_and_shared_project_scope(tmp_path, uds_dir):
    """User flow 3: two agents' facts keep a clear actor/host/source, and a
    project the second agent is authorized for is shared (with project filter)."""
    store = _store(tmp_path)
    _register(store, client_id=CLIENT)
    _register(
        store,
        client_id=DSH_CLIENT,
        host="dsh",
        instance_id=DSH_INSTANCE,
        actor_ids=("dsh-agent",),
        project_ids=("project:songta", "project:salt"),
    )
    app = _start(store, uds_dir, "kw")
    try:
        hermes_event = _event("message:flow3:hermes", "松塔项目端口为4200。")
        dsh_event = _event(
            "message:flow3:dsh",
            "盐场项目备份周期为每周。",
            project_ids=("project:salt",),
            host="dsh",
            instance_id=DSH_INSTANCE,
            actor_id="dsh-agent",
        )
        status, payload = _service_client(app).post_batch(
            _batch([hermes_event], "batch:flow3:hermes")
        )
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted", payload
        status, payload = _service_client(app, DSH_CLIENT).post_batch(
            _batch([dsh_event], "batch:flow3:dsh")
        )
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted", payload

        _drain(
            store,
            _ScriptedExtractor(
                {
                    hermes_event["event_id"]: lambda part: _proposal(
                        _claim(
                            part,
                            predicate="server.port",
                            facet="desired",
                            value=4200,
                            display_text="松塔项目端口为4200",
                        )
                    ),
                    dsh_event["event_id"]: lambda part: _proposal(
                        _claim(
                            part,
                            predicate="backup.schedule",
                            facet="desired",
                            value="每周",
                            display_text="盐场项目备份周期为每周",
                            project_hint="project:salt",
                        )
                    ),
                }
            ),
        )

        # Identity/source of each stored fact is unambiguous.
        songta_item = _only_current(store, "server.port")
        salt_item = _only_current(store, "backup.schedule")
        assert songta_item["project_id"] == "project:songta"
        assert salt_item["project_id"] == "project:salt"
        assert songta_item["source_actors"] == ["jun-test"]
        assert salt_item["source_actors"] == ["dsh-agent"]
        assert songta_item["sources"][0]["event_id"] == hermes_event["event_id"]
        assert salt_item["sources"][0]["event_id"] == dsh_event["event_id"]

        status, source = _service_client(app).get_source(hermes_event["event_id"], 1)
        assert status == 200
        assert (source["source"]["host"], source["source"]["actor_id"]) == (
            "hermes",
            "jun-test",
        )
        status, source = _service_client(app, DSH_CLIENT).get_source(
            dsh_event["event_id"], 1
        )
        assert status == 200
        assert (source["source"]["host"], source["source"]["actor_id"]) == (
            "dsh",
            "dsh-agent",
        )

        # The dsh agent shares the hermes-owned project knowledge it is
        # authorized for, and the provenance says whose fact it is.
        shared = _provider(app, client_id=DSH_CLIENT, project_ids=("project:songta",))
        shared_text = shared.prefetch("松塔项目端口", session_id="session:dsh")
        assert "4200" in shared_text
        assert "project:songta" in shared_text
        assert "来源者=jun-test" in shared_text
        assert "盐场项目备份周期" not in shared_text

        # Project scope filtering: asking for the salt project never returns
        # the songta fact.
        filtered = _provider(app, client_id=DSH_CLIENT, project_ids=("project:salt",))
        filtered_text = filtered.prefetch("盐场项目备份周期", session_id="session:dsh")
        assert "盐场项目备份周期" in filtered_text
        assert "4200" not in filtered_text

        # A scope the client is not authorized for is refused explicitly.
        denied = _provider(app, client_id=CLIENT, project_ids=("project:salt",))
        notice = denied.prefetch("盐场项目备份周期")
        assert "历史暂不可用" in notice
        assert not any(value in notice for value in ("4200", "周二", "盐场项目备份周期"))
        assert denied.last_error_code == "SCOPE_DENIED"
    finally:
        app.stop()


# ===========================================================================
# Flow 4 — user correction: new value current, old traceable, late extraction
# ===========================================================================


def test_user_flow_4_correction_current_old_traceable_and_late_extraction(
    tmp_path, uds_dir
):
    """User flow 4: after a user correction the new value is current, the old
    value keeps its source/time in history, and a late extraction of an older
    record cannot overwrite the correction."""
    store = _store(tmp_path)
    _register(store)
    app = _start(store, uds_dir, "kw")
    try:
        client = _service_client(app)
        old_event = _event(
            "message:flow4:old", "松塔项目数据库使用 SQLite。", event_time=T1
        )
        status, payload = client.post_batch(_batch([old_event], "batch:flow4:old"))
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted"
        old_part = sanitized_parts(
            load_event_group(store, old_event["event_id"], 1)
        )[0]
        _extract_after(
            store,
            old_event,
            _proposal(
                _claim(
                    old_part,
                    predicate="project.db",
                    facet="desired",
                    value="SQLite",
                    display_text="项目使用 SQLite",
                )
            ),
        )
        item = _only_current(store, "project.db")
        claim_id = item["claim_id"]
        old_revision = item["revision_id"]
        generation = get_current(store, claim_id)["projection_generation"]

        # The user's explicit correction arrives as a real user event.
        correction_event = _event(
            "message:flow4:correction", "改用本地 PostgreSQL。", event_time=T2
        )
        status, payload = client.post_batch(
            _batch([correction_event], "batch:flow4:correction")
        )
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted"
        correction = {
            "schema_version": "1.0",
            "operation_id": "op:flow4:correct:1",
            "claim_id": claim_id,
            "expected_revision_id": old_revision,
            "expected_projection_generation": generation,
            "new_value": "PostgreSQL",
            "display_text": "改用本地 PostgreSQL",
            "evidence": [_evidence_ref(correction_event)],
            "supersedes_revision_ids": [old_revision],
            "reason": "用户明确更正",
        }
        status, result = client.request("POST", "/v1/corrections", correction)
        assert status == 200, (status, result)
        assert result["status"] == "applied", result
        assert result["projection_generation"] > generation
        corrected_revision = result["revision_id"]

        # New value is the single current truth.
        current = _items(store, predicate="project.db")
        current_values = [entry["value"] for entry in current if entry["state"] == "current"]
        assert current_values == ["PostgreSQL"], current
        assert _only_current(store, "project.db")["revision_id"] == corrected_revision

        # Old value remains traceable with its own source and business time.
        with store.read() as conn:
            old_row = conn.execute(
                "SELECT * FROM claim_revisions WHERE revision_id = ?", (old_revision,)
            ).fetchone()
            old_links = conn.execute(
                "SELECT * FROM evidence_links WHERE revision_id = ?", (old_revision,)
            ).fetchall()
        assert old_row["status"] == "superseded", dict(old_row)
        assert old_row["event_time_value"] == T1["value"]
        assert [link["event_id"] for link in old_links] == [old_event["event_id"]]

        # A late extraction of an *older* record (March, before the June
        # correction) runs through the real worker and must stay stale.
        late_event = _event(
            "message:flow4:late", "松塔项目数据库使用 SQLite。", event_time=T_LATE
        )
        status, payload = client.post_batch(_batch([late_event], "batch:flow4:late"))
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted"
        _drain(
            store,
            _ScriptedExtractor(
                {
                    late_event["event_id"]: lambda part: _proposal(
                        _claim(
                            part,
                            predicate="project.db",
                            facet="desired",
                            value="SQLite",
                            display_text="项目使用 SQLite",
                        )
                    ),
                    correction_event["event_id"]: lambda part: _proposal(),
                }
            ),
        )
        current_after = _items(store, predicate="project.db")
        current_after_values = [
            entry["value"] for entry in current_after if entry["state"] == "current"
        ]
        assert current_after_values == ["PostgreSQL"], current_after
        assert get_current(store, claim_id)["state"] == "current"
        assert get_current(store, claim_id)["projection_generation"] >= result[
            "projection_generation"
        ]

        # Current recall by the old phrasing does not resurrect the old value.
        promoted = _recall(app, query="SQLite")
        assert promoted["status"] == "empty", promoted
        assert promoted["items"] == [], promoted["items"]

        # History recall (real retrieval entry) keeps the old value with source.
        history = _recall(app, query="SQLite PostgreSQL", intent="history")
        by_revision = {entry["revision_id"]: entry for entry in history["items"]}
        assert old_revision in by_revision, history["items"]
        assert by_revision[old_revision]["state"] == "superseded"
        assert by_revision[old_revision]["sources"], by_revision[old_revision]
        assert (
            by_revision[old_revision]["event_time"]["value"] == T1["value"]
        ), by_revision[old_revision]
    finally:
        app.stop()


# ===========================================================================
# Flow 5 — completion self-report / explicit example is never "verified"
# ===========================================================================


def _receive_tool(store: Store, text: str, *, native_id: str) -> dict:
    event = _event(
        native_id,
        text,
        role="tool",
        origin_kind="tool_execution",
        actor_id="tool:deploy",
    )
    received = receive_batch(store, _batch([event], f"batch:{native_id}"))
    assert received["receipts"][0]["disposition"] == "accepted", received
    return event


def _verified_value():
    conditions = list(ACCEPTANCE_CONDITIONS)
    return {"status": "verified", "conditions": conditions, "satisfied": conditions}


def _task_proposal(part, value, *, assertion_kind="observation", quote=None):
    evidence = [
        {
            "event_id": part.event_id,
            "event_revision": part.event_revision,
            "part_index": part.part_index,
            "support_quote": quote or part.text,
        }
    ]
    return _proposal(
        _claim(
            part,
            predicate="task.status",
            facet="observed",
            value=value,
            display_text=part.text,
            assertion_kind=assertion_kind,
            subject_hint="松塔项目部署任务",
            evidence=evidence,
        )
    )


def _stored_task_value(store: Store) -> dict:
    with store.read() as conn:
        row = conn.execute(
            "SELECT value_json FROM claim_revisions ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
    assert row is not None
    return json.loads(row["value_json"])


def _only_task_view(store: Store) -> dict:
    views = list_task_views(store, workspace_id=WORKSPACE)
    assert len(views) == 1, views
    return views[0]


def _assert_downgraded_completion(store: Store, event: dict) -> dict:
    """reported_done + backend_downgraded + preserved tool evidence/conditions."""
    view = _only_task_view(store)
    assert view["status"] == "reported_done", view
    assert view["status"] != "verified_done", view
    assert view["verified_revision_id"] is None, view
    assert view["verification_conditions"] == [], view

    stored = _stored_task_value(store)
    assert stored["status"] == "reported_done", stored
    assert (
        stored.get("backend_downgraded") == "conditions_not_grounded_in_evidence"
    ), stored
    assert stored.get("conditions") == list(ACCEPTANCE_CONDITIONS), stored

    # The real tool observation, evidence span, body hash and availability stay.
    items = [
        item
        for item in _items(store, predicate="task.status")
        if item["state"] == "current"
    ]
    assert len(items) == 1, items
    item = items[0]
    assert item["facet"] == "observed", item
    assert item["evidence_grade"] == "tool_verified", item
    assert item["sources"] and item["sources"][0]["event_id"] == event["event_id"], item
    with store.read() as conn:
        links = [dict(row) for row in conn.execute("SELECT * FROM evidence_links").fetchall()]
    assert len(links) == 1, links
    link = links[0]
    assert link["event_id"] == event["event_id"]
    assert link["body_sha256"] == event["body"]["sha256"]
    assert link["availability"] == "full"
    text = event["body"]["text"]
    assert link["start_byte"] == 0
    assert link["end_byte"] == len(text.encode("utf-8"))
    return view


@pytest.mark.parametrize(
    "text",
    [
        # F04-R6 real-entry minimal inputs (review/T20-CODEX-ROUND6.md §3):
        # the whole tool span is an explicit example / fiction / unfinished
        # record / interrogative / invalid count — never a complete result.
        "Example output only: security_review passed; restore_validation passed",
        "Fictional transcript: security_review passed; restore_validation passed",
        "security_review passed; restore_validation passed. These statements are fictional examples.",
        "示例输出：security_review 通过；restore_validation 通过。以上只是示例。",
        "security_review passed; restore_validation passed. Both reviews remain unfinished.",
        "security_review passed; restore_validation passed. 实际两项均不及格。",
        "Did security_review pass; restore_validation passed",
        "security_review passed 3 of 2 checks; restore_validation passed",
    ],
    ids=[
        "en-example-prefix",
        "en-fictional-prefix",
        "en-fictional-postfix",
        "zh-example-prefix",
        "en-unfinished-postfix",
        "zh-failed-postfix",
        "en-interrogative-prefix",
        "invalid-count-3-of-2",
    ],
)
def test_user_flow_5_reported_or_example_completion_is_not_verified(tmp_path, text):
    """User flow 5: an explicit example / fictional / unfinished / questioned
    record is shown as reported (with downgrade marker), never as verified."""
    store = _store(tmp_path)
    event = _receive_tool(store, text, native_id="message:flow5:example")
    part = sanitized_parts(load_event_group(store, event["event_id"], 1))[0]
    result = _extract_one(store, event, _task_proposal(part, _verified_value()))
    assert result["claims_written"] == 1, result
    _assert_downgraded_completion(store, event)


def test_user_flow_5_completion_self_report_is_only_reported(tmp_path):
    """User flow 5: an assistant completion self-report cannot grant verified."""
    store = _store(tmp_path)
    event = _event(
        "message:flow5:self-report",
        "两项验收均已完成，可以发布。",
        role="assistant",
        origin_kind="assistant_reply",
        actor_id="assistant:agent",
    )
    receive_batch(store, _batch([event], "batch:flow5:self-report"))
    part = sanitized_parts(load_event_group(store, event["event_id"], 1))[0]
    result = _extract_one(
        store,
        event,
        _task_proposal(part, _verified_value(), assertion_kind="completion_report"),
    )
    assert result["claims_written"] == 1, result
    view = _only_task_view(store)
    assert view["status"] == "reported_done", view
    assert view["verified_revision_id"] is None, view
    stored = _stored_task_value(store)
    assert stored["status"] == "reported_done", stored
    assert (
        stored.get("backend_downgraded") == "verified_requires_verified_evidence"
    ), stored
    assert stored.get("conditions") == [], stored
    # The assistant claim itself is still stored with its source evidence.
    items = [
        item for item in _items(store, predicate="task.status") if item["state"] == "current"
    ]
    assert len(items) == 1, items
    assert items[0]["evidence_grade"] == "reported", items[0]
    assert items[0]["sources"][0]["event_id"] == event["event_id"], items[0]


def test_user_flow_5_genuine_complete_result_still_verified(tmp_path):
    """User flow 5 control: a genuine complete review-result record still verifies.

    The tightening must not turn every task into ``reported_done``.
    """
    store = _store(tmp_path)
    event = _receive_tool(
        store,
        "security_review passed; restore_validation passed",
        native_id="message:flow5:positive",
    )
    part = sanitized_parts(load_event_group(store, event["event_id"], 1))[0]
    result = _extract_one(store, event, _task_proposal(part, _verified_value()))
    assert result["claims_written"] == 1, result
    view = _only_task_view(store)
    assert view["status"] == "verified_done", view
    assert view["verified_revision_id"] is not None, view
    assert view["verification_conditions"] == list(ACCEPTANCE_CONDITIONS), view
    stored = _stored_task_value(store)
    assert stored["status"] == "verified", stored
    assert "backend_downgraded" not in stored, stored


# ===========================================================================
# Flow 6 — interruption/unavailability is explicit and processing resumes
# ===========================================================================


def test_user_flow_6_interruption_state_and_resume_after_recovery(tmp_path, uds_dir):
    """User flow 6: a failed delivery and an unavailable memory backend have an
    explicit state; after the backend recovers, delivery/processing resumes."""
    outbox = _outbox(tmp_path, max_attempts=3, backoff_delays=(0, 0, 0))
    events = _dma_events("松塔项目部署窗口为周二。", "message:flow6:1")
    outbox.capture(events, stream="hermes:main", position="1")
    missing = _socket(uds_dir, "not-running")

    # 1) The memory backend is down: delivery fails with a durable,
    #    inspectable retry state and the payload is not dropped.
    first = deliver_once(outbox, DeliveryClient(missing, client_id=CLIENT, timeout=0.5))
    assert first["failed"] == 0, first
    assert first["retried"] == 1, first
    assert outbox.stats()["by_state"] == {"retry_wait": 1}
    with outbox.read() as conn:
        row = dict(conn.execute("SELECT * FROM outbox").fetchone())
    assert row["state"] == "retry_wait"
    assert row["attempts"] == 1
    assert row["last_error_code"] == "STORE_UNAVAILABLE"
    assert row["next_attempt_at"] is not None
    assert outbox.remote_meta().get("watermark") is None
    assert len(outbox.pending_rows()) == 1, "the failed payload must stay durable"

    # 2) The read path reports the same explicit unavailability, never an
    #    invented answer.
    down_provider = _provider(missing, timeout=0.5)
    notice = down_provider.prefetch("松塔项目部署窗口")
    assert "历史暂不可用" in notice
    assert not any(value in notice for value in ("4200", "周二", "盐场项目备份周期"))
    assert down_provider.last_error_code == "STORE_UNAVAILABLE"
    assert down_provider.recall_status() is None

    # A second failed attempt is still a bounded retry, not a lost memory.
    second = deliver_once(outbox, DeliveryClient(missing, client_id=CLIENT, timeout=0.5))
    assert second["retried"] == 1, second
    with outbox.read() as conn:
        row = dict(conn.execute("SELECT * FROM outbox").fetchone())
    assert row["state"] == "retry_wait" and row["attempts"] == 2

    # 3) The backend recovers; the durable outbox drains and the saved event is
    #    processed (extraction job) and recallable.
    store = _store(tmp_path)
    _register(store)
    app = _start(store, uds_dir, "kw")
    try:
        resumed = deliver_once(outbox, _delivery_client(app))
        assert resumed["accepted"] == 1, resumed
        assert outbox.stats()["by_state"] == {"delivered": 1}
        assert outbox.remote_meta()["watermark"] == store.watermark()
        status, source = _service_client(app).get_source(events[0]["event_id"], 1)
        assert status == 200 and source["text"] == "松塔项目部署窗口为周二。"
        with store.read() as conn:
            pending_jobs = conn.execute(
                "SELECT COUNT(*) AS c FROM jobs WHERE kind = 'extract' AND state = 'pending'"
            ).fetchone()["c"]
        assert pending_jobs == 1, "delivery must resume the durable processing queue"

        part = sanitized_parts(load_event_group(store, events[0]["event_id"], 1))[0]
        _extract_after(
            store,
            events[0],
            _proposal(
                _claim(
                    part,
                    predicate="deploy.window",
                    facet="desired",
                    value="周二",
                    display_text="部署窗口为周二",
                )
            ),
        )
        up_provider = _provider(app)
        up_text = up_provider.prefetch("松塔项目部署窗口", session_id="session:recovered")
        assert "周二" in up_text
        assert up_provider.last_error_code is None
        assert up_provider.recall_status() is not None
    finally:
        app.stop()


# ===========================================================================
# Flow 7 — expiry cleanup: conclusions retained, raw text expires
# ===========================================================================


def test_user_flow_7_expiry_keeps_conclusion_and_expires_raw_text(tmp_path, uds_dir):
    """User flow 7: the raw source text of an old record expires on schedule,
    while the still-valid conclusion is retained, current and recallable."""
    store = _store(tmp_path)
    _register(store)
    app = _start(store, uds_dir, "kw")
    try:
        client = _service_client(app)
        # An old statement (January, old process detail) ...
        old_event = _event(
            "message:flow7:old", "松塔项目端口改为4100。", event_time=T1
        )
        status, payload = client.post_batch(_batch([old_event], "batch:flow7:old"))
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted"
        old_part = sanitized_parts(load_event_group(store, old_event["event_id"], 1))[0]
        _extract_after(
            store,
            old_event,
            _proposal(
                _claim(
                    old_part,
                    predicate="server.port",
                    facet="desired",
                    value=4100,
                    display_text="松塔项目端口改为4100",
                )
            ),
        )
        # ... and the still-valid, newer conclusion (June).
        new_event = _event(
            "message:flow7:new", "松塔项目端口改为4200。", event_time=T2
        )
        status, payload = client.post_batch(_batch([new_event], "batch:flow7:new"))
        assert status == 200 and payload["receipts"][0]["disposition"] == "accepted"
        new_part = sanitized_parts(load_event_group(store, new_event["event_id"], 1))[0]
        _extract_after(
            store,
            new_event,
            _proposal(
                _claim(
                    new_part,
                    predicate="server.port",
                    facet="desired",
                    value=4200,
                    display_text="松塔项目端口改为4200",
                )
            ),
        )
        current = _only_current(store, "server.port")
        assert current["value"] == 4200
        current_revision = current["revision_id"]
        old_revision_row = None
        with store.read() as conn:
            old_revision_row = conn.execute(
                "SELECT revision_id FROM claim_revisions WHERE claim_id = ? "
                "AND revision_id <> ?",
                (current["claim_id"], current_revision),
            ).fetchone()
        assert old_revision_row is not None
        old_revision = old_revision_row["revision_id"]

        # The old raw process detail is due for cleanup; the conclusion is a
        # current fact and is not scheduled to expire.
        register_lifecycle(
            store,
            subject_kind="event_part",
            subject_id=f"{old_event['event_id']}#1@0",
            class_="completed_task",
            business_anchor="2020-01-01T00:00:00Z",
            reason="synthetic old process detail",
        )
        status, source = client.get_source(old_event["event_id"], 1)
        assert status == 200 and source["availability"] == "full"
        status, source = client.get_source(new_event["event_id"], 1)
        assert status == 200 and source["availability"] == "full"

        swept = sweep(store, now="2026-10-31T00:00:00Z")
        assert swept["event_parts"] == 2, swept
        assert swept["claim_revisions"] == 0, swept
        assert is_tombstoned(store, "event_part", f"{old_event['event_id']}#1@0")

        # The expired raw text is no longer readable as if it were still there.
        status, source = client.get_source(old_event["event_id"], 1)
        assert status == 200, (status, source)
        assert source["availability"] == "expired", source
        assert source["text"] is None, source

        # The conclusion is retained: current and unchanged. Both original
        # full source bodies are older than30 days, so neither stays forever
        # merely because one contains the current configuration.
        retained = _only_current(store, "server.port")
        assert retained["state"] == "current", retained
        assert retained["value"] == 4200, retained
        assert retained["revision_id"] == current_revision, retained
        assert retained["source_detail_expired"] is True, retained
        assert all(link["availability"] == "expired" for link in retained["sources"]), retained
        status, source = client.get_source(new_event["event_id"], 1)
        assert status == 200 and source["availability"] == "expired" and source["text"] is None
        with store.read() as conn:
            lifecycle_row = conn.execute(
                "SELECT class, expires_at FROM lifecycle_records WHERE subject_id = ?",
                (current_revision,),
            ).fetchone()
        assert dict(lifecycle_row) == {"class": "current_fact", "expires_at": None}, lifecycle_row

        # The older value stays traceable in history, with its source marked
        # expired instead of silently disappearing or claiming a readable text.
        history = _recall(app, query="松塔项目端口", intent="history")
        by_revision = {entry["revision_id"]: entry for entry in history["items"]}
        assert old_revision in by_revision, history["items"]
        old_history_item = by_revision[old_revision]
        assert old_history_item["value"] == 4100, old_history_item
        assert old_history_item["sources"], old_history_item
        assert all(
            link["availability"] == "expired" for link in old_history_item["sources"]
        ), old_history_item

        # The conclusion is still recallable for a new session.
        recalled = _provider(app).prefetch("松塔项目端口", session_id="session:after-sweep")
        assert "4200" in recalled
        assert "状态=current" in recalled
        # Repeated maintenance never renews or deletes the retained conclusion.
        again = sweep(store, now="2026-11-30T00:00:00Z")
        assert again["expired"] == 0, again
        assert _only_current(store, "server.port")["value"] == 4200
        assert lifecycle_status(store)["due_now"] == 0
    finally:
        app.stop()


# ===========================================================================
# Flow 8 — export/import/restore keeps state and never hides a real failure
# ===========================================================================


def test_user_flow_8_restore_keeps_failure_and_recovery_state(tmp_path, monkeypatch):
    """User flow 8: a snapshot restore keeps the durable failure/recovery state;
    an unrecovered failure stays visible after the restore (AC52)."""
    store = _store(tmp_path)
    receive_batch(
        store,
        _batch([_event("message:flow8:seed", "松塔项目端口为4200。")], "batch:flow8:seed"),
    )

    first_success = _succeed_export(store, tmp_path, "export-ok-1")
    failed = _fail_export(store, tmp_path, monkeypatch, "export-fail-1")
    visible = _pipelines(store)["export"]
    assert visible["failed"] == 1 and visible["state"] == "degraded", visible
    assert visible["last_error_code"] == "EXPORT_FAILED", visible

    recovering = _succeed_export(store, tmp_path, "export-ok-2")
    recovered = _pipelines(store)["export"]
    assert recovered["failed"] == 0 and recovered["state"] == "idle", recovered
    assert recovered["last_error_code"] is None, recovered
    failure_rows = [
        row for row in _operation_rows(store, "export") if row["state"] == "failed"
    ]
    assert len(failure_rows) == 1, failure_rows
    assert failure_rows[0]["recovered_by_job_id"] == recovering["job_id"], failure_rows
    assert failure_rows[0]["recovered_at"] is not None, failure_rows

    # A real failed cleanup afterwards is *not* recovered by the later snapshot
    # export (another operation): it must survive the restore.
    import knowledge_weaver.lifecycle.retention as retention

    with monkeypatch.context() as patch:
        patch.setattr(retention, "classify_claim_revisions", _raise_synthetic)
        with pytest.raises(RuntimeError):
            sweep(store, now="2026-10-31T00:00:00Z")
    cleanup_view = _pipelines(store)["cleanup"]
    assert cleanup_view["failed"] == 1 and cleanup_view["state"] == "degraded", cleanup_view
    assert cleanup_view["last_error_code"] == "CLEANUP_FAILED", cleanup_view

    snapshot = tmp_path / "snapshot"
    export_snapshot(store, snapshot)
    report = import_snapshot(snapshot, tmp_path / "restored")
    assert report["referential_integrity"] == "ok", report
    assert report["cleanup_warnings"] == [], report

    restored = Store(report["target_db"])
    restored_export = _pipelines(restored)["export"]
    assert restored_export["failed"] == 0 and restored_export["state"] == "idle", (
        restored_export
    )
    assert restored_export["last_error_code"] is None, restored_export
    restored_cleanup = _pipelines(restored)["cleanup"]
    assert restored_cleanup["failed"] == 1, restored_cleanup
    assert restored_cleanup["state"] == "degraded", restored_cleanup
    assert restored_cleanup["last_error_code"] == "CLEANUP_FAILED", restored_cleanup

    restored_failures = [
        row for row in _operation_rows(restored, "export") if row["state"] == "failed"
    ]
    assert len(restored_failures) == 1, restored_failures
    assert restored_failures[0]["job_id"] == failed["job_id"], restored_failures
    assert restored_failures[0]["recovered_by_job_id"] == recovering["job_id"], (
        restored_failures
    )
    assert restored_failures[0]["recovered_at"] is not None, restored_failures
    assert first_success["job_id"] != recovering["job_id"]

    # The restored database is self-contained: reopening it keeps the states.
    reopened = Store(report["target_db"])
    assert _pipelines(reopened)["export"]["last_error_code"] is None
    assert _pipelines(reopened)["cleanup"]["last_error_code"] == "CLEANUP_FAILED"


def test_user_flow_8_restore_keeps_unrecovered_failure_visible(tmp_path, monkeypatch):
    """User flow 8 (AC52): a failure that was never recovered is still visible
    after a real snapshot export/import restore (not silently cleared)."""
    store = _store(tmp_path)
    receive_batch(
        store,
        _batch([_event("message:flow8:seed", "松塔项目端口为4200。")], "batch:flow8:seed"),
    )
    failed = _fail_export(store, tmp_path, monkeypatch, "export-fail-1")
    live = _pipelines(store)["export"]
    assert live["failed"] == 1 and live["state"] == "degraded", live
    assert live["last_error_code"] == "EXPORT_FAILED", live

    # The real export entry point writes the snapshot; the failed operation was
    # not recovered at the moment the snapshot was captured, so it is part of
    # the exported state.
    export_snapshot(store, tmp_path / "snapshot")
    snapshot_rows = [
        row for row in _snapshot_jobs(tmp_path / "snapshot") if row["state"] == "failed"
    ]
    assert len(snapshot_rows) == 1, snapshot_rows
    assert snapshot_rows[0]["job_id"] == failed["job_id"], snapshot_rows
    assert snapshot_rows[0]["recovered_by_job_id"] is None, snapshot_rows

    report = import_snapshot(tmp_path / "snapshot", tmp_path / "restored")
    assert report["referential_integrity"] == "ok", report
    restored = Store(report["target_db"])
    restored_export = _pipelines(restored)["export"]
    assert restored_export["failed"] == 1, restored_export
    assert restored_export["state"] == "degraded", restored_export
    assert restored_export["last_error_code"] == "EXPORT_FAILED", restored_export
    restored_rows = [
        row for row in _operation_rows(restored, "export") if row["state"] == "failed"
    ]
    assert len(restored_rows) == 1, restored_rows
    assert restored_rows[0]["job_id"] == failed["job_id"], restored_rows
    assert restored_rows[0]["recovered_by_job_id"] is None, restored_rows
    # Restarting over the restored database does not green the failure either.
    reopened = Store(report["target_db"])
    assert _pipelines(reopened)["export"]["failed"] == 1
    assert _pipelines(reopened)["export"]["last_error_code"] == "EXPORT_FAILED"


def test_user_flow_8_restore_must_not_accept_unprovable_recovery(tmp_path, monkeypatch):
    """User flow 8 (AC52): a snapshot must not let an *earlier* success pose as
    the recovery of a later failure via a same-second timestamp; a failure that
    was not really recovered stays visible (or the snapshot is refused)."""
    store = _store(tmp_path)
    receive_batch(
        store,
        _batch([_event("message:flow8:seed", "松塔项目端口为4200。")], "batch:flow8:seed"),
    )
    earlier_success = _succeed_export(store, tmp_path, "export-ok-1")
    _fail_export(store, tmp_path, monkeypatch, "export-fail-1")
    # The snapshot is written by the real export entry point while the failure
    # is still unrecovered (the in-flight snapshot export itself is not part of
    # the capture), so a correct restore must keep that failure visible.
    export_snapshot(store, tmp_path / "snapshot")
    rows = _snapshot_jobs(tmp_path / "snapshot")
    failed_row = next(
        row for row in rows if row["state"] == "failed" and row["kind"] == "export"
    )
    earlier_row = next(
        row for row in rows if row["job_id"] == earlier_success["job_id"]
    )
    assert failed_row["recovered_by_job_id"] is None, failed_row

    # Synthetic fault injection (same seam style as the other isolated E2E
    # tests): the snapshot claims the failed run was recovered by the *earlier*
    # success, and fabricates the same-second timestamp so a plain string
    # comparison would accept it. The byte hashes are recomputed, so byte
    # damage cannot be the reason for any refusal.
    failed_row["recovered_by_job_id"] = earlier_row["job_id"]
    earlier_row["updated_at"] = failed_row["updated_at"]
    failed_row["recovered_at"] = failed_row["updated_at"]
    _write_snapshot_jobs(tmp_path / "snapshot", rows)

    target = tmp_path / "restored"
    try:
        report = import_snapshot(tmp_path / "snapshot", target)
    except SnapshotError as exc:
        assert exc.code == "INVALID_REQUEST", (exc.code, str(exc))
        assert "invalid_recovery_links" in str(exc), str(exc)
        assert not (target / "knowledge.db").exists(), "a refused import publishes nothing"
        return
    # Alternative correct behaviour: the restore is accepted but the
    # unrecovered failure is still visible. Silently clearing it is the bug.
    restored = Store(report["target_db"])
    pipeline = _pipelines(restored)["export"]
    assert pipeline["failed"] >= 1, (
        "an unprovable earlier-success recovery must not clear the failure: "
        f"{pipeline}"
    )
    assert pipeline["last_error_code"] is not None, pipeline
