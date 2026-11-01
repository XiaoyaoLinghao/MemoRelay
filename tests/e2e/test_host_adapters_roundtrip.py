"""M3 e2e: real Hermes/DSH adapter fixtures -> durable outbox -> KW service.

Synthetic fixtures only; no user profile, no model call, Unix socket only.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

from dma.adapters.dsh.adapter import DshAdapter
from dma.adapters.dsh.reader import DshSessionReader
from dma.adapters.hermes.adapter import HermesAdapter
from dma.adapters.hermes.reader import HermesSqliteReader
from dma.delivery import DeliveryClient, deliver_once
from dma.outbox import Outbox
from dma.pipeline import capture
from dma.redaction import Redactor

from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, register_client

WORKSPACE = "owner:demo"
HERMES_INSTANCE = "instance:hermes-test"
DSH_INSTANCE = "instance:dsh-test"
HERMES_CLIENT = "client:hermes-main"
DSH_CLIENT = "client:dsh-main"


@pytest.fixture()
def kw(tmp_path):
    store = Store(tmp_path / "kw" / "knowledge.db")
    for client_id, instance in ((HERMES_CLIENT, HERMES_INSTANCE), (DSH_CLIENT, DSH_INSTANCE)):
        register_client(
            store,
            client_id=client_id,
            workspace_id=WORKSPACE,
            host="hermes" if client_id == HERMES_CLIENT else "dsh",
            instance_id=instance,
            actor_ids=["*"],
            project_ids=["project:songta"],
        )
    socket_path = Path("/tmp") / f"umkw-m3-{time.time_ns() % 10**9}.sock"
    app = ServiceApp(store, socket_path).start()
    try:
        yield store, app
    finally:
        app.stop()


def _deliver(outbox, app, client_id):
    return deliver_once(outbox, DeliveryClient(app.socket_path, client_id=client_id, timeout=3.0))


def _make_hermes_db(path: Path, now: float) -> Path:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT, "
        "timestamp REAL, active INTEGER, compacted INTEGER, _compressed_summary INTEGER, display_kind TEXT)"
    )
    connection.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, parent_session_id TEXT, end_reason TEXT)")
    connection.execute("INSERT INTO sessions VALUES ('session:root', NULL, 'compression')")
    rows = [
        (1, "session:root", "user", json.dumps("松塔项目端口改为4200", ensure_ascii=False), now - 100, 1, 0, 0, None),
        (2, "session:root", "tool", json.dumps("exit code 0; listener 4200", ensure_ascii=False), now - 90, 1, 0, 0, None),
        (3, "session:root", "assistant", json.dumps("已记录4200", ensure_ascii=False), now - 80, 1, 0, 0, None),
    ]
    for row in rows:
        connection.execute("INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    connection.commit()
    connection.close()
    return path


def test_hermes_adapter_roundtrip(kw, tmp_path):
    store, app = kw
    db = _make_hermes_db(tmp_path / "state.db", time.time())
    reader = HermesSqliteReader(db, instance_id=HERMES_INSTANCE, profile="main")
    adapter = HermesAdapter(Redactor(), workspace_id=WORKSPACE, instance_id=HERMES_INSTANCE)
    outbox = Outbox(
        tmp_path / "dma-hermes" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=HERMES_INSTANCE,
    )
    captured = capture(reader, outbox, stream="hermes:main", convert=adapter.to_events)
    assert captured["inserted"] == 3
    result = _deliver(outbox, app, HERMES_CLIENT)
    assert result["accepted"] == 3
    assert outbox.stats()["by_state"]["delivered"] == 3
    with store.read() as conn:
        rows = conn.execute(
            "SELECT e.role, e.origin_kind, r.lifecycle FROM events e "
            "JOIN event_revisions r ON r.event_id = e.event_id ORDER BY e.created_at, e.role"
        ).fetchall()
    assert {(row["role"], row["origin_kind"]) for row in rows} == {
        ("user", "user_input"),
        ("assistant", "assistant_reply"),
        ("tool", "tool_execution"),
    }


def _make_dsh_log(path: Path, now_ms: int) -> Path:
    events = [
        {"type": "turn/start", "seq": 0, "time": now_ms, "data": {"turn": 1}},
        {"type": "user/message", "seq": 1, "time": now_ms + 1000, "data": {"turn": 1, "content": "松塔项目端口改为4200"}},
        {"type": "assistant/message", "seq": 2, "time": now_ms + 2000, "data": {"turn": 1, "step": 1, "message": {"content": [{"type": "text", "text": "已记录4200"}]}}},
        {"type": "tool/result", "seq": 3, "time": now_ms + 3000, "data": {"turn": 1, "step": 1, "message": {"content": [{"type": "text", "text": "listener 4200"}], "toolCallId": "call:1"}}},
        {"type": "runtime-context", "seq": 4, "time": now_ms + 4000, "data": {"text": "injected context"}},
        {"type": "system/message", "seq": 5, "time": now_ms + 4500, "data": {"turn": 1, "step": 1, "message": {"content": "You are DSH."}}},
        {"type": "turn/end", "seq": 6, "time": now_ms + 5000, "data": {"turn": 1, "reason": {"kind": "completed"}}},
    ]
    path.write_text("\n".join(json.dumps(event, ensure_ascii=False) for event in events) + "\n", encoding="utf-8")
    return path


def test_dsh_adapter_roundtrip_with_exclusions(kw, tmp_path):
    store, app = kw
    log = _make_dsh_log(tmp_path / "session.jsonl", int(time.time() * 1000))
    reader = DshSessionReader(log, instance_id=DSH_INSTANCE, session_id="session:dsh-e2e")
    adapter = DshAdapter(Redactor(), workspace_id=WORKSPACE, instance_id=DSH_INSTANCE)
    outbox = Outbox(
        tmp_path / "dma-dsh" / "outbox.db",
        workspace_id=WORKSPACE,
        instance_id=DSH_INSTANCE,
    )
    captured = capture(reader, outbox, stream="dsh:session:dsh-e2e", convert=adapter.to_events)
    assert captured["inserted"] == 4  # user, assistant, tool, runtime-context; system skipped
    result = _deliver(outbox, app, DSH_CLIENT)
    assert result["accepted"] == 3
    assert result["excluded"] == 1
    with store.read() as conn:
        events = conn.execute(
            "SELECT e.role, e.origin_kind, r.lifecycle FROM events e "
            "JOIN event_revisions r ON r.event_id = e.event_id ORDER BY e.created_at, e.role"
        ).fetchall()
        parts = conn.execute("SELECT COUNT(*) AS c FROM event_parts").fetchone()["c"]
    origins = {(row["role"], row["origin_kind"]) for row in events}
    assert ("user", "user_input") in origins
    assert ("tool", "tool_execution") in origins
    assert ("user", "memory_injection") in origins
    assert ("system", "host_system") not in origins  # system prompts are never ingested
    assert parts == 4
