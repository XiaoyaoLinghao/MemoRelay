"""T16 e2e: v1 daily-memory derived import -> DMA outbox -> KW -> legacy_unknown claims."""
from __future__ import annotations

import time
from pathlib import Path

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.importers.daily_memory import DailyMemoryImporter
from dma.outbox import Outbox
from dma.redaction import Redactor

from knowledge_weaver.extraction import load_event_group, run_worker_once, sanitized_parts
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, current_items, register_client

WORKSPACE = "owner:demo"
INSTANCE = "instance:legacy-import"
CLIENT = "client:legacy-import"
DAILY = """---
title: "2026年05月22日 - 会话记忆"
date: "2026-05-22"
---

## 决策与结论
- 05:00 - 决定松塔项目端口改为4200
- 无时间条目的备份周期为每周
"""


class MockExtractor:
    def __init__(self, proposal):
        self.proposal = proposal

    def extract(self, parts, context):
        return self.proposal


@pytest.fixture()
def kw(tmp_path):
    store = Store(tmp_path / "kw" / "knowledge.db")
    register_client(
        store,
        client_id=CLIENT,
        workspace_id=WORKSPACE,
        host="legacy",
        instance_id=INSTANCE,
        actor_ids=["*"],
        project_ids=["project:songta"],
    )
    socket_path = Path("/tmp") / f"umkw-legacy-{time.time_ns() % 10**9}.sock"
    app = ServiceApp(store, socket_path).start()
    try:
        yield store, app
    finally:
        app.stop()


def test_daily_import_becomes_derived_events_and_legacy_unknown_claims(kw, tmp_path):
    store, app = kw
    memory = tmp_path / "memory"
    memory.mkdir()
    (memory / "2026-05-22.md").write_text(DAILY, encoding="utf-8")
    outbox = Outbox(
        tmp_path / "dma" / "outbox.db", workspace_id=WORKSPACE, instance_id=INSTANCE
    )
    importer = DailyMemoryImporter(
        workspace_id=WORKSPACE, instance_id=INSTANCE, default_project_id="project:songta"
    )
    report = importer.import_path(memory, outbox)
    assert report["imported_events"] == 2
    delivered = deliver_once(
        outbox, DeliveryClient(app.socket_path, client_id=CLIENT, timeout=3.0)
    )
    assert delivered["accepted"] == 2
    with store.read() as conn:
        origins = {
            row["origin_kind"] for row in conn.execute("SELECT origin_kind FROM events").fetchall()
        }
        event_ids = [row["event_id"] for row in conn.execute("SELECT event_id FROM events").fetchall()]
    assert origins == {"derived_import"}

    # KW extraction of derived history must not promote it to user-confirmed truth.
    group = load_event_group(store, event_ids[0], 1)
    part = sanitized_parts(group)[0]
    proposal = {
        "schema_version": "1.0",
        "claims": [
            {
                "subject_hint": "松塔项目服务器",
                "project_hint": "project:songta",
                "qualifiers": {},
                "predicate": "server.port",
                "facet": "desired",
                "value": 4200,
                "display_text": "决定松塔项目端口改为4200",
                "assertion_kind": "explicit_statement",
                "evidence": [
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
        ],
        "excluded_event_ids": [],
        "warnings": [],
    }
    result = run_worker_once(store, MockExtractor(proposal))
    assert result["state"] == "succeeded", result
    items = current_items(store, workspace_id=WORKSPACE, include_states=("current", "uncertain"))
    assert items
    assert items[0]["evidence_grade"] == "legacy_unknown"
    assert items[0]["state"] == "current"
