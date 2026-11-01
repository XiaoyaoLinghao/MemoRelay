"""T11 e2e: real installed Hermes MemoryManager + DMA recall provider."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from dma.integrations.recall_client import RecallClient  # noqa: F401 - importable check

from knowledge_weaver import contracts
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.store import Store, receive_batch, register_client
from knowledge_weaver.store.claims import record_claim_revision

HERMES_AGENT = Path.home() / ".hermes-research/runtime/hermes-agent"
PROBE = Path(__file__).resolve().parent / "hermes_manager_probe.py"
pytestmark = pytest.mark.skipif(
    not HERMES_AGENT.is_dir(), reason="installed Hermes agent components unavailable"
)


def _event(native_id: str, text: str) -> dict:
    return {
        "schema_version": "1.0",
        "event_id": contracts.canonical_event_id("owner:demo", "instance:hermes-test", native_id),
        "event_revision": 1,
        "workspace_id": "owner:demo",
        "operation": "create",
        "source": {
            "instance_id": "instance:hermes-test",
            "host": "hermes",
            "actor_id": "jun-test",
            "session_id": "session:test",
            "native_event_id": native_id,
            "native_schema": "hermes-snapshot-20261030",
            "role": "user",
            "origin_kind": "user_input",
            "locator": f"hermes://session:test/{native_id}",
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


def _claim(store, event, value, predicate, text):
    receive_batch(store, {"schema_version": "1.0", "batch_id": f"batch:{event['event_id'][-8:]}", "events": [event]})
    record_claim_revision(
        store,
        workspace_id="owner:demo",
        project_id="project:songta",
        subject_id="subject:songta-server",
        predicate=predicate,
        facet="desired" if predicate == "server.port" else "decision",
        qualifiers={},
        value=value,
        display_text=text,
        authority="user_confirmed",
        assertion_kind="explicit_statement",
        event_time=event["event_time"],
        evidence=[
            {
                "event_id": event["event_id"],
                "event_revision": 1,
                "part_index": 0,
                "start_byte": 0,
                "end_byte": len(text.encode("utf-8")),
                "body_sha256": event["body"]["sha256"],
                "availability": "full",
            }
        ],
    )


def test_hermes_manager_first_turn_recall(tmp_path):
    store = Store(tmp_path / "kw" / "knowledge.db")
    register_client(
        store,
        client_id="client:hermes-main",
        workspace_id="owner:demo",
        host="hermes",
        instance_id="instance:hermes-test",
        actor_ids=["*"],
        project_ids=["project:songta"],
    )
    port_text = "松塔项目端口为4200"
    backup_text = "松塔项目备份周期为每周"
    _claim(store, _event("message:1", port_text), 4200, "server.port", port_text)
    _claim(store, _event("message:2", backup_text), "weekly", "backup_interval", backup_text)
    socket_dir = Path(tempfile.mkdtemp(prefix="umhm-", dir="/tmp"))
    socket_path = socket_dir / "kw.sock"
    app = ServiceApp(store, socket_path).start()
    hermes_home = tmp_path / "hermes-profile"
    hermes_home.mkdir()
    try:
        completed = subprocess.run(
            [sys.executable, str(PROBE), str(app.socket_path), "client:hermes-main"],
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "HERMES_HOME": str(hermes_home),
                "HERMES_RESEARCH_HOME": str(hermes_home),
            },
        )
        if completed.returncode != 0:
            pytest.fail(completed.stderr[-3000:] + completed.stdout[-3000:])
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
        assert payload["passed"] is True, payload
        assert payload["externalLlmCalls"] == 0
        assert len(payload["checks"]) >= 5
        # Record the host wrapper risk for T19 review instead of hiding it.
        assert "hostWrapperAddsAuthoritativeNote" in payload
    finally:
        app.stop()
        shutil.rmtree(socket_dir, ignore_errors=True)
