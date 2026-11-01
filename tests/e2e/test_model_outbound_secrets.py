"""AC-11/AC-45 isolated: recordable model proxy + all-pipeline canary scans.

Synthetic canaries only; the proxy is loopback-only and calls no external model.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from dma.delivery import DeliveryClient, deliver_once
from dma.event import build_message_events
from dma.outbox import Outbox
from dma.redaction import Redactor

from knowledge_weaver.embeddings.http_provider import HttpEmbeddingProvider
from knowledge_weaver.embeddings.index import configure_model, run_embed_worker_once
from knowledge_weaver.extraction import HttpExtractor, run_worker_once
from knowledge_weaver.portability import export_snapshot
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.service.client import ServiceClient
from knowledge_weaver.store import Store, current_items, register_client

from .mock_model_server import MockModelServer

WORKSPACE = "owner:demo"
INSTANCE = "instance:hermes-secret-e2e"
CLIENT = "client:hermes-secret-e2e"
CANARIES = [
    "CANARY-PW-7f3a",
    "CANARY-API-91b2c3d4e5f6g7h8",
    "CANARY-COOKIE-4d8e",
    "CANARY-PRIVATE-KEY-SYNTHETIC",
]
EVENT_TIME = {"value": "2026-10-30T01:00:00Z", "precision": "instant", "timezone": "UTC"}


def _source(native_id: str) -> dict:
    return {
        "instance_id": INSTANCE,
        "host": "hermes",
        "actor_id": "jun-test",
        "session_id": "session:secret",
        "native_event_id": native_id,
        "native_schema": "hermes-snapshot-20261030",
        "role": "user",
        "origin_kind": "user_input",
        "locator": f"hermes://session:secret/{native_id}",
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


def _socket() -> Path:
    return Path("/tmp") / f"umkw-secret-{time.time_ns() % 10**9}.sock"


def test_no_secret_reaches_model_proxy_or_storage(kw_env, tmp_path):
    store, app = kw_env
    outbox = Outbox(tmp_path / "dma" / "outbox.db", workspace_id=WORKSPACE, instance_id=INSTANCE)
    mixed = (
        "松塔项目端口改为4200；数据库密码: CANARY-PW-7f3a；"
        "API key sk-CANARY-API-91b2c3d4e5f6g7h8；"
        "Cookie: session=CANARY-COOKIE-4d8e"
    )
    private = "-----BEGIN PRIVATE KEY-----\nCANARY-PRIVATE-KEY-SYNTHETIC\n-----END PRIVATE KEY-----"
    events = _events("message:secret-1", mixed) + _events("message:secret-2", private)
    outbox.capture(events, stream="hermes:secret", position="1")
    delivered = deliver_once(outbox, DeliveryClient(app.socket_path, client_id=CLIENT, timeout=3.0))
    assert delivered["accepted"] == 1  # mixed message accepted after redaction
    assert delivered["withheld"] == 1  # private key is receipt-only withheld

    provider_model = "mock:embedding"
    configure_model(provider_model, model_revision="1", dimension=8)
    try:
        os.environ["MOCK_MODEL_KEY"] = "sk-test-invalid-not-a-real-credential"
        with MockModelServer() as proxy:
            extractor = HttpExtractor(proxy.chat_url, model="mock-extract", api_key_env="MOCK_MODEL_KEY")
            extraction = run_worker_once(store, extractor)
            assert extraction["state"] == "succeeded", extraction
            assert extraction["claims_written"] == 1
            embedding = HttpEmbeddingProvider(
                proxy.embeddings_url,
                model_id=provider_model,
                dimension=8,
                api_key_env="MOCK_MODEL_KEY",
            )
            for _ in range(3):
                if run_embed_worker_once(store, embedding)["claimed"] == 0:
                    break
            assert proxy.requests, "model proxy received no requests"
            outbound = "\n".join(request["raw"] for request in proxy.requests)
        assert "Authorization" not in json.dumps(proxy.requests[0]["body"])
    finally:
        configure_model(None)
        os.environ.pop("MOCK_MODEL_KEY", None)

    for canary in CANARIES:
        assert canary not in outbound, f"canary {canary!r} leaked to model outbound"
        assert canary.encode() not in outbox.db_path.read_bytes(), canary
        assert canary.encode() not in store.db_path.read_bytes(), canary
    snapshot = tmp_path / "snapshot"
    export_snapshot(store, snapshot)
    for path in snapshot.rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            for canary in CANARIES:
                assert canary.encode() not in data, f"{canary!r} leaked into {path.name}"
    items = current_items(store, workspace_id=WORKSPACE)
    assert items and items[0]["value"] == 4200
    assert items[0]["evidence_grade"] == "user_confirmed"
    # Only sanitized evidence reached the model proxy; credentials are sent only
    # as an Authorization header (never in the request body or stored content).
    assert all("Authorization" not in json.dumps(request["body"]) for request in proxy.requests)


@pytest.fixture()
def kw_env(tmp_path):
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
    app = ServiceApp(store, _socket()).start()
    try:
        yield store, app
    finally:
        app.stop()
