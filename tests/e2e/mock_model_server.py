"""Recordable local model proxy for secret-outbound acceptance (AC-11/45).

Loopback-only HTTP mock for an OpenAI-compatible embeddings endpoint and a chat
completions extraction endpoint. It records every request body/header so tests
can scan the actual outbound payload for synthetic canaries.
"""
from __future__ import annotations

import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIMENSION = 8


def _vector(text: str) -> list[float]:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    values = [((byte / 255.0) * 2 - 1) for byte in digest[:DIMENSION]]
    norm = sum(value * value for value in values) ** 0.5 or 1.0
    return [value / norm for value in values]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: D401 - silence default logging
        return

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = {}
        server = self.server
        server.requests.append(
            {
                "path": self.path,
                "body": body,
                "raw": raw.decode("utf-8", "replace"),
                "authorization_present": "Authorization" in self.headers,
            }
        )
        if self.path.endswith("/embeddings"):
            texts = body.get("input") or []
            self._send(200, {"data": [{"embedding": _vector(str(text))} for text in texts]})
            return
        if self.path.endswith("/chat/completions"):
            messages = body.get("messages") or []
            payload_text = messages[-1].get("content") if messages else "{}"
            try:
                payload = json.loads(payload_text)
            except (TypeError, json.JSONDecodeError):
                payload = {}
            parts = payload.get("parts") or []
            if not parts:
                self._send(200, {"choices": [{"message": {"content": json.dumps({
                    "schema_version": "1.0", "claims": [], "excluded_event_ids": [], "warnings": []
                })}}]})
                return
            part = parts[0]
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
                        "display_text": "端口改为4200",
                        "assertion_kind": "explicit_statement",
                        "evidence": [
                            {
                                "event_id": part["event_id"],
                                "event_revision": part["event_revision"],
                                "part_index": part["part_index"],
                                "support_quote": part["text"],
                            }
                        ],
                        "target_claim_hint": None,
                        "effective_time_quote": None,
                    }
                ],
                "excluded_event_ids": [],
                "warnings": [],
            }
            self._send(200, {"choices": [{"message": {"content": json.dumps(proposal, ensure_ascii=False)}}]})
            return
        self._send(404, {"error": "unknown endpoint"})


class MockModelServer:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.requests = self.requests
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self.port = self._server.server_address[1]

    @property
    def embeddings_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/embeddings"

    @property
    def chat_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/chat/completions"

    def __enter__(self) -> "MockModelServer":
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
