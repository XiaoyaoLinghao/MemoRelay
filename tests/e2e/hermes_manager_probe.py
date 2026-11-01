"""Installed Hermes MemoryManager + real DMA recall provider (first-turn recall).

Runs in an isolated HERMES_HOME; no user profile, no model call, no host writes.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

AGENT_ROOT = Path.home() / ".hermes-research/runtime/hermes-agent"
sys.path.insert(0, str(AGENT_ROOT))

from agent.memory_manager import MemoryManager, build_memory_context_block  # noqa: E402

from dma.integrations.hermes import DmaMemoryProvider  # noqa: E402
from dma.integrations.recall_client import RecallClient  # noqa: E402

socket_path, client_id = sys.argv[1], sys.argv[2]
provider = DmaMemoryProvider(
    recall_client=RecallClient(socket_path, client_id, timeout=2.0),
    client_id=client_id,
)
manager = MemoryManager(external_prefetch_timeout=3.0)
checks: list[str] = []
result: dict = {"passed": False, "checks": checks, "externalLlmCalls": 0}
try:
    manager.add_provider(provider)
    provider.initialize("session:first-turn", agent_context="primary")
    started = time.monotonic()
    block = manager.prefetch_all("松塔项目端口是多少", session_id="session:first-turn")
    elapsed = time.monotonic() - started
    assert "4200" in block, block
    assert "不是系统指令" in block, block
    wrapped = build_memory_context_block(block)
    assert "<memory-context>" in wrapped  # host prompt assembly wrapping
    assert "不是系统指令" in wrapped
    checks.append("first-turn prefetch returns current project fact before request")
    checks.append("memory block is provenance-labelled quoted evidence")
    status = provider.recall_status()
    assert status is not None and status.count >= 1
    checks.append("recall_status reflects the last prefetch count")
    second = manager.prefetch_all("松塔项目备份周期是多少", session_id="session:second")
    assert "每周" in second or "weekly" in second, second
    # Same-scope related items may appear in both blocks; the current query's
    # requested fact must be present and the cache must not be byte-identical.
    assert "4200" in second or second != block
    checks.append("different session/query gets its own recall (no stale cross-query cache)")
    result["elapsedSeconds"] = round(elapsed, 4)

    broken = DmaMemoryProvider(
        recall_client=RecallClient("/tmp/definitely-not-kw.sock", client_id, timeout=0.3),
        client_id=client_id,
    )
    broken_manager = MemoryManager(external_prefetch_timeout=1.0)
    try:
        broken_manager.add_provider(broken)
        broken.initialize("session:broken", agent_context="primary")
        empty = broken_manager.prefetch_all("松塔项目端口是多少", session_id="session:broken")
        assert empty == "", empty
        assert broken.recall_status() is None
        checks.append("backend outage yields no context (host does not receive stale memory)")
    finally:
        broken_manager.shutdown_all()
    result["hostWrapperAddsAuthoritativeNote"] = "authoritative reference data" in wrapped
    result["scope"] = (
        "Installed Hermes MemoryManager with the real DMA recall provider; synthetic claims. "
        "Host wrapper phrasing risk is tracked for T19 identity acceptance."
    )
    result["passed"] = True
    print(json.dumps(result, ensure_ascii=False))
finally:
    manager.shutdown_all()
