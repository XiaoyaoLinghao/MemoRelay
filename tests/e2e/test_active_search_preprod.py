"""Actual UDS read-only active search, shared API/identity/age and pagination."""
import json
import tempfile
import shutil
from pathlib import Path
from knowledge_weaver.store import Store,register_client,receive_batch
from knowledge_weaver.store.claims import record_claim_revision
from knowledge_weaver.service import ServiceApp
from dma.integrations.recall_client import RecallClient
from dma.integrations.search import ReadOnlySearch
from dma.integrations.hermes import DmaMemoryProvider
from knowledge_weaver import contracts


def event(i,text):
 return {"schema_version":"1.0","event_id":contracts.canonical_event_id("owner:demo","instance:search",f"msg:{i}"),"event_revision":1,"workspace_id":"owner:demo","operation":"create","source":{"instance_id":"instance:search","host":"hermes","actor_id":"search-test","session_id":"s:test","native_event_id":f"msg:{i}","native_schema":"synthetic-search","role":"user","origin_kind":"user_input","locator":f"hermes://search/{i}"},"event_time":{"value":f"2026-10-{i:02d}T00:00:00Z","precision":"instant","timezone":"UTC"},"body":{"text":text,"sha256":contracts.body_digest(text),"part_index":0,"part_count":1,"content_revision":"native:1","format":"text/plain"},"redaction":{"policy_version":"redact:1","status":"passed","replacements":0,"credential_refs":[]},"project_ids":["project:songta"],"parent_revision":None,"derived_from":[]}


def test_active_search_paginates_history_without_changing_state_or_scope(tmp_path):
 store=Store(tmp_path/"kw/knowledge.db")
 register_client(store,client_id="client:search",workspace_id="owner:demo",host="hermes",instance_id="instance:search",actor_ids=["*"],project_ids=["project:songta"])
 for i,port in [(1,4317),(2,4328)]:
  e=event(i,f"松塔搜索项目端口为{port}");receive_batch(store,{"schema_version":"1.0","batch_id":f"batch:{i}","events":[e]})
  record_claim_revision(store,workspace_id="owner:demo",project_id="project:songta",subject_id="subject:search-server",predicate="server.port",facet="desired",qualifiers={},value=port,display_text=e["body"]["text"],authority="user_confirmed",assertion_kind="explicit_statement",event_time=e["event_time"],evidence=[{"event_id":e["event_id"],"event_revision":1,"part_index":0,"start_byte":0,"end_byte":len(e["body"]["text"].encode()),"body_sha256":e["body"]["sha256"],"availability":"full"}])
 def facts():
  with store.read() as c:return [tuple(v) for v in c.execute("SELECT revision_id,value_json,event_time_value,status FROM claim_revisions ORDER BY revision_id")]
 before=facts();watermark=store.meta()["watermark"]
 root=Path(tempfile.mkdtemp(prefix="umsearch-",dir="/tmp"));app=ServiceApp(store,root/"kw.sock").start()
 try:
  search=ReadOnlySearch(RecallClient(app.socket_path,"client:search"),project_ids=("project:songta",))
  pages=[];args={"query":"松塔搜索端口","intent":"history","limit":1}
  first=search.call(args,session_id="s:search");assert first["ok"] and first["next_cursor"]
  second=search.call(dict(args,cursor=first["next_cursor"]),session_id="s:search")
  assert second["ok"]
  items=first["items"]+second["items"]
  assert {v["value"] for v in items}=={4317,4328}
  assert {v["state"] for v in items}=={"current","superseded"}
  assert all(v["sources"] and v["source_actors"]==["search-test"] and v["event_time"]["value"].startswith("2026-10-") for v in items)
  p=DmaMemoryProvider(recall_client=search.client,client_id="client:search",project_ids=("project:songta",),enable_search_tool=True);p.initialize("s:provider")
  result=json.loads(p.handle_tool_call("unified_memory_search",{"query":"松塔搜索端口"}))
  assert [v["value"] for v in result["items"]]==[4328]
  assert "4328" in p.prefetch("松塔搜索端口")
  assert facts()==before and store.meta()["watermark"]==watermark
 finally:app.stop();shutil.rmtree(root)
