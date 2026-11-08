"""Capture only an explicitly staged Hermes SQLite copy; no model/network call."""
import argparse,json
from pathlib import Path
from dma.adapters.hermes.reader import HermesSqliteReader
from dma.adapters.hermes.adapter import HermesAdapter
from dma.outbox import Outbox
from dma.pipeline import capture
from dma.redaction import Redactor
from dma.redaction.references import redactor_from_references
def main():
 p=argparse.ArgumentParser();p.add_argument("--config",required=True);a=p.parse_args();cfg=json.loads(Path(a.config).read_text());workspace=Path(cfg["workspace"]).resolve()
 def staged(value):
  x=Path(value).resolve()
  if workspace not in x.parents:raise ValueError("STAGED_PATH_REQUIRED")
  return x
 try:
  source=staged(cfg["staged_state_db"]);outbox=staged(cfg["outbox"])
  reader=HermesSqliteReader(source,instance_id=cfg["instance_id"],actor_id=cfg["actor_id"],profile=cfg["profile"],project_ids=tuple(cfg.get("project_ids",[])),tool_scope_since_id=cfg.get("tool_scope_since_id"))
  adapter=HermesAdapter(redactor_from_references(cfg.get('redaction_secret_refs',[])),workspace_id=cfg["workspace_id"],instance_id=cfg["instance_id"])
  result=capture(reader,Outbox(outbox,workspace_id=cfg["workspace_id"],instance_id=cfg["instance_id"]),stream="hermes:"+cfg["profile"],convert=adapter.to_events)
  print(json.dumps({"ok":True,"capture":result,"model_calls":0}));return 0
 except Exception as exc:
  print(json.dumps({"ok":False,"error":"STAGED_NATIVE_CAPTURE_FAILED","type":type(exc).__name__}));return 1
if __name__=="__main__":raise SystemExit(main())
