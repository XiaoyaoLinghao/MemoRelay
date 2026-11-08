"""External trial scheduler: deliver new work first, feed historical evidence in bounded groups.
No memory source is deleted or given a fabricated delivery receipt.
"""
from pathlib import Path
import argparse,concurrent.futures,datetime,fcntl,hashlib,io,json,os,sqlite3,sys,threading,time,urllib.request
HOLD="9999-01-01T00:00:00Z"
def release_history(config,store):
 policy=config.get("history_backfill",{})
 if not policy.get("enabled"):return {"released_groups":0,"reason":"paused"}
 if policy.get("storage")!="kw_jobs":raise ValueError("HISTORY_QUEUE_CONFIGURATION_REQUIRED")
 with store.read() as conn:
  # Held cold jobs remain visible to KW coverage accounting, but never block new work.
  active=conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='extract' AND state IN ('pending','retry_wait','running') AND COALESCE(run_after,'')!=?",(HOLD,)).fetchone()[0]
  embedding_active=conn.execute("SELECT COUNT(*) FROM jobs WHERE kind='embed' AND state IN ('pending','retry_wait','running')").fetchone()[0]
  held={row[0] for row in conn.execute("SELECT input_key FROM jobs WHERE kind='extract' AND state='pending' AND attempts=0 AND run_after=?",(HOLD,))}
 if active>=policy.get("max_active_before_release",4):return {"released_groups":0,"reason":"new_or_retry_work_first","active":active}
 if embedding_active>=policy.get("max_embedding_backlog",128):return {"released_groups":0,"reason":"index_catchup_first","embedding_active":embedding_active}
 planfile=Path(policy["plan"]);plan=json.loads(planfile.read_text());selected=[]
 for entry in plan["entries"]:
  if entry.get("supplement_after_native_review"):continue
  if entry.get("priority",0)>policy.get("max_priority",99):continue
  key=entry["event_id"]+"@"+str(entry["revision"])
  if key in held:
   selected.append((entry,key))
   if len(selected)>=policy.get("release_per_cycle",8):break
 released=[];moment=datetime.datetime.now(datetime.timezone.utc).isoformat()
 # One backend transaction releases complete revision jobs. No payload/cursor/ACK edits.
 with store.transaction() as conn:
  for entry,key in selected:
   n=conn.execute("UPDATE jobs SET run_after=NULL,updated_at=? WHERE kind='extract' AND input_key=? AND state='pending' AND attempts=0 AND run_after=?",(moment,key,HOLD)).rowcount
   if n:entry["admitted_at"]=moment;released.append(key)
 if released:
  pending=planfile.with_suffix(".pending")
  with pending.open("w") as stream:json.dump(plan,stream,indent=2);stream.write("\n");stream.flush();os.fsync(stream.fileno())
  pending.chmod(0o600);os.replace(pending,planfile)
 return {"released_groups":len(released),"active_before":active,"remaining_held_groups":len(held)-len(released),"storage":"kw_jobs"}
def obsolete_embedding_result(store,result):
 if result.get("state")!="stale" or result.get("error")!="STALE_CURSOR" or not result.get("job_id"):return False
 with store.read() as conn:
  row=conn.execute("SELECT r.status,a.detail FROM jobs j JOIN claim_revisions r ON r.revision_id=j.input_key LEFT JOIN job_attempts a ON a.job_id=j.job_id AND a.attempt=j.attempts WHERE j.job_id=? AND j.kind='embed'",(result["job_id"],)).fetchone()
 return bool(row and row[0] in ("superseded","expired","revoked") and str(row[1] or "").startswith("generation fence rejected"))

def cycle(config):
 from knowledge_weaver.store import Store
 from knowledge_weaver.jobs import status_counts,recover
 from knowledge_weaver.extraction import HttpExtractor,run_worker_once
 from knowledge_weaver.extraction import http_client
 from knowledge_weaver.embeddings.http_provider import HttpEmbeddingProvider
 from knowledge_weaver.embeddings.index import configure_model,run_embed_worker_once
 root=Path(config["state_dir"]);report={"ok":True,"at":datetime.datetime.now(datetime.timezone.utc).isoformat()}
 with (root/"trial-driver.lock").open("a") as lock:
  try:fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return {"ok":True,"idle":"another_cycle_running"}
  store=Store(config["db"],initialize=False);report["lease_recovery"]=recover(store)
  sys.path.insert(0,config["release_root"]);import operations
  quiet=dict(config,extraction_jobs_per_cycle=0,embedding_jobs_per_cycle=0)
  first=operations.cycle(quiet);report["initial_delivery"]=first["adapters"]
  if not first["ok"]:return dict(report,ok=False,error_code="DELIVERY_NOT_READY")
  report["history"]=release_history(config,store)
  if report["history"]["released_groups"]:
   second=operations.cycle(quiet)
   if not second["ok"]:return dict(report,ok=False,error_code="HISTORY_DELIVERY_NOT_READY")
  model=config["model"];key=json.loads(Path(model["credential_file"]).read_text())
  for part in model["credential_json_path"]:key=key[part]
  envname="UNIFIED_MEMORY_TRIAL_LLM_KEY";old=os.environ.get(envname);os.environ[envname]=key
  extractor=HttpExtractor(model["url"],model=model["name"],api_key_env=envname,timeout=model.get("timeout",150),thinking=False)
  original=urllib.request.urlopen;ledger_lock=threading.Lock()
  def audited(request,*a,**kw):
   if getattr(request,"full_url","")!=model["url"]:return original(request,*a,**kw)
   request=with_output_budget(request,model.get("max_output_tokens"))
   from model_affinity import with_model_affinity
   request=with_model_affinity(request)
   t=time.monotonic();record={"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"billing_outcome":"unknown","input_request_bytes":len(request.data or b"")}
   try:
    request_data=json.loads(request.data);context=json.loads(request_data["messages"][1]["content"])["context"];record.update(event_id=context["event_id"],event_revision=context["event_revision"],prompt_version=context["prompt_version"])
    with original(request,*a,**kw) as response:data=response.read();record["http_status"]=response.status
    payload=json.loads(data);record.update(response_metadata(payload));record.update(usage=payload.get("usage"),model=payload.get("model"),billing_outcome="provider_usage_reported" if payload.get("usage") else "unknown");return io.BytesIO(data)
   except Exception as exc:
    record["exception_class"]=type(exc).__name__
    if isinstance(exc,urllib.error.HTTPError):record["http_status"]=exc.code
    raise
   finally:
    record["seconds"]=time.monotonic()-t
    with ledger_lock:
     date=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
     with (root/"logs"/("model-usage-"+date+".jsonl")).open("a") as f:f.write(json.dumps(record,ensure_ascii=False)+"\n")
  http_client.urllib.request.urlopen=audited
  from trial_diagnostics import DiagnosticExtractor,with_output_budget,response_metadata
  from knowledge_weaver import contracts
  extractor=DiagnosticExtractor(extractor,contracts.validate_extraction_output,root/"logs"/"extraction-diagnostics.jsonl")
  report["extract"]=[]
  try:
   workers=max(1,min(4,int(config.get("extraction_concurrency",2))))
   budget=max(0,int(config.get("extraction_jobs_per_cycle",12)))
   def work(n):return run_worker_once(store,extractor,worker_id="worker:trial-"+str(n),lease_seconds=max(240,int(model.get("timeout",150))+30))
   with concurrent.futures.ThreadPoolExecutor(workers) as pool:
    for offset in range(0,budget,workers):
     results=list(pool.map(work,range(offset,min(offset+workers,budget))));report["extract"].extend(results)
     failures=[x for x in results if x.get("claimed") and x.get("state")!="succeeded"]
     if failures:
      report["ok"]=False
      # A bounded semantic rejection must not stop unrelated ready work.
      # Provider/unknown failures still stop this cycle to avoid outage churn.
      if any(x.get("failure_category") not in ("field_identity_collision","model_review_required") for x in failures):break
     if not any(x.get("claimed") for x in results):break
  finally:
   http_client.urllib.request.urlopen=original
   if old is None:os.environ.pop(envname,None)
   else:os.environ[envname]=old
  embedding=config.get("embedding")
  report["embed"]=[]
  if embedding:
   provider=HttpEmbeddingProvider(embedding["url"],model_id=embedding["model"],model_revision=embedding["revision"],dimension=embedding["dimension"]);configure_model(provider.model_id,model_revision=provider.model_revision,dimension=provider.dimension)
   for _ in range(config.get("embedding_jobs_per_cycle",32)):
    result=run_embed_worker_once(store,provider,worker_id="worker:trial-embedding");report["embed"].append(result)
    if not result["claimed"]:break
    if obsolete_embedding_result(store,result):
     result["disposition"]="obsolete_generation_skipped";continue
    if result["state"]!="succeeded":report["ok"]=False;break
  report["jobs"]=status_counts(store)
  temp=root/"trial-status.pending";temp.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n");temp.replace(root/"trial-status.json")
  return report
def main():
 parser=argparse.ArgumentParser();parser.add_argument("--config",required=True);args=parser.parse_args();os.umask(0o077)
 try:result=cycle(json.loads(Path(args.config).read_text()))
 except Exception as exc:result={"ok":False,"error_code":type(exc).__name__,"message":"Trial cycle failed; native sources and outboxes retained."}
 print(json.dumps(result,ensure_ascii=False));return 0 if result["ok"] else 1
if __name__=="__main__":raise SystemExit(main())
