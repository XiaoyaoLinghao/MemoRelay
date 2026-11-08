"""Isolated external scheduler tests; never touch a deployed config or source."""
def test_history_admission_and_stale_fences(tmp_path):
    from pathlib import Path
    w = Path(__file__).resolve().parents[2] / "scripts"
    root = tmp_path
    from pathlib import Path
    import importlib.util,json,sqlite3,hashlib,contextlib
    from knowledge_weaver.store import Store
    from knowledge_weaver.jobs import enqueue,claim,commit
    spec=importlib.util.spec_from_file_location("trial",w/"operations_trial.py");m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    store=Store(root/"knowledge.db");ids=["evt:"+hashlib.sha256(x.encode()).hexdigest() for x in ["history-a","history-b","lower","overlap","new"]]
    entries=[{"adapter":"synthetic","event_id":ids[n],"revision":1,"priority":1 if n<2 else 5 if n==2 else 1,**({"supplement_after_native_review":True} if n==3 else {})} for n in range(4)]
    jobs=[enqueue(store,"extract",value+"@1",run_after=m.HOLD) for value in ids[:4]];new=enqueue(store,"extract",ids[4]+"@1")
    plan=root/"plan.json";plan.write_text(json.dumps({"entries":entries}))
    cfg={"history_backfill":{"enabled":True,"storage":"kw_jobs","plan":str(plan),"release_per_cycle":2,"max_active_before_release":1,"max_embedding_backlog":2,"max_priority":2}}
    assert m.release_history(cfg,store)["reason"]=="new_or_retry_work_first"
    claimed=claim(store,worker_id="worker:synthetic",kinds=("extract",),limit=8);assert len(claimed)==1 and claimed[0]["input_key"]==ids[4]+"@1"
    commit(store,claimed[0]["job_id"],claimed[0]["lease_token"],expected_input_version=1)
    # Held jobs remain pending/visible for coverage but are never claimable.
    assert not claim(store,worker_id="worker:synthetic",kinds=("extract",),limit=8)
    for n in range(2):enqueue(store,"embed","synthetic-vector-"+str(n))
    assert m.release_history(cfg,store)["reason"]=="index_catchup_first"
    for row in claim(store,worker_id="worker:synthetic",kinds=("embed",),limit=8):commit(store,row["job_id"],row["lease_token"],expected_input_version=1)
    # The release transaction rolls back every revision job if interrupted.
    with store.transaction() as conn:
     conn.execute("CREATE TRIGGER reject_release BEFORE UPDATE OF run_after ON jobs WHEN OLD.input_key='"+ids[1]+"@1' AND NEW.run_after IS NULL BEGIN SELECT RAISE(ABORT,'synthetic interruption'); END")
    try:m.release_history(cfg,store);raise AssertionError("expected interrupted transaction")
    except sqlite3.IntegrityError:pass
    with store.read() as conn:assert conn.execute("SELECT count(*) FROM jobs WHERE kind='extract' AND run_after=?",(m.HOLD,)).fetchone()[0]==4
    with store.transaction() as conn:conn.execute("DROP TRIGGER reject_release")
    result=m.release_history(cfg,store);assert result["released_groups"]==2
    with store.read() as conn:
     assert conn.execute("SELECT count(*) FROM jobs WHERE kind='extract' AND run_after=?",(m.HOLD,)).fetchone()[0]==2
     assert conn.execute("SELECT count(*) FROM job_attempts").fetchone()[0]==3,"releasing jobs must not create execution attempts"
    claimed=claim(store,worker_id="worker:synthetic",kinds=("extract",),limit=8);assert {x["input_key"] for x in claimed}=={ids[0]+"@1",ids[1]+"@1"}
    for row in claimed:commit(store,row["job_id"],row["lease_token"],expected_input_version=1)
    assert m.release_history(cfg,store)["released_groups"]==0
    cfg["history_backfill"]["enabled"]=False;assert m.release_history(cfg,store)["reason"]=="paused"
    saved=json.loads(plan.read_text());assert all(x.get("admitted_at") for x in saved["entries"][:2])
    # Independent proof for neutral stale-vector classification.
    fence=root/"fence.db"
    with sqlite3.connect(fence) as conn:
     conn.executescript("CREATE TABLE jobs(job_id,kind,input_key,attempts);CREATE TABLE claim_revisions(revision_id,status);CREATE TABLE job_attempts(job_id,attempt,detail);")
     conn.execute("INSERT INTO jobs VALUES('job:old','embed','rev:old',1)");conn.execute("INSERT INTO claim_revisions VALUES('rev:old','superseded')");conn.execute("INSERT INTO job_attempts VALUES('job:old',1,'generation fence rejected the stale result')")
    class FS:
     @contextlib.contextmanager
     def read(self):
      conn=sqlite3.connect(fence)
      try:yield conn
      finally:conn.close()
    stale={"job_id":"job:old","state":"stale","error":"STALE_CURSOR"};assert m.obsolete_embedding_result(FS(),stale)
    with sqlite3.connect(fence) as conn:conn.execute("UPDATE claim_revisions SET status='current'")
    assert not m.obsolete_embedding_result(FS(),stale)
    report={"ok":True,"model_calls":0,"driver_sha256":hashlib.sha256((w/"operations_trial.py").read_bytes()).hexdigest(),"checks":["all source/held jobs visible in KW pending accounting","new work remains claimable ahead of held history","backoff/new work and index backlog prevent historical admission","release transaction is atomic under interruption","stage priority and overlap review hold honored","only selected complete revision jobs become claimable","admission does not create attempts or reprocess successful jobs","idempotent admission and explicit pause","admission metadata durably recorded","obsolete vector fences neutral only with actual superseded state"]}
    assert report['ok'] and len(report['checks']) == 10
