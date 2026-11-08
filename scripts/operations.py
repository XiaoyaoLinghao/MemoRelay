"""External one-shot delivery/extraction/reconciliation; scheduling stays outside KW."""
import argparse,fcntl,json,os
from pathlib import Path
from knowledge_weaver.store import Store
from knowledge_weaver.extraction import HttpExtractor,CommandExtractor,run_worker_once
from knowledge_weaver.embeddings.http_provider import HttpEmbeddingProvider
from knowledge_weaver.embeddings.index import configure_model,run_embed_worker_once
from knowledge_weaver.jobs import status_counts
from dma.outbox import Outbox
from dma.delivery import DeliveryClient,deliver_once
from dma.reconcile import reconcile_after_restore

def cycle(config):
 root=Path(config['state_dir']);assert root.is_dir(),'state directory must be initialized explicitly'
 with (root/'operations.lock').open('a') as lock:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return {'ok':True,'idle':'another_cycle_running'}
  store=Store(config['db'],initialize=False);report={'ok':True,'adapters':{},'extract':[],'embed':[]}
  for item in config['adapters']:
   if not Path(item['outbox']).is_file():
    report['adapters'][item['name']]={'state':'awaiting_first_native_capture'};continue
   box=Outbox(item['outbox'],workspace_id=config['workspace_id'],instance_id=item['instance_id']);client=DeliveryClient(config['socket'],item['client_id'])
   try:
    recovery=reconcile_after_restore(box,client)
    deliveries=[deliver_once(box,client) for _ in range(config.get('delivery_rounds',4))]
    report['adapters'][item['name']]={'recovery_ok':recovery['ok'],'gaps':len(recovery.get('gaps',[])),'deliveries':deliveries,'stats':box.stats()}
    if not recovery['ok']:report['ok']=False
   except Exception as exc:
    report['adapters'][item['name']]={'state':'unavailable','error_code':type(exc).__name__};report['ok']=False
  model=config['model'];key_env='UNIFIED_MEMORY_LLM_KEY';old=os.environ.get(key_env)
  try:
   if model.get('command'):
    extractor=CommandExtractor(model['command'],timeout=model.get('timeout',150))
   else:
    # The reviewed existing gateway credential stays in process memory; neither
    # argv, model payloads, exported memory nor operation reports contain it.
    key=json.loads(Path(model['credential_file']).read_text())
    for part in model['credential_json_path']:key=key[part]
    if not isinstance(key,str) or not key:raise ValueError('credential unavailable')
    os.environ[key_env]=key
    extractor=HttpExtractor(model['url'],model=model['name'],api_key_env=key_env,timeout=model.get('timeout',150),thinking=False)
   for _ in range(config.get('extraction_jobs_per_cycle',2)):
    result=run_worker_once(store,extractor,worker_id='worker:local-operations',lease_seconds=max(240,int(model.get('timeout',150))+30));report['extract'].append(result)
    if not result['claimed']:break
    if result['state']!='succeeded':report['ok']=False;break
  finally:
   if old is None:os.environ.pop(key_env,None)
   else:os.environ[key_env]=old
  embedding=config.get('embedding')
  if embedding:
   provider=HttpEmbeddingProvider(embedding['url'],model_id=embedding['model'],model_revision=embedding['revision'],dimension=embedding['dimension']);configure_model(provider.model_id,model_revision=provider.model_revision,dimension=provider.dimension)
   for _ in range(config.get('embedding_jobs_per_cycle',8)):
    result=run_embed_worker_once(store,provider,worker_id='worker:local-embedding');report['embed'].append(result)
    if not result['claimed']:break
    if result['state']!='succeeded':report['ok']=False;break
  report['jobs']=status_counts(store)
  return report

def main():
 p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args()
 try:report=cycle(json.loads(Path(a.config).read_text()))
 except Exception as exc:report={'ok':False,'error_code':type(exc).__name__,'message':'Operation failed; original source/outbox retained. Inspect scoped status before retry.'}
 print(json.dumps(report,ensure_ascii=False));return 0 if report['ok'] else 1
if __name__=='__main__':raise SystemExit(main())
