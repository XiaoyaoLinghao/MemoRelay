import json,tempfile,shutil
from pathlib import Path
from knowledge_weaver.store import Store,register_client,receive_batch
from knowledge_weaver.store.claims import record_claim_revision
from knowledge_weaver.service import ServiceApp
from dma.reader import NativeRecord
from dma.adapters.base import NativeMessageAdapter
from dma.redaction import Redactor
from dma.integrations.hermes import DmaMemoryProvider
from dma.integrations.recall_client import RecallClient


def seed(store):
 adapter=NativeMessageAdapter(Redactor(),workspace_id='owner:demo',instance_id='instance:scope',host='hermes')
 for i,project,text,value,predicate in [(1,'project:imac','iMac项目服务端口为4311',4311,'server.port'),(2,'project:fnos','fnOS项目服务端口为5422',5422,'server.port'),(3,None,'用户全局汇报偏好是简洁中文','简洁中文','report.preference')]:
  event=adapter.to_events(NativeRecord(native_event_id=f'msg:{i}',text=text,role='user',origin_kind='user_input',session_id='s:scope',actor_id='actor:scope',native_schema='synthetic-scopes',content_revision='native:1',locator=f'hermes://scope/{i}',event_time={'value':f'2026-10-0{i}T01:00:00Z','precision':'instant','timezone':'UTC'},project_ids=(project,) if project else (),aliases=()))[0]
  receive_batch(store,{'schema_version':'1.0','batch_id':f'batch:scope:{i}','events':[event]})
  record_claim_revision(store,workspace_id='owner:demo',project_id=project,subject_id=f'subject:scope:{i}',predicate=predicate,facet='desired',qualifiers={},value=value,display_text=text,authority='user_confirmed',assertion_kind='explicit_statement',event_time=event['event_time'],evidence=[{'event_id':event['event_id'],'event_revision':1,'part_index':0,'start_byte':0,'end_byte':len(text.encode()),'body_sha256':event['body']['sha256'],'availability':'full'}])


def test_native_provider_project_switch_candidates_and_global_preference(tmp_path):
 store=Store(tmp_path/'knowledge.db');register_client(store,client_id='client:scope',workspace_id='owner:demo',host='hermes',instance_id='instance:scope',actor_ids=['*'],project_ids=[]);seed(store)
 root=Path(tempfile.mkdtemp(prefix='umscope-',dir='/tmp'));app=ServiceApp(store,root/'kw.sock').start();watermark=store.meta()['watermark']
 try:
  provider=DmaMemoryProvider(recall_client=RecallClient(app.socket_path,'client:scope'),client_id='client:scope',enable_search_tool=True,query_project_aliases={'iMac':'project:imac','fnOS':'project:fnos'},allowed_project_ids=('project:imac','project:fnos'),allow_workspace_search=True);provider.initialize('session:test')
  a=provider.prefetch('iMac服务端口');b=provider.prefetch('fnOS服务端口');c=provider.prefetch('服务端口');d=provider.prefetch('iMac服务端口')
  assert '4311' in a and '5422' not in a and '5422' in b and '4311' not in b
  assert '尚未确定项目范围' in c and 'project:imac' in c and 'project:fnos' in c and '4311' not in c and '5422' not in c
  assert d==a
  selected=json.loads(provider.handle_tool_call('unified_memory_search',{'query':'服务端口','project_ids':['project:fnos']}));assert [v['value'] for v in selected['items']]==[5422]
  global_pref=json.loads(provider.handle_tool_call('unified_memory_search',{'query':'用户全局汇报偏好','intent':'all_projects'}));assert any(v['value']=='简洁中文' and v['project_id'] is None for v in global_pref['items'])
  assert store.meta()['watermark']==watermark
 finally:app.stop();shutil.rmtree(root)
