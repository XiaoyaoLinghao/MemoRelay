from pathlib import Path
import tempfile,shutil
import pytest
from knowledge_weaver.store import Store,register_client
from knowledge_weaver.service import ServiceApp
from knowledge_weaver.lifecycle import register_lifecycle,sweep
from dma.integrations.recall_client import RecallClient,RecallUnavailable
from dma.integrations.search import ReadOnlySearch
import importlib.util
_spec=importlib.util.spec_from_file_location("scope_seed",Path(__file__).with_name("test_project_scopes_preprod.py"))
_fixture=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_fixture)
seed=_fixture.seed


def test_real_api_details_source_age_expiry_and_scope(tmp_path):
 store=Store(tmp_path/'knowledge.db');seed(store)
 register_client(store,client_id='client:source',workspace_id='owner:demo',host='hermes',instance_id='instance:scope',actor_ids=['*'],project_ids=['project:imac'])
 register_client(store,client_id='client:other',workspace_id='owner:demo',host='hermes',instance_id='instance:other',actor_ids=['*'],project_ids=['project:fnos'])
 root=Path(tempfile.mkdtemp(prefix='umsource-',dir='/tmp'));app=ServiceApp(store,root/'kw.sock').start()
 try:
  client=RecallClient(app.socket_path,'client:source');tool=ReadOnlySearch(client,project_ids=('project:imac',))
  before=store.meta()['watermark'];a=tool.call({'query':'iMac服务端口','include_source_details':True},session_id='s:source');assert a['ok'] and a['source_details_complete']
  detail=a['source_details'][0];assert detail['text']=='iMac项目服务端口为4311' and detail['source']['actor_id']=='actor:scope' and detail['event_time']['value']=='2026-10-01T01:00:00Z'
  assert a['items'][0]['value']==4311 and store.meta()['watermark']==before
  with pytest.raises(RecallUnavailable) as exc:RecallClient(app.socket_path,'client:other').fetch_source(detail['event_id'],1,0)
  assert exc.value.code=='SCOPE_DENIED'
  with pytest.raises(RecallUnavailable) as exc:client.fetch_source(detail['event_id'],2,0)
  assert exc.value.code=='SOURCE_NOT_FOUND'
  register_lifecycle(store,subject_kind='event_part',subject_id=detail['event_id']+'#1@0',class_='temporary_detail',business_anchor='2026-10-01T01:00:00Z')
  assert sweep(store,now='2026-11-01T12:00:00Z')['event_parts']==1
  before=store.meta()['watermark'];b=tool.call({'query':'iMac服务端口','include_source_details':True},session_id='s:source');assert b['items'][0]['value']==4311 and not b['source_details_complete']
  assert b['source_details'][0]['text'] is None and b['source_details'][0]['detail_status']=='expired' and b['source_details'][0]['event_time']==detail['event_time']
  assert store.meta()['watermark']==before
 finally:app.stop();shutil.rmtree(root)
