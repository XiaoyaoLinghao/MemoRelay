"""Metadata-only extractor diagnostics. Never logs source/model content."""
import datetime,json,re,threading
from pathlib import Path
_LOCK=threading.Lock()
_ALLOWED_FIELDS=frozenset("schema_version claims excluded_event_ids warnings claim_id facet assertion_kind project_hint subject_hint predicate value display_text qualifiers evidence event_id event_revision part_index support_quote effective_time_quote target_claim_hint confidence".split())
def issue_summary(issues):
 result=[]
 for issue in issues[:8]:
  # Do not preserve free-form keys, values or validator messages.
  fields=[x for x in re.findall(r"[A-Za-z_]+",str(issue.path)) if x in _ALLOWED_FIELDS]
  message=str(issue.message).lower()
  category=next((label for needle,label in [
   ("required","required"),("enum","enum"),("additional","additional_properties"),
   ("type","type"),("minimum","minimum"),("maximum","maximum"),("pattern","pattern")
  ] if needle in message),"validation")
  result.append({"fields":fields,"category":category})
 return {"issue_count":len(issues),"issues":result}
def emit(path,record):
 # A failed diagnostic sink must never change business processing.
 try:
  with _LOCK:
   with Path(path).open("a") as f:f.write(json.dumps({"at":datetime.datetime.now(datetime.timezone.utc).isoformat(),**record},sort_keys=True)+"\n")
 except OSError:pass
class DiagnosticExtractor:
 def __init__(self,delegate,validator,path):
  self.delegate=delegate;self.validator=validator;self.path=path
 def extract(self,parts,context):
  record={k:context.get(k) for k in ("event_id","event_revision","prompt_version")}
  try:proposal=self.delegate.extract(parts,context=context)
  except Exception as exc:
   message=str(getattr(exc,"message",""))
   category=next((label for prefix,label in [
    ("model endpoint unreachable","transport"),("model endpoint returned invalid JSON","response_json"),
    ("model response has no content","empty_content"),("model content is not a JSON proposal","proposal_json"),
    ("model proposal is not an object","proposal_type")
   ] if message.startswith(prefix)),"extractor_error")
   emit(self.path,{**record,"stage":category,"exception_class":type(exc).__name__})
   raise
  issues=self.validator(proposal)
  if issues:emit(self.path,{**record,"stage":"proposal_schema",**issue_summary(issues)})
  return proposal

def with_output_budget(request,budget):
 import urllib.request
 if budget is None:return request
 if type(budget) is not int or not 1024<=budget<=32768:raise ValueError("INVALID_OUTPUT_BUDGET")
 body=json.loads(request.data)
 body.setdefault("max_tokens",budget)
 return urllib.request.Request(request.full_url,data=json.dumps(body,ensure_ascii=False).encode("utf-8"),headers=dict(request.header_items()),method=request.get_method())
def response_metadata(payload):
 if not isinstance(payload,dict):return {}
 choices=payload.get("choices") or []
 if not choices or not isinstance(choices[0],dict):return {}
 choice=choices[0];reason=choice.get("finish_reason");message=choice.get("message") or {}
 out={"finish_reason":reason if reason in ("stop","length","content_filter","tool_calls",None) else "other"}
 if isinstance(message,dict) and isinstance(message.get("content"),str):out["output_content_bytes"]=len(message["content"].encode("utf-8"))
 return out
