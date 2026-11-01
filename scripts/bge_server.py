"""Local CPU BGE-M3 endpoint; external deployment helper, no scheduler."""
import argparse,json,http.server,time,resource
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('--model-dir',required=True);p.add_argument('--port',type=int,required=True);a=p.parse_args()
 import numpy as np
 import onnxruntime as ort
 from tokenizers import Tokenizer
 root=Path(a.model_dir);options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1;options.graph_optimization_level=ort.GraphOptimizationLevel.ORT_ENABLE_BASIC
 session=ort.InferenceSession(str(root/'model.onnx'),sess_options=options,providers=['CPUExecutionProvider'])
 tok=Tokenizer.from_file(str(root/'tokenizer.json'));tok.enable_truncation(max_length=512);tok.enable_padding(pad_id=tok.token_to_id('<pad>'),pad_token='<pad>')
 count=0
 class Handler(http.server.BaseHTTPRequestHandler):
  def log_message(self,*args):pass
  def reply(self,code,body):
   data=json.dumps(body,allow_nan=False).encode();self.send_response(code);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
  def do_GET(self):
   if self.path!='/health':return self.reply(404,{'error':'NOT_FOUND'})
   self.reply(200,{'ok':True,'model':'BAAI/bge-m3','model_revision':'5617a9f61b028005a4858fdac845db406aefb181','dimension':1024,'maximum_tokens':512,'completed_requests':count,'peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
  def do_POST(self):
   nonlocal count
   if self.path!='/v1/embeddings':return self.reply(404,{'error':'NOT_FOUND'})
   try:
    n=int(self.headers.get('Content-Length','0'))
    if n<1 or n>262144:return self.reply(413,{'error':'REQUEST_TOO_LARGE'})
    body=json.loads(self.rfile.read(n));texts=body.get('input')
    if not isinstance(texts,list) or not 1<=len(texts)<=16 or not all(isinstance(t,str) and len(t)<=65536 for t in texts):return self.reply(400,{'error':'INVALID_INPUT'})
    enc=tok.encode_batch(texts);data={'input_ids':np.array([x.ids for x in enc],dtype=np.int64),'attention_mask':np.array([x.attention_mask for x in enc],dtype=np.int64),'token_type_ids':np.array([x.type_ids for x in enc],dtype=np.int64)}
    emb=session.run(None,{x.name:data[x.name] for x in session.get_inputs()})[0]
    if emb.ndim==3:emb=emb[:,0,:]
    if emb.shape!=(len(texts),1024):raise ValueError('unexpected shape')
    norms=np.linalg.norm(emb,axis=1,keepdims=True)
    if not np.all(np.isfinite(emb)) or not np.all(norms>0):raise ValueError('invalid embedding')
    emb=emb/norms;count+=1;self.reply(200,{'data':[{'index':i,'embedding':v} for i,v in enumerate(emb.tolist())]})
   except Exception:self.reply(503,{'error':'EMBEDDING_UNAVAILABLE'})
 # One bounded model inference at a time, no growing in-memory request history.
 server=http.server.HTTPServer(('127.0.0.1',a.port),Handler);print(json.dumps({'ready':True,'host':'127.0.0.1','port':server.server_port}),flush=True)
 try:server.serve_forever()
 except KeyboardInterrupt:pass
 finally:server.server_close()
if __name__=='__main__':main()
