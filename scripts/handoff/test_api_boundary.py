#!/usr/bin/env python3
"""Real API journal -> two real relayers -> Anvil, through a >120s outcome gap."""
import os
os.environ['LAB_CHAIN_ID']='1'
import argparse,json,subprocess,time,hashlib
from pathlib import Path
import lab
from test_handoff import startup,activate,done,check_money
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--api-snapshot',required=True);p.add_argument('--out',required=True);a=p.parse_args()
out=Path(a.out).resolve();out.mkdir(parents=True,exist_ok=True);pg=lab.Postgres(str(out));ctx=None;worker=None
try:
 pg.start();ctx=lab.Ctx(pg,str(out),'relayer_api_boundary',a.binary,None,lab.TIP_WEI);ctx.start_infra();startup(ctx);ctx.start_rr('new',a.binary,lab.PORTS['api_b'])
 snapshot=Path(a.api_snapshot).resolve();dbname='api_boundary';pg.createdb(dbname)
 pg.psql(dbname,(snapshot/'test/pg/fixtures/base-schema.sql').read_text())
 env=dict(os.environ,DATABASE_URL=f'postgres://postgres@127.0.0.1:{lab.PORTS["pg"]}/{dbname}',HANDOFF_API_SNAPSHOT=str(snapshot),HANDOFF_API_PORT=os.environ.get('HANDOFF_API_PORT','13002'),RELAYER_URL=ctx.rr['new']['api'],RELAYER_USER=lab.AUTH_USER,RELAYER_PASS=lab.AUTH_PASS,SOLVER_PRIVATE_KEY=lab.cast('wallet','private-key','--mnemonic',lab.MNEMONIC,'--mnemonic-index','0'),NODE_ENV='production')
 for chain in [97,4114,5115,8453,1,10143,4663,56,137,143,42161,43113,80002,84532,421614,688688,688689,46630,11155111,11155420,123420001114]:env[f'RPC_URL_{chain}']=ctx.anvil_url
 here=Path(__file__).resolve().parent
 worker=lab.spawn('api-boundary',['node','--import',str(here/'egress-guard.mjs'),'--import',str(snapshot/'node_modules/tsx/dist/loader.mjs'),str(here/'api-boundary-worker.mjs')],str(out/'api-worker.log'),env=env,cwd=str(snapshot))
 url='http://127.0.0.1:'+env['HANDOFF_API_PORT']
 def healthy():
  try:return lab.http('GET',url+'/status')[0]==200
  except Exception:return False
 assert lab.wait_until(healthy,30), 'API worker startup'
 ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':135,'stall_seconds':30})
 started=time.monotonic();assert lab.http('POST',url+'/start',{})[0]==200
 assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),15)
 ctx.kill_rr('old');activate(ctx,'new','old')
 checks=[]
 for target in [5,125]:
  time.sleep(max(0,target-(time.monotonic()-started)))
  code,result=lab.http('POST',url+'/cancel',{});assert code==200 and result=={'unresolved':True,'cancelled':False},result
  checks.append({'elapsedSeconds':round(time.monotonic()-started,2),**result,'status':lab.http('GET',url+'/status')[1]})
 assert lab.wait_until(lambda:all(r['status'] in lab.OK_STATUSES for r in ctx.rows().values()),35),ctx.rows()
 code,first=lab.http('POST',url+'/repeat',{});assert code==200,first
 code,second=lab.http('POST',url+'/repeat',{});assert code==200 and second==first,second
 status=lab.http('GET',url+'/status')[1];assert len(status['journal'])==1 and status['journal'][0]['outcome']=='mined',status
 assert status['escrow']==[{'status':'blocked','spicenet_cancel_tx_hash':None}],status
 rows=ctx.rows();assert len(rows)==1;tx={'id':next(iter(rows)),'value':424242};result=check_money(ctx,[tx])
 result.update({'pass':True,'scope':'Actual API journal/client/refund guard; native payout, not full rollup settlement/indexer','checks':checks,'apiStatus':status,'repeat':first,'elapsedSeconds':round(time.monotonic()-started,2),'binarySha256':hashlib.sha256(Path(a.binary).read_bytes()).hexdigest()})
 (out/'verdict.json').write_text(json.dumps(result,indent=2));print(json.dumps({'pass':True,'elapsedSeconds':result['elapsedSeconds'],'recipientBalanceWei':result['recipientBalanceWei']}))
finally:
 if worker:lab.stop_proc(worker)
 if ctx:ctx.db_dump(str(out/'transactions.tsv'));ctx.stop_all()
 pg.stop()
