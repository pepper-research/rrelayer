#!/usr/bin/env python3
"""Real processes, PostgreSQL and Anvil. No remote RPC or funded wallet access."""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import hashlib
import time
import uuid
import lab


def activate(ctx, name, expected=None, bootstrap=False):
    code, body = lab.http('POST', ctx.rr[name]['api']+'/handoff/activate',
                          {'expected_release':expected,'bootstrap':bootstrap}, auth=True)
    assert code == 200, (code, body)


def send(ctx, name, value, key=None):
    key = key or str(uuid.uuid4())
    body = {'to':lab.SINK,'value':hex(value),'data':'0x','externalId':key}
    started = time.monotonic();attempts=[]
    for _ in range(100):
        code, result = lab.http('POST', ctx.rr[name]['api']+f'/transactions/relayers/{lab.CHAIN_ID}/send-idempotent', body, auth=True)
        attempts.append(code)
        if code == 200:
            return {**result,'value':value,'key':key,'acceptedAt':time.time(),'httpStatuses':attempts,'ms':round((time.monotonic()-started)*1000,2)}
        if code not in (500,503): raise AssertionError((code,result))
        time.sleep(.05)
    raise AssertionError(('admission unavailable',code,result))


def done(ctx, txs, seconds=45):
    assert lab.wait_until(lambda: all(ctx.rows().get(t['id'],{}).get('status') in lab.OK_STATUSES for t in txs),seconds,.25), ctx.rows()


def check_money(ctx, txs):
    chain = ctx.chain_scan()
    counts = lab.payload_counts(chain)
    for tx in txs:
        assert counts.get(tx['value']) == 1, (tx, chain)
        row = ctx.rows()[tx['id']]
        assert row['status'] in lab.OK_STATUSES, row
        mined = [t for t in chain if t['value']==tx['value'] and t['to'].lower()==lab.SINK.lower()]
        assert row['nonce']==mined[0]['nonce'] and row['hash']==mined[0]['hash'], (row,mined)
    balance = int(lab.rpc(ctx.anvil_url,'eth_getBalance',[lab.SINK,'latest']),16)
    assert balance == sum(t['value'] for t in txs), (balance,txs)
    return {'recipientBalanceWei':balance,'rows':ctx.rows(),'chain':chain}


def startup(ctx, legacy=False):
    binary = ctx.old_bin if legacy else ctx.new_bin
    ctx.start_rr('old',binary,lab.PORTS['api'])
    # Production has pre-existing relayers. Seed that state, then restart to load it.
    # Current production's dynamic-create path recursively locks TransactionsQueues;
    # it is outside handoff scope and must not distort this deployment fixture.
    ctx.stop_rr('old')
    ctx.relayer_id = str(uuid.uuid4())
    ctx.relayer = lab.cast('wallet','address','--mnemonic',lab.MNEMONIC,'--mnemonic-index','0')
    ctx.pg.psql(ctx.dbname, f"INSERT INTO relayer.record(id,name,chain_id,address,wallet_index) VALUES('{ctx.relayer_id}','lab',{lab.CHAIN_ID},decode('{ctx.relayer[2:]}','hex'),0)")
    ctx.lab('/lab/sender',{'address':ctx.relayer})
    ctx.start_rr('old',binary,lab.PORTS['api'])
    if not legacy: activate(ctx,'old',bootstrap=True)


def warm_handoff(ctx):
    startup(ctx)
    ctx.lab('/lab/hide_receipts',{'seconds':4})
    txs = [send(ctx,'old',11001)]
    ctx.lab('/lab/hold',{'on':True})
    txs += [send(ctx,'old',11002)]
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    code,ready=lab.http('GET',ctx.rr['new']['api']+'/ready')
    assert code==200 and ready['durableIntakeReady'] and not ready['processing'], ready
    # A standby accepts a durable request and the current release discovers it.
    txs += [send(ctx,'new',11003)]
    repeat=send(ctx,'new',11003,txs[-1]['key'])
    assert repeat['id']==txs[-1]['id']
    start=time.monotonic()
    activate(ctx,'new','old')
    ctx.lab('/lab/hold',{'on':False})
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        txs += list(pool.map(lambda i:send(ctx,'new' if i%2 else 'old',11100+i),range(12)))
    done(ctx,txs)
    elapsed=time.monotonic()-start
    result=check_money(ctx,txs)
    attempts=ctx.proxy_sends()
    dispatch=[]
    for tx in txs:
        first=min(e['t'] for e in attempts if e.get('value')==tx['value'])
        dispatch.append(max(0,round((first-tx['acceptedAt'])*1000,2)))
    result.update({'handoffThroughDrainSeconds':round(elapsed,3),'maxAdmissionMs':max(t['ms'] for t in txs),'requests':len(txs),'httpStatuses':[status for tx in txs for status in tx['httpStatuses']],'maxAcceptedToFirstRpcAttemptMs':max(dispatch)})
    # Compatible rollback prewarms the old release, then atomically gives it work.
    activate(ctx,'old','new')
    txs += [send(ctx,'new',11999)]
    done(ctx,txs)
    check_money(ctx,txs)
    return result


def lost_response(ctx):
    startup(ctx)
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':8,'stall_seconds':30})
    tx = send(ctx,'old',22001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    ctx.kill_rr('old')
    activate(ctx,'new','old')
    repeat=send(ctx,'new',22001,tx['key'])
    assert repeat['id']==tx['id']
    tail=send(ctx,'new',22002)
    done(ctx,[tx,tail])
    return check_money(ctx,[tx,tail])


def stale_revival(ctx):
    startup(ctx)
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':8,'stall_seconds':30})
    tx=send(ctx,'old',33001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    proc=ctx.rr['old']['proc']
    os.killpg(proc.pid,signal.SIGSTOP)
    try:
        # Terminate only the connection holding this test wallet's advisory lock.
        killed=ctx.pg.psql(ctx.dbname,"SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted AND pid<>pg_backend_pid()",tuples_only=True)
        assert 't' in killed
        activate(ctx,'new','old')
        tail=send(ctx,'new',33002)
        done(ctx,[tx,tail])
    finally:
        os.killpg(proc.pid,signal.SIGCONT)
    time.sleep(4)
    return check_money(ctx,[tx,tail])


def failed_startup(ctx):
    startup(ctx)
    try:
        ctx.start_rr('broken',ctx.new_bin,lab.PORTS['api_b'],extra_network_yaml=f"\n  automatic_top_up:\n    - from:\n        relayer:\n          address: {ctx.relayer}\n      relayers: '*'\n")
        raise AssertionError('Unfenced automatic producer started')
    except RuntimeError: pass
    assert 'UnfencedAutomaticTopUp' in (Path(ctx.sdir)/'rrelayer-broken.log').read_text()
    # An activation aimed at a release that did not start cannot be sent. An
    # incorrect expected epoch is rejected without changing the existing owner.
    code,_=lab.http('POST',ctx.rr['old']['api']+'/handoff/activate',{'expected_release':'missing','bootstrap':False},auth=True)
    assert code != 200
    tx=send(ctx,'old',44001);done(ctx,[tx])
    return check_money(ctx,[tx])


def kill_barrier(ctx, point):
    directory=Path(ctx.sdir)/'barriers';directory.mkdir()
    os.environ['RRELAYER_TEST_POINT']=point
    os.environ['RRELAYER_TEST_BARRIER_DIR']=str(directory)
    try: startup(ctx)
    finally:
        os.environ.pop('RRELAYER_TEST_POINT');os.environ.pop('RRELAYER_TEST_BARRIER_DIR')
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    tx=send(ctx,'old',55001)
    assert lab.wait_until(lambda:(directory/point).exists(),15), point
    ctx.kill_rr('old')
    activate(ctx,'new','old')
    repeat=send(ctx,'new',55001,tx['key']);assert repeat['id']==tx['id']
    done(ctx,[tx])
    return check_money(ctx,[tx])


def replacement_cancel(ctx):
    startup(ctx)
    ctx.lab('/lab/hold',{'on':True})
    tx=send(ctx,'old',66001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state')['held_count']>0,10)
    # Attempted pending payload edits are rejected, including from a standby.
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    def change(method,path,body=None):
        for _ in range(100):
            code,result=lab.http(method,ctx.rr['new']['api']+path,body,auth=True)
            if code!=503:return code,result
            time.sleep(.05)
        raise AssertionError((code,result))
    code,reason=change('PUT','/transactions/replace/'+tx['id'],{'to':lab.OTHER_SINK,'value':hex(66002),'data':'0x'})
    assert code==400 and "already attempted broadcast" in str(reason),(code,reason)
    code,result=change('PUT','/transactions/cancel/'+tx['id'])
    assert code==200 and result['cancelTransactionId'],(code,result)
    cancel=result['cancelTransactionId']
    code,again=change('PUT','/transactions/cancel/'+tx['id'])
    assert code==200 and again['cancelTransactionId']==cancel,(code,again)
    ctx.kill_rr('old');activate(ctx,'new','old')
    ctx.lab('/lab/hold',{'on':False})
    assert lab.wait_until(lambda:ctx.rows().get(cancel,{}).get('status') in lab.OK_STATUSES,30),ctx.rows()
    rows=ctx.rows();assert rows[tx['id']]['status']=='CANCELLED',rows
    assert rows[tx['id']]['nonce']==rows[cancel]['nonce']==0
    assert int(lab.rpc(ctx.anvil_url,'eth_getBalance',[lab.SINK,'latest']),16)==0
    # A subsequent transaction must use nonce 1, never renumber the cancelled one.
    tail=send(ctx,'new',66003);done(ctx,[tail]);result=check_money(ctx,[tail]);result['cancelled']=rows
    return result


def bootstrap_refuses_pending(ctx):
    startup(ctx)
    # Create a durable pending row then model legacy migration metadata (fence
    # disabled). Activation must not reinterpret unknown legacy state as unsent.
    ctx.lab('/lab/hold',{'on':True});tx=send(ctx,'old',77001)
    ctx.stop_rr('old')
    ctx.pg.psql(ctx.dbname,"UPDATE relayer.sender_deployment SET enabled=FALSE,active_release=NULL")
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    code,_=lab.http('POST',ctx.rr['new']['api']+'/handoff/activate',{'expected_release':None,'bootstrap':True},auth=True)
    assert code!=200
    time.sleep(1)
    assert ctx.rows()[tx['id']]['status']=='PENDING'
    assert ctx.chain_scan()==[]
    return {'bootstrapRefused':True,'rows':ctx.rows()}


def database_reconnect(ctx):
    startup(ctx)
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':8,'stall_seconds':30})
    tx=send(ctx,'old',88001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    subprocess.check_call([ctx.pg.pg('pg_ctl'),'-D',ctx.pg.data,'-m','immediate','-w','stop'],stdout=subprocess.DEVNULL,env=ctx.pg.ENV)
    time.sleep(2)
    subprocess.check_call([ctx.pg.pg('pg_ctl'),'-D',ctx.pg.data,'-l',ctx.pg.log,'-w','-o',f"-p {lab.PORTS['pg']} -c listen_addresses=127.0.0.1 -c unix_socket_directories=''",'start'],stdout=subprocess.DEVNULL,env=ctx.pg.ENV)
    assert lab.wait_until(lambda:lab.http('GET',ctx.rr['new']['api']+'/ready')[0]==200,30)
    activate(ctx,'new','old')
    repeated=send(ctx,'new',88001,tx['key']);assert repeated['id']==tx['id']
    tail=send(ctx,'new',88002);done(ctx,[tx,tail]);return check_money(ctx,[tx,tail])


def abandoned_session(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':20,'stall_seconds':30})
    tx=send(ctx,'old',99001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    proc=ctx.rr['old']['proc'];os.killpg(proc.pid,signal.SIGSTOP)
    started=time.monotonic()
    try:
        activate(ctx,'new','old')
        # PostgreSQL must expire the paused owner's session without manual termination.
        done(ctx,[tx],40)
    finally:os.killpg(proc.pid,signal.SIGCONT)
    time.sleep(4)
    result=check_money(ctx,[tx]);result['recoverySeconds']=round(time.monotonic()-started,3);return result


def legacy_control(ctx):
    assert ctx.old_bin, '--baseline required'
    startup(ctx,legacy=True)
    ctx.lab('/lab/arm',{'mode':'nonce_too_low','hide_seconds':12})
    tx=ctx.submit('old');assert tx['id'],tx
    assert lab.wait_until(lambda:len(ctx.chain_scan())>=2,30),ctx.rows()
    counts=lab.payload_counts(ctx.chain_scan())
    assert counts.get(tx['value'],0)>1,counts
    return {'expectedUnsafeControl':True,'rows':ctx.rows(),'chain':ctx.chain_scan(),'logs':lab.log_counts(ctx.sdir)}


def legacy_bootstrap(ctx):
    assert ctx.old_bin, '--baseline required'
    startup(ctx,legacy=True)
    ctx.lab('/lab/hold',{'on':True});tx=ctx.submit('old');assert tx['id'],tx
    ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    code,_=lab.http('POST',ctx.rr['new']['api']+'/handoff/activate',{'expected_release':None,'bootstrap':True},auth=True)
    assert code!=200
    ctx.lab('/lab/hold',{'on':False})
    assert lab.wait_until(lambda:ctx.rows()[tx['id']]['status']=='CONFIRMED',30),ctx.rows()
    ctx.stop_rr('old');activate(ctx,'new',bootstrap=True)
    tail=send(ctx,'new',123001);done(ctx,[tail]);return check_money(ctx,[tx,tail])


def accepted_nonce_error(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/arm',{'mode':'nonce_too_low','hide_seconds':8})
    tx=send(ctx,'old',141001)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    activate(ctx,'new','old');tail=send(ctx,'new',141002)
    done(ctx,[tx,tail]);return check_money(ctx,[tx,tail])


def pending_edit_and_expiry(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    # No active worker: exercise genuinely unattempted durable admissions.
    ctx.pg.psql(ctx.dbname,"UPDATE relayer.sender_deployment SET active_release='standby'")
    tx=send(ctx,'new',151001)
    code,result=lab.http('PUT',ctx.rr['old']['api']+'/transactions/replace/'+tx['id'],{'to':lab.SINK,'value':hex(151002),'data':'0x'},auth=True)
    assert code==200 and result['replaceTransactionId']==tx['id'],(code,result)
    # Idempotence binds to the first accepted request, not the edited payload.
    assert send(ctx,'old',151001,tx['key'])['id']==tx['id']
    expired=send(ctx,'new',151003)
    ctx.pg.psql(ctx.dbname,f"SELECT set_config('rrelayer.sender_token',(SELECT token FROM relayer.sender_owner LIMIT 1),false); UPDATE relayer.transaction SET expires_at=NOW()-interval '1 hour' WHERE id='{expired['id']}'")
    ctx.pg.psql(ctx.dbname,"UPDATE relayer.record SET paused=TRUE")
    for path,body in [('cancel',None),('replace',{'to':lab.SINK,'value':'0x1','data':'0x'})]:
        code,_=lab.http('PUT',ctx.rr['old']['api']+'/transactions/'+path+'/'+tx['id'],body,auth=True);assert code==403,code
    ctx.pg.psql(ctx.dbname,"UPDATE relayer.record SET paused=FALSE")
    activate(ctx,'new','standby');tx['value']=151002
    done(ctx,[tx]);assert lab.wait_until(lambda:ctx.rows()[expired['id']]['status']=='EXPIRED',30),ctx.rows()
    tail=send(ctx,'old',151004);done(ctx,[tail]);return check_money(ctx,[tx,tail])


def original_wins_cancel(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    ctx.lab('/lab/hide_receipts',{'seconds':8});tx=send(ctx,'old',161001)
    assert lab.wait_until(lambda:ctx.rows()[tx['id']]['status']=='INMEMPOOL',10)
    assert lab.wait_until(lambda:len(ctx.chain_scan())==1,10)
    code,result=lab.http('PUT',ctx.rr['new']['api']+'/transactions/cancel/'+tx['id'],auth=True)
    assert code==200,(code,result)
    ctx.kill_rr('old');activate(ctx,'new','old');done(ctx,[tx]);rows=ctx.rows()
    assert rows[result['cancelTransactionId']]['status']=='DROPPED',rows
    return check_money(ctx,[tx])


def inmem_replacement(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    lab.rpc(ctx.anvil_url,'evm_setAutomine',[False]);lab.rpc(ctx.anvil_url,'anvil_setIntervalMining',[0])
    tx=send(ctx,'old',171001)
    assert lab.wait_until(lambda:ctx.rows()[tx['id']]['status']=='INMEMPOOL',10)
    code,result=lab.http('PUT',ctx.rr['new']['api']+'/transactions/replace/'+tx['id'],{'to':lab.SINK,'value':hex(171002),'data':'0x'},auth=True)
    assert code==200,(code,result)
    replacement={'id':result['replaceTransactionId'],'value':171002}
    ctx.kill_rr('old');activate(ctx,'new','old')
    assert lab.wait_until(lambda:ctx.rows()[replacement['id']]['status']=='INMEMPOOL',15),ctx.rows()
    lab.rpc(ctx.anvil_url,'evm_mine',[]);done(ctx,[replacement]);rows=ctx.rows()
    assert rows[tx['id']]['status']=='REPLACED' and rows[tx['id']]['nonce']==rows[replacement['id']]['nonce']==0,rows
    return check_money(ctx,[replacement])


def reverted_receipt(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    lab.rpc(ctx.anvil_url,'evm_setAutomine',[False]);lab.rpc(ctx.anvil_url,'anvil_setIntervalMining',[0]);tx=send(ctx,'old',181001)
    assert lab.wait_until(lambda:ctx.rows()[tx['id']]['status']=='INMEMPOOL',10)
    lab.rpc(ctx.anvil_url,'anvil_setCode',[lab.SINK,'0x60006000fd'])
    ctx.kill_rr('old');activate(ctx,'new','old');lab.rpc(ctx.anvil_url,'evm_mine',[])
    assert lab.wait_until(lambda:ctx.rows()[tx['id']]['status']=='FAILED',15),ctx.rows()
    rows=ctx.rows();receipt=lab.rpc(ctx.anvil_url,'eth_getTransactionReceipt',[rows[tx['id']]['hash']]);assert int(receipt['status'],16)==0
    assert int(lab.rpc(ctx.anvil_url,'eth_getBalance',[lab.SINK,'latest']),16)==0
    lab.rpc(ctx.anvil_url,'anvil_setCode',[lab.SINK,'0x']);lab.rpc(ctx.anvil_url,'evm_setAutomine',[True])
    tail=send(ctx,'new',181002);done(ctx,[tail]);assert ctx.rows()[tail['id']]['nonce']==1
    # Reverted transactions appear on chain but do not credit their value.
    assert int(lab.rpc(ctx.anvil_url,'eth_getBalance',[lab.SINK,'latest']),16)==tail['value']
    return {'rows':ctx.rows(),'chain':ctx.chain_scan(),'recipientBalanceWei':tail['value']}


def authorization_handoff(ctx):
    startup(ctx);ctx.start_rr('new',ctx.new_bin,lab.PORTS['api_b'])
    # Cast emits a signed EIP-7702 authorization as RLP. Decode its public fields.
    signed=lab.cast('wallet','sign-auth','0x0000000000000000000000000000000000000000','--mnemonic',lab.MNEMONIC,'--mnemonic-index','1','--chain',str(lab.CHAIN_ID),'--nonce','0')
    decoded=json.loads(lab.cast('from-rlp',signed))
    auth={'chainId':int(decoded[0],16),'address':decoded[1],'nonce':int(decoded[2][2:] or '0',16),'yParity':int(decoded[3][2:] or '0',16),'r':'0x'+decoded[4][2:].zfill(64),'s':'0x'+decoded[5][2:].zfill(64)}
    body={'to':lab.SINK,'value':hex(191001),'data':'0x','externalId':str(uuid.uuid4()),'authorizationList':[auth]}
    ctx.lab('/lab/arm',{'mode':'stall','hide_seconds':5,'stall_seconds':30})
    path=f'/transactions/relayers/{lab.CHAIN_ID}/send-idempotent'
    code,result=lab.http('POST',ctx.rr['new']['api']+path,body,auth=True);assert code==200,(code,result)
    assert lab.wait_until(lambda:ctx.lab('/lab/state').get('fault'),10)
    ctx.kill_rr('old');activate(ctx,'new','old')
    code,repeated=lab.http('POST',ctx.rr['new']['api']+path,body,auth=True);assert code==200 and repeated['id']==result['id'],(code,repeated)
    changed={**body,'authorizationList':[{**auth,'nonce':1}]}
    code,mismatch=lab.http('POST',ctx.rr['new']['api']+path,changed,auth=True);assert code==400 and 'already bound to a different transaction' in str(mismatch),(code,mismatch)
    tx={'id':result['id'],'value':191001};done(ctx,[tx]);result=check_money(ctx,[tx])
    authority=lab.cast('wallet','address','--mnemonic',lab.MNEMONIC,'--mnemonic-index','1')
    assert int(lab.rpc(ctx.anvil_url,'eth_getTransactionCount',[authority,'latest']),16)==1
    return result


SCENARIOS={'authorization_handoff':authorization_handoff,'inmem_replacement':inmem_replacement,'reverted_receipt':reverted_receipt,'accepted_nonce_error':accepted_nonce_error,'pending_edit_and_expiry':pending_edit_and_expiry,'original_wins_cancel':original_wins_cancel,'database_reconnect':database_reconnect,'abandoned_session':abandoned_session,'warm_handoff':warm_handoff,'lost_response':lost_response,'stale_revival':stale_revival,'failed_startup':failed_startup,'replacement_cancel':replacement_cancel,'bootstrap_refuses_pending':bootstrap_refuses_pending,**{p:(lambda ctx,point=p:kill_barrier(ctx,point)) for p in ['before_attempt_commit','after_attempt_commit','before_broadcast','after_broadcast']}}

LEGACY={'legacy_control':legacy_control,'legacy_bootstrap':legacy_bootstrap}

def main():
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--baseline');p.add_argument('--out',required=True);p.add_argument('--scenarios',default=','.join(SCENARIOS));args=p.parse_args();SCENARIOS.update(LEGACY)
    out=Path(args.out).resolve();out.mkdir(parents=True,exist_ok=True)
    pg=lab.Postgres(str(out));results=[]
    try:
        pg.start()
        for name in args.scenarios.split(','):
            sdir=out/name;sdir.mkdir(exist_ok=True)
            ctx=lab.Ctx(pg,str(sdir),'handoff_'+name,str(Path(args.binary).resolve()),args.baseline,lab.TIP_WEI)
            try:
                ctx.start_infra();result=SCENARIOS[name](ctx)
                results.append({'scenario':name,'pass':True,'binarySha256':hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),**result})
            except Exception as error:
                results.append({'scenario':name,'pass':False,'error':str(error)})
                raise
            finally:
                ctx.db_dump(str(sdir/'transactions.tsv')) if ctx.rr else None
                ctx.stop_all()
                (out/'verdict.json').write_text(json.dumps(results,indent=2))
                print(json.dumps({k:v for k,v in results[-1].items() if k not in ('rows','chain','cancelled')}),flush=True)
    finally:
        pg.stop()

if __name__=='__main__': main()
