#!/usr/bin/env python3
"""Restricted Base on real local processes, using the unchanged API gateway code.

No funded/live acceptance. API budget persistence is replaced by a deterministic
reservation store; its real meter, signer guard and response handling still run.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import uuid

os.environ['LAB_CHAIN_ID'] = '8453'
import lab
from test_handoff import startup


def run(binary, snapshot, out):
    out.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get('LAB_GATEWAY_PORT', '18547'))
    assert lab.port_free(port), 'Gateway port already owned'
    gateway = f'http://127.0.0.1:{port}'
    signer = lab.cast('wallet', 'address', '--mnemonic', lab.MNEMONIC, '--mnemonic-index', '0')
    os.environ.update(RRELAYER_LIMIT_RPC_SIGNER_ADDRESS=signer,
                      RRELAYER_LIMIT_RPC_GATEWAY_URL=gateway+'/internal/orders/base-rpc',
                      RRELAYER_LIMIT_RPC_GATEWAY_ENABLED='true')
    pg = lab.Postgres(str(out))
    ctx = None
    worker = None
    result = {'binarySha256': hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
              'apiGatewaySha256': hashlib.sha256((snapshot/'src/orders/base-rpc-gateway.ts').read_bytes()).hexdigest(),
              'scope': 'local chain8453; real gateway/guard/meter; simulated budget store; no live acceptance'}
    try:
        pg.start()
        ctx = lab.Ctx(pg, str(out), 'restricted_base', binary, None, lab.TIP_WEI)
        ctx.start_infra()
        env = dict(os.environ, HANDOFF_API_SNAPSHOT=str(snapshot), LAB_GATEWAY_PORT=str(port),
                   LAB_GATEWAY_UPSTREAM=ctx.proxy_url, RELAYER_USER=lab.AUTH_USER, RELAYER_PASS=lab.AUTH_PASS,
                   LIMIT_ORDER_BASE_RELAYER_ADDRESS=signer,
                   DATABASE_URL=f'postgres://postgres@127.0.0.1:{lab.PORTS["pg"]}/{ctx.dbname}')
        worker = lab.spawn('base-gateway', ['bun', str(Path(__file__).with_name('base-gateway-worker.mjs'))],
                           str(out/'gateway.log'), env=env, cwd=str(out))
        assert lab.wait_until(lambda: not lab.port_free(port) or worker.poll() is not None, 20)
        assert worker.poll() is None, 'Gateway failed; inspect gateway.log'
        state = lambda: lab.http('GET', gateway+'/lab/state')[1]
        mode = lambda name: lab.http('POST', gateway+'/lab/mode', {'mode': name})
        rpc = {'jsonrpc': '2.0', 'id': 1, 'method': 'eth_chainId', 'params': []}
        assert lab.http('POST', gateway+'/internal/orders/base-rpc', rpc)[0] == 401
        for raw in lab.http('GET', gateway+'/lab/envelopes')[1].values():
            assert lab.http('POST', gateway+'/internal/orders/base-rpc',
                            {**rpc, 'method': 'eth_sendRawTransaction', 'params': [raw]}, auth=True)[0] == 400
        assert not state()['sends']
        result['gatewayNegativeEnvelopes'] = True
        startup(ctx)
        assert lab.http('GET', ctx.rr['old']['api']+'/ready')[1]['queuesReady']
        result['controlledRestrictedQueueReady'] = True
        ctx.stop_rr('old')
        os.environ['RRELAYER_LIMIT_RPC_SIGNER_ADDRESS'] = lab.cast(
            'wallet', 'address', '--mnemonic', lab.MNEMONIC, '--mnemonic-index', '1')
        ctx.start_rr('old', binary, lab.PORTS['api'])
        assert not lab.http('GET', ctx.rr['old']['api']+'/ready')[1]['queuesReady']
        assert not state()['sends']
        ctx.stop_rr('old')
        os.environ['RRELAYER_LIMIT_RPC_SIGNER_ADDRESS'] = signer
        ctx.start_rr('old', binary, lab.PORTS['api'])
        assert lab.http('GET', ctx.rr['old']['api']+'/ready')[1]['queuesReady']
        result['mismatchedSignerStaysOffline'] = True

        def envelope(tid):
            return pg.psql(ctx.dbname, f"SELECT encode(signed_envelope,'hex') FROM relayer.transaction WHERE id='{tid}'", tuples_only=True).strip()

        def submit(tag):
            body = {'to': lab.SINK, 'value': '0x0', 'data': '0x'+tag, 'externalId': str(uuid.uuid4())}
            code, response = lab.http('POST', ctx.rr['old']['api']+f'/transactions/relayers/{ctx.relayer_id}/send', body, auth=True)
            assert code == 200, (code, response)
            return response

        # Pre-forward budget rejection cannot fall back to the ordinary provider.
        mode('reject')
        first = submit('01')
        assert lab.wait_until(lambda: state()['denied'] > 0, 30)
        raw = envelope(first['id'])
        assert raw and not ctx.chain_scan() and not state()['sends']
        for route in ('cancel', 'replace'):
            code, _ = lab.http('PUT', ctx.rr['old']['api']+f'/transactions/{route}/{first["id"]}',
                               {'to': lab.SINK, 'value': '0x0', 'data': '0x'}, auth=True)
            assert code == 401, (route, code)
        code, _ = lab.http('POST', ctx.rr['old']['api']+'/transactions/relayers/8453/send-random',
                           {'to': lab.SINK, 'value': '0x0', 'data': '0x'}, auth=True)
        assert code == 400, code
        result['routeRestrictionsAndNoFallback'] = True
        ctx.kill_rr('old')
        mode('normal')
        ctx.start_rr('old', binary, lab.PORTS['api'])
        assert lab.wait_until(lambda: ctx.rows()[first['id']]['status'] in lab.OK_STATUSES, 60)
        assert envelope(first['id']) == raw
        assert len(ctx.chain_scan()) == 1
        assert all(value == '0x'+raw for value in state()['sends'])
        result['preForwardRestartPreservesEnvelope'] = True

        # The gateway forwards once but loses its response. Hide the original
        # hash long enough to kill the sender, then recover by that same hash.
        ctx.lab('/lab/arm', {'mode': 'swallow', 'hide_seconds': 8})
        mode('uncertain')
        second = submit('02')
        assert lab.wait_until(lambda: len(state()['sends']) >= 2, 30)
        second_raw = envelope(second['id'])
        ctx.kill_rr('old')
        ctx.lab('/lab/hold', {'on': False})
        ctx.start_rr('old', binary, lab.PORTS['api'])
        assert lab.wait_until(lambda: ctx.rows()[second['id']]['status'] in lab.OK_STATUSES, 60)
        assert envelope(second['id']) == second_raw
        chain = ctx.chain_scan()
        assert len(chain) == 2 and [tx['nonce'] for tx in chain] == [0, 1], chain
        assert set(state()['sends']) == {'0x'+raw, '0x'+second_raw}
        result['uncertainRestartNoDuplicate'] = True

        # A configured gateway cannot redirect the relayer to another route.
        mode('redirect')
        code, _ = lab.http('GET', ctx.rr['old']['api']+f'/transactions/status/{first["id"]}', auth=True)
        assert code == 500, code
        assert state()['redirectHits'] == 0
        result['redirectRefused'] = True
        mode('normal')
        result['chain'] = chain
        result['passed'] = True
    finally:
        if ctx: ctx.stop_all()
        lab.stop_proc(worker)
        pg.stop()
        lab.write_json(str(out/'result.json'), result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--api-snapshot', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    run(str(Path(args.binary).resolve()), Path(args.api_snapshot).resolve(), Path(args.out).resolve())
