#!/usr/bin/env python3
"""Linux dev-host rehearsal through an isolated Nginx listener, not ECS/ALB."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time
import uuid

import lab
from test_handoff import activate, check_money, done, startup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get('HANDOFF_PROXY_PORT', '28762'))
    assert lab.port_free(port), 'Proxy port already in use'
    pg = lab.Postgres(str(out))
    ctx = proxy = None
    stop = threading.Event()
    requests, errors, events = [], [], []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        pg.start()
        ctx = lab.Ctx(pg, str(out), 'proxy_handoff', str(Path(args.binary).resolve()), None, lab.TIP_WEI)
        ctx.start_infra()
        startup(ctx)
        conf = out / 'nginx.conf'

        def configure(names):
            servers = '\n'.join(f'server 127.0.0.1:{lab.PORTS["api" if name == "old" else "api_b"]};' for name in names)
            conf.write_text(f'''pid {out}/nginx.pid;
error_log {out}/nginx-error.log info;
events {{ worker_connections 128; }}
http {{
  access_log {out}/nginx-access.log;
  client_body_temp_path {out}/client-temp;
  proxy_temp_path {out}/proxy-temp;
  upstream senders {{ {servers} }}
  server {{ listen 127.0.0.1:{port};
    location / {{ proxy_pass http://senders; proxy_next_upstream off; }}
  }}
}}
''')

        configure(['old'])
        proxy = lab.spawn('nginx', ['nginx', '-p', str(out), '-c', str(conf), '-g', 'daemon off;'], str(out/'nginx.log'))
        assert lab.wait_until(lambda: not lab.port_free(port), 5)
        assert proxy.poll() is None
        url = f'http://127.0.0.1:{port}'

        def traffic():
            index = 0
            while not stop.is_set():
                value = 210001 + index
                body = {'to': lab.SINK, 'value': hex(value), 'data': '0x', 'externalId': str(uuid.uuid4())}
                started = time.monotonic()
                try:
                    code, result = lab.http('POST', url+f'/transactions/relayers/{lab.CHAIN_ID}/send-idempotent', body, auth=True)
                    requests.append({'id': result.get('id') if isinstance(result, dict) else None, 'value': value,
                                     'status': code, 'ms': round((time.monotonic()-started)*1000, 2), 'acceptedAt': time.time()})
                    if code != 200:
                        errors.append({'status': code, 'body': result})
                except Exception as error:
                    errors.append({'error': str(error)})
                index += 1
                stop.wait(.15)

        def reload_and_drain(names):
            # Wait for this isolated master's old workers to drain before retiring
            # their backend. Never signal the host's shared Nginx service.
            workers = subprocess.check_output(['pgrep', '-P', str(proxy.pid)], text=True).split()
            configure(names)
            os.kill(proxy.pid, signal.SIGHUP)
            assert lab.wait_until(lambda: all(not Path(f'/proc/{pid}').exists() for pid in workers), 30), 'Nginx old workers did not drain'
            events.append({'event': 'routed', 'backends': names, 'time': time.time()})

        future = pool.submit(traffic)
        time.sleep(2)
        # Failed warmup must leave the old processing release available.
        try:
            ctx.start_rr('broken', ctx.new_bin, lab.PORTS['api_b'], f"\n  automatic_top_up:\n    - from:\n        relayer:\n          address: {ctx.relayer}\n      relayers: '*'\n")
            raise AssertionError('Unsupported producer unexpectedly started')
        except RuntimeError:
            assert 'UnfencedAutomaticTopUp' in (out/'rrelayer-broken.log').read_text()
        assert lab.http('GET', url+'/ready')[1]['activeRelease'] == 'old'
        events.append({'event': 'failed-warmup-old-still-active', 'time': time.time()})
        ctx.start_rr('new', ctx.new_bin, lab.PORTS['api_b'])
        reload_and_drain(['old', 'new'])
        lab.rpc(ctx.anvil_url, 'evm_setAutomine', [False])
        lab.rpc(ctx.anvil_url, 'anvil_setIntervalMining', [0])
        time.sleep(2)
        activate(ctx, 'new', 'old')
        events.append({'event': 'activated-new', 'time': time.time()})
        reload_and_drain(['new'])
        ctx.stop_rr('old')
        lab.rpc(ctx.anvil_url, 'evm_mine', [])
        lab.rpc(ctx.anvil_url, 'evm_setAutomine', [True])
        lab.rpc(ctx.anvil_url, 'anvil_setIntervalMining', [1])
        time.sleep(2)
        ctx.start_rr('old', ctx.new_bin, lab.PORTS['api'])
        activate(ctx, 'old', 'new')
        events.append({'event': 'compatible-rollback', 'time': time.time()})
        reload_and_drain(['old'])
        ctx.stop_rr('new')
        time.sleep(2)
        stop.set()
        future.result(timeout=35)
        assert not errors, errors
        assert len(requests) >= 30, len(requests)
        done(ctx, requests, seconds=90)
        result = check_money(ctx, requests)
        attempts = ctx.proxy_sends()
        dispatch = [max(0, (min(e['t'] for e in attempts if e.get('value') == tx['value'])-tx['acceptedAt'])*1000) for tx in requests]
        result.update({'pass': True, 'scope': 'Isolated Linux dev-host Nginx and release binary; not ECS/ALB or full settlement',
                       'binarySha256': hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
                       'requests': len(requests), 'httpStatuses': [r['status'] for r in requests],
                       'maxAdmissionMs': max(r['ms'] for r in requests),
                       'maxAcceptedToFirstRpcAttemptMs': round(max(dispatch), 2), 'events': events})
        (out/'verdict.json').write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({k: v for k, v in result.items() if k not in ('rows', 'chain', 'events', 'httpStatuses')}))
    finally:
        stop.set()
        pool.shutdown(wait=True)
        lab.stop_proc(proxy)
        if ctx:
            ctx.db_dump(str(out/'transactions.tsv'))
            ctx.stop_all()
        pg.stop()


if __name__ == '__main__':
    main()
