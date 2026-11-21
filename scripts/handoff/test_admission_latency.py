#!/usr/bin/env python3
"""Local admission isolation benchmark and same-sender nonce regression.

Uses the existing disposable PostgreSQL/Anvil lab. No live endpoints or keys.
Run with --candidate BINARY --out DIR; add --baseline BINARY for an ABBA comparison.
"""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import time
import uuid

import lab
from test_handoff import activate, check_money, done


def send(ctx, relayer_id, value):
    started = time.time()
    code, result = lab.http(
        'POST', ctx.rr['bench']['api'] + f'/transactions/relayers/{relayer_id}/send',
        {'to': lab.SINK, 'value': hex(value), 'data': '0x'}, auth=True,
    )
    assert code == 200, (code, result)
    return dict(result, value=value, started=started, accepted=time.time(),
                admissionMs=(time.time() - started) * 1000)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--baseline')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=True)
    pg = lab.Postgres(str(root))
    samples = []
    versions = ['baseline', 'candidate', 'candidate', 'baseline'] if args.baseline else ['candidate']
    try:
        pg.start()
        for block, version in enumerate(versions):
            folder = root / f'{block}-{version}'
            folder.mkdir(exist_ok=True)
            binary = str(Path(args.baseline if version == 'baseline' else args.candidate).resolve())
            ctx = lab.Ctx(pg, str(folder), f'admission_bench_{block}', binary, None, lab.TIP_WEI)
            transactions = []
            try:
                ctx.start_infra()
                ctx.start_rr('bench', binary, lab.PORTS['api'])
                ctx.stop_rr('bench')
                ids = [str(uuid.uuid4()), str(uuid.uuid4())]
                addresses = []
                for index, relayer_id in enumerate(ids):
                    address = lab.cast('wallet', 'address', '--mnemonic', lab.MNEMONIC,
                                       '--mnemonic-index', str(index))
                    addresses.append(address)
                    pg.psql(ctx.dbname, f"INSERT INTO relayer.record(id,name,chain_id,address,wallet_index) VALUES('{relayer_id}','bench-{index}',{lab.CHAIN_ID},decode('{address[2:]}','hex'),{index})")
                ctx.start_rr('bench', binary, lab.PORTS['api'])
                activate(ctx, 'bench', bootstrap=True)
                transactions += [send(ctx, ids[i], 100 + i) for i in range(2)]
                done(ctx, transactions)
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    for round_index in range(8):
                        delay = 2.0 if round_index % 2 else 0.0
                        value = 10000 + block * 100 + round_index * 2
                        ctx.lab('/lab/admission_delay', {'value': value, 'seconds': delay})
                        slow_future = pool.submit(send, ctx, ids[0], value)
                        assert lab.wait_until(lambda: ctx.lab('/lab/state').get('admission_started') is not None, 10, .01), 'estimate not observed'
                        fast = send(ctx, ids[1], value + 1)
                        slow = slow_future.result(timeout=30)
                        transactions += [slow, fast]
                        done(ctx, [slow, fast], 30)
                        attempts = [event for event in ctx.proxy_sends() if event.get('value') == fast['value']]
                        assert attempts, 'missing broadcast'
                        broadcast = min(event['t'] for event in attempts)
                        sample = dict(version=version, block=block, round=round_index,
                                      slowAdmissionSeconds=delay, admissionMs=fast['admissionMs'],
                                      submitToBroadcastMs=(broadcast - fast['started']) * 1000,
                                      broadcastBeforeOtherAdmissionCompleted=broadcast < slow['accepted'],
                                      id=fast['id'])
                        samples.append(sample)
                        (root / 'samples.json').write_text(json.dumps(samples, indent=2))
                        print(json.dumps(sample), flush=True)
                        if version == 'candidate' and delay:
                            assert broadcast < slow['accepted'], 'unrelated sender blocked on admission'
                    # Eight concurrent admissions for the SAME sender must retain
                    # unique nonces and exactly-once delivery, not become parallel sends.
                    burst = list(pool.map(lambda i: send(ctx, ids[0], 20000 + block * 100 + i), range(8)))
                    transactions += burst
                    done(ctx, burst, 45)
                    rows = ctx.rows()
                    assert len({rows[tx['id']]['nonce'] for tx in burst}) == len(burst)
                chain = []
                for address in addresses:
                    ctx.relayer = address
                    chain += ctx.chain_scan()
                ctx.chain_scan = lambda: chain
                accounting = check_money(ctx, transactions)
                (folder / 'verdict.json').write_text(json.dumps(dict(
                    passed=True, binarySha256=hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
                    transfers=len(transactions), **accounting), indent=2))
            finally:
                ctx.stop_all()
    finally:
        pg.stop()


if __name__ == '__main__':
    main()
