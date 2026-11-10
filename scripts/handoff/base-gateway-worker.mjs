// Real API gateway/guard/forwarder/meter from an immutable API snapshot.
// Only the upstream RPC and budget reservation store are local test dependencies.
import './egress-guard.mjs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
const root = process.env.HANDOFF_API_SNAPSHOT;
const require = createRequire(`${root}/package.json`);
const express = require('express');
const { mnemonicToAccount } = require('viem/accounts');
const { createBaseRpcGateway, forwardGasLaneRpc } = await import(pathToFileURL(`${root}/src/orders/base-rpc-gateway.ts`));
const { createMeteredLimitBaseFetch } = await import(pathToFileURL(`${root}/src/orders/rpc-meter.ts`));
const { limitOrderRpcScope, currentLimitOrderRpc } = await import(pathToFileURL(`${root}/src/orders/context.ts`));
const upstream = new URL(process.env.LAB_GATEWAY_UPSTREAM).toString();
if (new URL(upstream).hostname !== '127.0.0.1') throw Error('Local RPC required');
const state = { mode: 'normal', sends: [], units: 0, denied: 0, redirectHits: 0 };
const send = createMeteredLimitBaseFetch(currentLimitOrderRpc, async (_bucket, units) => {
  if (state.mode === 'reject' && units === 1000) { state.denied++; return false; }
  state.units += units;
  return true;
}, async (request) => {
  const message = await request.clone().json();
  if (message.method === 'eth_sendRawTransaction') state.sends.push(message.params[0]);
  const response = await fetch(request);
  if (message.method === 'eth_sendRawTransaction' && state.mode === 'uncertain') {
    state.mode = 'normal';
    // Consume the actual response, then simulate loss after the node accepted it.
    await response.text();
    throw Error('Local lost response after forwarding');
  }
  return response;
});
const app = express();
app.get('/lab/state', (_req, res) => res.json(state));
app.post('/lab/mode', express.json(), (req, res) => { state.mode = req.body.mode; res.json({ ok: true }); });
app.post('/lab/redirect-target', (_req, res) => { state.redirectHits++; res.json({}); });
app.use('/internal/orders/base-rpc', (req, res, next) => {
  if (state.mode === 'redirect') res.redirect(307, '/lab/redirect-target');
  else next();
});
app.use(createBaseRpcGateway(async message => {
  try {
    return await forwardGasLaneRpc(message, send,
      (bucket, fn) => limitOrderRpcScope.run({ bucket, rpcUrl: upstream }, fn));
  } catch (error) {
    console.error('Local gateway failure:', error.message);
    throw error;
  }
}));
// Negative envelopes are signed only with Anvil's public mnemonic.
app.get('/lab/envelopes', async (_req, res) => {
  const account = mnemonicToAccount('test test test test test test test test test test test junk');
  const other = mnemonicToAccount('test test test test test test test test test test test junk', { addressIndex: 1 });
  const tx = { type: 'eip1559', chainId: 8453, nonce: 0, to: '0x000000000000000000000000000000000000dEaD', value: 0n, gas: 21000n, maxFeePerGas: 1000000000n, maxPriorityFeePerGas: 100000000n };
  res.json({ wrongSigner: await other.signTransaction(tx), wrongChain: await account.signTransaction({ ...tx, chainId: 1 }), value: await account.signTransaction({ ...tx, value: 1n }), excessiveGas: await account.signTransaction({ ...tx, gas: 1000000n }) });
});
const server = app.listen(Number(process.env.LAB_GATEWAY_PORT), '127.0.0.1');
process.on('SIGTERM', () => server.close(() => process.exit(0)));
