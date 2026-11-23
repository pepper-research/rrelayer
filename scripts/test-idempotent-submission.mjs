// Native Node/Bun harness: real rrelayer, PostgreSQL and task-owned Anvil processes.
// No Docker, broad process kills, real chain RPCs, or production keys are used.
import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { mkdtemp, writeFile, open, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import { randomUUID } from "node:crypto";

const database = process.env.RRELAYER_TEST_DATABASE_URL;
assert(
  database &&
    new URL(database).hostname === "127.0.0.1" &&
    new URL(database).pathname === "/rrelayer_rep503",
  "Use the disposable local rrelayer_rep503 database",
);
const binary = process.env.RRELAYER_BIN;
assert(binary, "Set RRELAYER_BIN to the built server binary");
const fixture = await mkdtemp(join(tmpdir(), "rrelayer-idempotence-"));
const children = new Set();
const processLogs = [];
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function port() {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const value = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return value;
}
async function start(command, args, env = {}) {
  const path = join(fixture, `process-${processLogs.length}.log`);
  processLogs.push(path);
  const log = await open(path, "a");
  const child = spawn(command, args, {
    env: { ...process.env, ...env },
    stdio: ["ignore", log.fd, log.fd],
  });
  children.add(child);
  child.once("exit", () => children.delete(child));
  return child;
}
async function stop(child) {
  if (child.exitCode !== null || child.signalCode) return;
  const closed = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await Promise.race([closed, sleep(10000)]);
  if (child.exitCode === null && !child.signalCode) {
    child.kill("SIGKILL");
    await closed;
  }
}
async function until(fn) {
  let error;
  for (let i = 0; i < 100; i++) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (caught) {
      error = caught;
    }
    await sleep(300);
  }
  throw error ?? new Error("Timed out waiting for test fixture");
}
const rpcPort = await port();
const apiPort = await port();
const auth = `Basic ${Buffer.from("rep503:fixture-only").toString("base64")}`;
const api = `http://127.0.0.1:${apiPort}`;
async function rpc(method, params) {
  const body = await (
    await fetch(`http://127.0.0.1:${rpcPort}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
    })
  ).json();
  if (body.error) throw new Error(body.error.message);
  return body.result;
}
async function request(path, body, authorized = true, base = api) {
  const response = await fetch(base + path, {
    method: body ? "POST" : "GET",
    headers: {
      "content-type": "application/json",
      ...(authorized ? { authorization: auth } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await response.text();
  let parsed;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = text;
  }
  return { status: response.status, body: parsed };
}
try {
  await start(process.env.ANVIL_BIN ?? "anvil", [
    "--host",
    "127.0.0.1",
    "--port",
    String(rpcPort),
    "--silent",
    "--chain-id",
    "31337",
  ]);
  await until(() => rpc("eth_chainId", []));
  await writeFile(
    join(fixture, "rrelayer.yaml"),
    `name: idempotent-fixture\napi_config:\n  host: 127.0.0.1\n  port: ${apiPort}\n  authentication_username: rep503\n  authentication_password: fixture-only\nsigning_provider:\n  raw:\n    mnemonic: test test test test test test test test test test test junk\nnetworks:\n  - name: local\n    chain_id: 31337\n    provider_urls:\n      - http://127.0.0.1:${rpcPort}\n    allowed_random_relayers: '*'\n`,
  );
  const env = {
    DATABASE_URL: database,
    RRELAYER_RELEASE_ID: "idempotent-fixture",
    RRELAYER_AUTH_USERNAME: "rep503",
    RRELAYER_AUTH_PASSWORD: "fixture-only",
    RUST_LOG: "error",
  };
  let server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  await stop(server);
  const relayerId = randomUUID();
  execFileSync(process.env.PSQL_BIN ?? "psql", [database, "-v", "ON_ERROR_STOP=1", "-c",
    `INSERT INTO relayer.record(id,name,chain_id,address,wallet_index) VALUES('${relayerId}','fixture',31337,decode('f39fd6e51aad88f6f4ce6ab8827279cfffb92266','hex'),0)`]);
  server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  assert.equal((await request("/handoff/activate", {expected_release:null,bootstrap:true})).status,200);
  // The original endpoint treats externalId as correlation only: an identical
  // retry creates another transaction. Keep this control visible in the test.
  const controlDestination = "0x0000000000000000000000000000000000001236";
  const controlBefore = BigInt(
    await rpc("eth_getBalance", [controlDestination, "latest"]),
  );
  const controlTransfer = {
    externalId: randomUUID(),
    to: controlDestination,
    value: "1000000",
    data: "0x",
    speed: "FAST",
  };
  const controlOne = await request(
    "/transactions/relayers/31337/send-random",
    controlTransfer,
  );
  const controlTwo = await request(
    "/transactions/relayers/31337/send-random",
    controlTransfer,
  );
  assert.equal(controlOne.status, 200, JSON.stringify(controlOne));
  assert.equal(controlTwo.status, 200, JSON.stringify(controlTwo));
  assert.notEqual(controlOne.body.id, controlTwo.body.id);
  await until(
    async () =>
      BigInt(await rpc("eth_getBalance", [controlDestination, "latest"])) -
        controlBefore ===
      2000000n,
  );
  console.log(
    "CONTROL: the existing send-random route created two transactions and transferred twice for the same externalId",
  );
  const destination = "0x0000000000000000000000000000000000001234";
  const before = BigInt(await rpc("eth_getBalance", [destination, "latest"]));
  const transfer = {
    externalId: randomUUID(),
    to: destination,
    value: "1000000",
    data: "0x",
    speed: "FAST",
  };
  const secondPort = await port();
  const secondFixture = await mkdtemp(
    join(tmpdir(), "rrelayer-idempotence-peer-"),
  );
  const configText = await (
    await import("node:fs/promises")
  ).readFile(join(fixture, "rrelayer.yaml"), "utf8");
  await writeFile(
    join(secondFixture, "rrelayer.yaml"),
    configText.replace(`port: ${apiPort}`, `port: ${secondPort}`),
  );
  const peer = await start(binary, ["start", "--path", secondFixture], env);
  const peerApi = `http://127.0.0.1:${secondPort}`;
  await until(
    async () =>
      (await request("/health", undefined, true, peerApi)).status === 200,
  );
  const endpoint = "/transactions/relayers/31337/send-idempotent";
  assert.equal((await request(endpoint, transfer, false)).status, 401);
  // Concurrent requests cover the response-loss retry case: every response but one can be discarded.
  const results = await Promise.all(
    Array.from({ length: 12 }, (_, i) =>
      request(endpoint, transfer, true, i % 2 ? peerApi : api),
    ),
  );
  for (const result of results)
    assert.equal(result.status, 200, JSON.stringify(result));
  const id = results[0].body.id;
  assert(results.every((result) => result.body.id === id));
  assert.equal(
    (await request(endpoint, { ...transfer, value: "1000001" })).status,
    400,
  );
  assert.equal(
    (
      await request(endpoint, {
        ...transfer,
        to: "0x0000000000000000000000000000000000001235",
      })
    ).status,
    400,
  );
  await until(async () =>
    ["MINED", "CONFIRMED"].includes(
      (await request(`/transactions/${id}`)).body?.status,
    ),
  );
  assert.equal(
    BigInt(await rpc("eth_getBalance", [destination, "latest"])) - before,
    1000000n,
  );
  const signer = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
  const unavailableSigner = "0x000000000000000000000000000000000000dead";
  const senderEndpoint = "/transactions/relayers/31337/send-idempotent-for-sender";
  const pinnedEndpoint = senderEndpoint + "?expectedSender=" + signer;
  const wrongSenderEndpoint = senderEndpoint + "?expectedSender=" + unavailableSigner;
  const pinned = {
    ...transfer,
    externalId: randomUUID(),
    to: "0x0000000000000000000000000000000000001237",
  };
  const pinnedBefore = BigInt(await rpc("eth_getBalance", [pinned.to, "latest"]));
  assert.equal((await request(pinnedEndpoint, pinned, false)).status, 401);
  assert.equal((await request(senderEndpoint, pinned)).status, 400);
  assert.equal((await request(wrongSenderEndpoint, pinned)).status, 400);
  assert.equal((await request("/transactions/" + pinned.externalId)).body, null);
  const acceptedPinned = await request(pinnedEndpoint, pinned);
  assert.equal(acceptedPinned.status, 200, JSON.stringify(acceptedPinned));
  assert.equal(acceptedPinned.body.id, pinned.externalId);
  assert.equal((await request(wrongSenderEndpoint, pinned)).status, 400);
  assert.deepEqual((await request(pinnedEndpoint, pinned)).body, acceptedPinned.body);
  await until(async () =>
    BigInt(await rpc("eth_getBalance", [pinned.to, "latest"])) - pinnedBefore === 1000000n,
  );

  await stop(peer);
  await stop(server);
  server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  const replay = await request(endpoint, transfer);
  assert.equal(replay.status, 200);
  assert.equal(replay.body.id, id);
  assert.equal((await request(`/transactions/${id}/prebroadcast-failure`)).body, null);
  await rpc("anvil_setBalance", ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "0x0"]);
  const rejected = { ...transfer, externalId: randomUUID() };
  assert.equal((await request(endpoint, rejected)).status, 500);
  const failurePath = `/transactions/${rejected.externalId}/prebroadcast-failure`;
  assert.equal((await request(failurePath, undefined, false)).status, 401);
  const proof = await request(failurePath);
  assert.equal(proof.status, 200, JSON.stringify(proof));
  assert.equal(proof.body.kind, "terminal_prebroadcast_v1");
  assert.equal(proof.body.transaction.id, rejected.externalId);
  assert.equal(proof.body.transaction.status, "FAILED");
  assert.equal(proof.body.transaction.txHash, undefined);

  // An unsigned admission failure leaves a nonce reusable. Give the
  // independent legacy fixture a nonce with no other transaction history.
  const nextNonce = BigInt(await rpc("eth_getTransactionCount", [signer, "latest"])) + 1n;
  await rpc("anvil_setNonce", [signer, "0x" + nextNonce.toString(16)]);
  const legacyEndpoint = "/transactions/relayers/31337/send-random";
  const legacyExternalId = "send_" + randomUUID().replaceAll("-", "");
  assert.equal((await request(legacyEndpoint, { ...transfer, externalId: legacyExternalId })).status, 500);
  const legacyRecord = await request("/transactions/external/" + legacyExternalId);
  assert.equal(legacyRecord.status, 200);
  assert.equal(legacyRecord.body.externalId, legacyExternalId);
  const legacyPath = "/transactions/" + legacyRecord.body.id + "/prebroadcast-failure";
  const legacyProofPath = legacyPath + "?legacyExternalId=" + legacyExternalId;
  assert.equal((await request(legacyPath)).body, null);
  assert.equal((await request(legacyProofPath, undefined, false)).status, 401);
  assert.equal((await request(legacyPath + "?legacyExternalId=send_invalid")).status, 400);
  assert.equal((await request(legacyPath + "?legacyExternalId=send_" + "f".repeat(32))).body, null);
  assert.equal((await request(failurePath + "?legacyExternalId=" + legacyExternalId)).body, null);
  const legacyProof = await request(legacyProofPath);
  assert.equal(legacyProof.status, 200, JSON.stringify(legacyProof));
  assert.equal(legacyProof.body.kind, "legacy_terminal_prebroadcast_v1");
  assert.equal(legacyProof.body.transaction.id, legacyRecord.body.id);
  assert.equal(legacyProof.body.transaction.externalId, legacyExternalId);
  assert.equal(legacyProof.body.transaction.status, "FAILED");
  assert.equal(legacyProof.body.transaction.txHash, undefined);

  // Refilling the sender cannot revive the same rejected key, even after restart.
  await rpc("anvil_setBalance", ["0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266", "0x100000000000000000000000000000000000000000000"]);
  await stop(server);
  server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  assert.equal((await request(endpoint, rejected)).status, 400);
  assert.deepEqual((await request(failurePath)).body, proof.body);
  assert.deepEqual((await request(legacyProofPath)).body, legacyProof.body);
  const sibling = randomUUID();
  const legacyId = legacyRecord.body.id;
  execFileSync(process.env.PSQL_BIN ?? "psql", [database, "-v", "ON_ERROR_STOP=1", "-c",
    "BEGIN; SELECT set_config('rrelayer.sender_token', o.token, true) FROM relayer.sender_owner o " +
    "JOIN relayer.transaction t ON o.chain_id=t.chain_id AND o.signer=t.\"from\" WHERE t.id='" + legacyId + "' FOR SHARE OF o; " +
    "INSERT INTO relayer.transaction (id,relayer_id,\"to\",\"from\",nonce,chain_id,value,speed,status,expires_at,external_id,broadcast_attempted) " +
    "SELECT '" + sibling + "',relayer_id,\"to\",\"from\",nonce,chain_id,value,speed,'FAILED',expires_at,'" + sibling + "',TRUE " +
    "FROM relayer.transaction WHERE id='" + legacyId + "'; COMMIT;"]);
  assert.equal((await request(legacyProofPath)).body, null);
  execFileSync(process.env.PSQL_BIN ?? "psql", [database, "-v", "ON_ERROR_STOP=1", "-c",
    "DELETE FROM relayer.transaction WHERE id='" + sibling + "'"]);
  assert.deepEqual((await request(legacyProofPath)).body, legacyProof.body);
  // The immutable history, not just today's missing hash, controls the proof.
  execFileSync(process.env.PSQL_BIN ?? "psql", [database, "-v", "ON_ERROR_STOP=1", "-c",
    `UPDATE relayer.transaction_audit_log SET broadcast_attempted=TRUE WHERE id IN ('${rejected.externalId}', '${legacyRecord.body.id}')`]);
  assert.equal((await request(failurePath)).body, null);
  assert.equal((await request(legacyProofPath)).body, null);
  await sleep(1500);
  assert.equal(
    BigInt(await rpc("eth_getBalance", [destination, "latest"])) - before,
    1000000n,
  );
  console.log(
    "PASS: concurrent keyed submissions, exact sender pin, restart, mismatch, auth, keyed and legacy prebroadcast proof, ambiguous sibling and attempted history refuse proof",
  );
} catch (error) {
  // Only task-owned processes and public local fixture identities reach these logs.
  for (const path of processLogs) {
    console.error(
      `Process diagnostic ${path}:\n${(await readFile(path, "utf8")).slice(-8000)}`,
    );
  }
  throw error;
} finally {
  await Promise.all([...children].map(stop));
  // Logs stay with the fixture if a test fails; they contain only public local test identities.
  console.log(`Fixture artifacts: ${fixture}`);
}
