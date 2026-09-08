// Native Node/Bun harness: real rrelayer, PostgreSQL and task-owned Anvil processes.
// No Docker, broad process kills, real chain RPCs, or production keys are used.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
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
    RRELAYER_AUTH_USERNAME: "rep503",
    RRELAYER_AUTH_PASSWORD: "fixture-only",
    RUST_LOG: "error",
  };
  let server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  const created = await request("/relayers/31337/new", {
    name: "idempotent-fixture",
  });
  assert.equal(created.status, 200, JSON.stringify(created));
  await rpc("anvil_setBalance", [created.body.address, "0x56BC75E2D63100000"]);
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
  await stop(peer);
  await stop(server);
  server = await start(binary, ["start", "--path", fixture], env);
  await until(async () => (await request("/health")).status === 200);
  const replay = await request(endpoint, transfer);
  assert.equal(replay.status, 200);
  assert.equal(replay.body.id, id);
  await sleep(1500);
  assert.equal(
    BigInt(await rpc("eth_getBalance", [destination, "latest"])) - before,
    1000000n,
  );
  console.log(
    "PASS: 12 concurrent submissions across two server processes, repeated acceptance, process restart, payload mismatch, auth; one native transfer",
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
