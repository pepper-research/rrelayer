#!/usr/bin/env python3
"""Shared local PostgreSQL, Anvil and fault-proxy fixtures (stdlib only).

Derived from the nonce-used reproduction lab. Run test_handoff.py or
 test_api_boundary.py; this module owns only disposable local child processes.
"""
import argparse
import base64
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
MNEMONIC = "test test test test test test test test test test test junk"  # anvil's public dev mnemonic
CHAIN_ID = int(os.environ.get("LAB_CHAIN_ID", 31337))
AUTH_USER, AUTH_PASS = "lab", "lab-password"
SINK = "0x000000000000000000000000000000000000dEaD"
OTHER_SINK = "0x000000000000000000000000000000000000bEEF"
BASE_VALUE = 1001  # submission i carries value BASE_VALUE + i wei, which identifies its payload on chain
OTHER_VALUE = 7777
N_TXS = 5
OK_STATUSES = ("MINED", "CONFIRMED")
TERMINAL = OK_STATUSES + ("FAILED", "EXPIRED", "CANCELLED", "DROPPED", "REPLACED")

PORTS = {
    "anvil": int(os.environ.get("LAB_ANVIL_PORT", 18545)),
    "proxy": int(os.environ.get("LAB_PROXY_PORT", 18546)),
    "api": int(os.environ.get("LAB_API_PORT", 13000)),
    "api_b": int(os.environ.get("LAB_API_PORT_B", 13001)),
    "pg": int(os.environ.get("LAB_PG_PORT", 15447)),
}
HIDE_SECONDS = float(os.environ.get("LAB_HIDE_SECONDS", 10))
SETTLE_TIMEOUT = float(os.environ.get("LAB_SETTLE_TIMEOUT", 90))
SETTLE_TIMEOUT_LONG = float(os.environ.get("LAB_SETTLE_TIMEOUT_LONG", 150))  # covers PR #7's 60 s bounded wait
STALL_OBSERVE = float(os.environ.get("LAB_STALL_OBSERVE_SECONDS", 45))
OVERLAP_SECONDS = max(float(os.environ.get("LAB_OVERLAP_SECONDS", 120)), 120.0)
OVERLAP_CADENCE = float(os.environ.get("LAB_OVERLAP_CADENCE_SECONDS", 2))
OVERLAP_DRAIN_TIMEOUT = float(os.environ.get("LAB_OVERLAP_DRAIN_TIMEOUT", 600))
ROLLBACK_OBSERVE = float(os.environ.get("LAB_ROLLBACK_OBSERVE_SECONDS", 120))
TIP_WEI = 10 ** 9
PG_BIN = os.environ.get("LAB_PG_BIN", "/opt/homebrew/opt/postgresql@16/bin")
INSUFFICIENT_FUNDS = "insufficient funds for gas * price + value: balance 0, tx cost 21000000000000"
INTRINSIC_GAS = "intrinsic gas too low: gas 20000, minimum needed 21000"

STARTED = []  # {"name", "pid", "cmd"} for every process this run started


def say(msg):
    print(f"[lab {time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def port_free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def spawn(name, cmd, log_path, env=None, cwd=None):
    f = open(log_path, "ab")
    p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, env=env, cwd=cwd, start_new_session=True)
    STARTED.append({"name": name, "pid": p.pid, "cmd": " ".join(cmd[:2])})
    return p


def stop_proc(p, timeout=15):
    if p is None or p.poll() is not None:
        return
    try:
        os.killpg(p.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        p.wait(timeout)
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
        p.wait(5)


def http(method, url, body=None, auth=False, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(f"{AUTH_USER}:{AUTH_PASS}".encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def rpc(url, method, params):
    _, r = http("POST", url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in r:
        raise RuntimeError(f"{method}: {r['error']}")
    return r["result"]


def wait_until(pred, timeout, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return None


def cast(*args):
    return subprocess.check_output(["cast", *args], text=True).strip()


def write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)


# ---------------------------------------------------------------------------------------------
class Postgres:
    ENV = dict(os.environ, LC_ALL="C", LANG="C")  # macOS: postmaster aborts as "multithreaded" without it

    def __init__(self, out):
        self.data = os.path.join(out, ".pgdata")
        self.log = os.path.join(out, "postgres.log")

    def pg(self, tool):
        return os.path.join(PG_BIN, tool)

    def start(self):
        if os.path.exists(self.data):
            shutil.rmtree(self.data)
        subprocess.check_call([self.pg("initdb"), "-D", self.data, "-U", "postgres", "--auth=trust", "-E", "UTF8"],
                              stdout=subprocess.DEVNULL, env=self.ENV)
        subprocess.check_call([self.pg("pg_ctl"), "-D", self.data, "-l", self.log, "-w", "-o",
                               f"-p {PORTS['pg']} -c listen_addresses=127.0.0.1 -c unix_socket_directories=''",
                               "start"], stdout=subprocess.DEVNULL, env=self.ENV)
        with open(os.path.join(self.data, "postmaster.pid")) as f:
            STARTED.append({"name": "postgres", "pid": int(f.readline().strip()), "cmd": "postgres (pg_ctl)"})

    def createdb(self, name):
        subprocess.check_call([self.pg("createdb"), "-h", "127.0.0.1", "-p", str(PORTS["pg"]), "-U", "postgres", name],
                              env=self.ENV)

    def psql(self, db, sql, sep="\t", tuples_only=False):
        args = [self.pg("psql"), "-h", "127.0.0.1", "-p", str(PORTS["pg"]), "-U", "postgres", "-d", db,
                "-A", "-F", sep, "-v", "ON_ERROR_STOP=1", "-c", sql]
        if tuples_only:
            args.insert(-2, "-t")
        r = subprocess.run(args, capture_output=True, text=True, env=self.ENV)
        if r.returncode != 0:
            raise RuntimeError(f"psql failed: {r.stderr.strip()}")
        return r.stdout

    def stop(self):
        if os.path.exists(self.data):
            subprocess.call([self.pg("pg_ctl"), "-D", self.data, "-m", "fast", "-w", "stop"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=self.ENV)
            shutil.rmtree(self.data, ignore_errors=True)


# ---------------------------------------------------------------------------------------------
class Ctx:
    """One scenario: anvil + fault proxy + database, and the rrelayer processes started on them."""

    def __init__(self, pg, sdir, dbname, new_bin, old_bin, tip_wei):
        self.pg, self.sdir, self.dbname = pg, sdir, dbname
        self.new_bin, self.old_bin, self.tip_wei = new_bin, old_bin, tip_wei
        self.anvil_url = f"http://127.0.0.1:{PORTS['anvil']}"
        self.proxy_url = f"http://127.0.0.1:{PORTS['proxy']}"
        self.infra = []
        self.rr = {}  # name -> {"proc", "api", "binary", "started_at", "stopped_at"}
        self.subs = []  # every submission: {"i", "value", "id", "via", "at"}
        self.next_i = 0
        self.relayer = self.relayer_id = self.relayer_key = None

    # --- infrastructure ---
    def start_infra(self):
        for k in ("anvil", "proxy", "api", "api_b"):
            if not port_free(PORTS[k]):
                raise RuntimeError(f"port {PORTS[k]} ({k}) is in use; set LAB_{k.upper()}_PORT")
        self.pg.createdb(self.dbname)
        self.infra.append(spawn("anvil", ["anvil", "--port", str(PORTS["anvil"]), "--host", "127.0.0.1",
                                          "--block-time", "1", "--mnemonic", MNEMONIC, "--chain-id", str(CHAIN_ID)],
                                os.path.join(self.sdir, "anvil.log")))
        if not wait_until(lambda: not port_free(PORTS["anvil"]), 20):
            raise RuntimeError("anvil did not start")
        self.infra.append(spawn("proxy", [sys.executable, os.path.join(HERE, "proxy.py"), "--port", str(PORTS["proxy"]),
                                          "--upstream", self.anvil_url, "--log", os.path.join(self.sdir, "proxy.jsonl")],
                                os.path.join(self.sdir, "proxy.stderr.log")))
        if not wait_until(lambda: not port_free(PORTS["proxy"]), 20):
            raise RuntimeError("proxy did not start")
        time.sleep(.2)
        if any(proc.poll() is not None for proc in self.infra):
            raise RuntimeError("Owned Anvil/proxy exited; possible port collision")
        if int(rpc(self.proxy_url, 'eth_chainId', []), 16) != CHAIN_ID:
            raise RuntimeError("Test RPC chain identity mismatch")
        if self.tip_wei is not None:
            self.lab("/lab/tip", {"wei": self.tip_wei})

    def start_rr(self, name, binary, api_port, extra_network_yaml=""):
        project = os.path.join(self.sdir, f"project-{name}")
        os.makedirs(project, exist_ok=True)
        with open(os.path.join(project, "rrelayer.yaml"), "w") as f:
            f.write(f"""name: rrelayer-nonce-used-lab
api_config:
  host: 127.0.0.1
  port: {api_port}
  authentication_username: ${{RRELAYER_AUTH_USERNAME}}
  authentication_password: ${{RRELAYER_AUTH_PASSWORD}}
signing_provider:
  raw:
    mnemonic: ${{RAW_DANGEROUS_MNEMONIC}}
networks:
- name: local_anvil
  chain_id: {CHAIN_ID}
  provider_urls:
    - {self.proxy_url}
  allowed_random_relayers: "*"
  confirmations: 1
  max_gas_price_multiplier: 4
  gas_bump_blocks_every:
    slow: 10
    medium: 5
    fast: 4
    super_fast: 2
""")
        if extra_network_yaml:
            with open(os.path.join(project, "rrelayer.yaml"), "a") as f: f.write(extra_network_yaml)
        db_url = f"postgres://postgres@127.0.0.1:{PORTS['pg']}/{self.dbname}"
        with open(os.path.join(project, ".env"), "w") as f:
            f.write(f"DATABASE_URL={db_url}\nRRELAYER_AUTH_USERNAME={AUTH_USER}\nRRELAYER_AUTH_PASSWORD={AUTH_PASS}\n"
                    f"RAW_DANGEROUS_MNEMONIC=\"{MNEMONIC}\"\n")
        env = dict(os.environ, DATABASE_URL=db_url, RRELAYER_AUTH_USERNAME=AUTH_USER,
                   RRELAYER_AUTH_PASSWORD=AUTH_PASS, RAW_DANGEROUS_MNEMONIC=MNEMONIC,
                   RUST_LOG=os.environ.get("LAB_RUST_LOG", "info"), RUST_BACKTRACE="1", RRELAYER_RELEASE_ID=name)
        proc = spawn(f"rrelayer-{name}", [binary, "start", "--path", project],
                     os.path.join(self.sdir, f"rrelayer-{name}.log"), env=env, cwd=project)
        api = f"http://127.0.0.1:{api_port}"
        ok = wait_until(lambda: self._health(api) or proc.poll() is not None, 180, 1)
        if proc.poll() is not None or not ok:
            raise RuntimeError(f"rrelayer {name} did not start (see rrelayer-{name}.log)")
        self.rr[name] = {"proc": proc, "api": api, "binary": binary, "started_at": time.time(), "stopped_at": None}
        return api

    def stop_rr(self, name):
        stop_proc(self.rr[name]["proc"])
        self.rr[name]["stopped_at"] = time.time()

    def kill_rr(self, name):
        """SIGKILL, no graceful shutdown."""
        proc = self.rr[name]["proc"]
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(10)
        self.rr[name]["stopped_at"] = time.time()

    @staticmethod
    def _health(api):
        try:
            return http("GET", api + "/health", timeout=2)[0] == 200
        except Exception:  # noqa: BLE001
            return False

    def stop_all(self):
        for r in self.rr.values():
            stop_proc(r["proc"])
        for p in reversed(self.infra):
            stop_proc(p)

    # --- helpers ---
    def lab(self, path, body=None):
        if body is None:
            return http("GET", self.proxy_url + path)[1]
        return http("POST", self.proxy_url + path, body)[1]

    def create_relayer(self, api):
        code, r = http("POST", f"{api}/relayers/{CHAIN_ID}/new", {"name": "lab"}, auth=True)
        if code != 200:
            raise RuntimeError(f"create relayer failed: {code} {r}")
        self.relayer_id, self.relayer = r["id"], r["address"]
        for i in range(0, 30):
            if cast("wallet", "address", "--mnemonic", MNEMONIC, "--mnemonic-index", str(i)).lower() == self.relayer.lower():
                self.relayer_key = cast("wallet", "private-key", "--mnemonic", MNEMONIC, "--mnemonic-index", str(i))
                break
        bal = int(rpc(self.anvil_url, "eth_getBalance", [self.relayer, "latest"]), 16)
        if bal < 10 ** 18:
            funder = cast("wallet", "private-key", "--mnemonic", MNEMONIC, "--mnemonic-index", "9")
            cast("send", "--private-key", funder, "--rpc-url", self.anvil_url, self.relayer, "--value", "100ether")
        self.lab("/lab/sender", {"address": self.relayer})

    def submit(self, via):
        i = self.next_i
        self.next_i += 1
        api = self.rr[via]["api"]
        body = {"to": SINK, "value": hex(BASE_VALUE + i), "data": "0x"}
        code, r = http("POST", f"{api}/transactions/relayers/{self.relayer_id}/send", body, auth=True)
        sub = {"i": i, "value": BASE_VALUE + i, "via": via, "at": time.time(),
               "id": r["id"] if code == 200 else None, "error": None if code == 200 else f"{code} {r}"}
        self.subs.append(sub)
        return sub

    def rows(self):
        out = self.pg.psql(self.dbname, "select id, nonce, status, coalesce(encode(hash,'hex'),''), value::text, "
                                        "replace(coalesce(failed_reason,''), E'\\n', ' ') from relayer.transaction order by queued_at",
                           sep="\x1f", tuples_only=True)
        rows = {}
        for line in out.splitlines():
            if not line.strip():
                continue
            tid, nonce, status, h, value, reason = line.split("\x1f")
            rows[tid] = {"id": tid, "nonce": int(nonce), "status": status, "hash": ("0x" + h) if h else None,
                         "value": int(value), "failed_reason": reason or None}
        return rows

    def row(self, sub):
        return self.rows().get(sub["id"], {})

    def statuses(self, subs):
        rows = self.rows()
        return [rows.get(s["id"], {}).get("status") for s in subs]

    def onchain_nonce(self):
        return int(rpc(self.anvil_url, "eth_getTransactionCount", [self.relayer, "latest"]), 16)

    def chain_scan(self):
        latest = int(rpc(self.anvil_url, "eth_blockNumber", []), 16)
        out = []
        for n in range(0, latest + 1):
            b = rpc(self.anvil_url, "eth_getBlockByNumber", [hex(n), True])
            for t in b["transactions"]:
                if t["from"].lower() != (self.relayer or "").lower():
                    continue
                rc = rpc(self.anvil_url, "eth_getTransactionReceipt", [t["hash"]])
                out.append({"block": n, "hash": t["hash"], "nonce": int(t["nonce"], 16), "to": t["to"],
                            "value": int(t["value"], 16), "status": int(rc["status"], 16) if rc else None})
        return out

    def proxy_sends(self, since=None):
        out = []
        path = os.path.join(self.sdir, "proxy.jsonl")
        with open(path) as f:
            for line in f:
                if '"send_' not in line:
                    continue
                e = json.loads(line)
                if since is None or e["t"] >= since:
                    out.append(e)
        return out

    def db_dump(self, path):
        with open(path, "w") as f:
            for t in ("relayer.transaction", "relayer.transaction_audit_log"):
                f.write(f"==== {t}\n")
                try:
                    f.write(self.pg.psql(self.dbname, f"select * from {t} order by 1"))
                except Exception as e:  # noqa: BLE001
                    f.write(f"(dump failed: {e})\n")


# ---------------------------------------------------------------------------------------------
def log_counts(sdir):
    text = ""
    for fn in sorted(os.listdir(sdir)):
        if fn.startswith("rrelayer-") and fn.endswith(".log"):
            text += open(os.path.join(sdir, fn), errors="replace").read().lower()
    keys = {
        "nonce_too_high": "nonce too high",
        "nonce_too_low": "nonce too low",
        "baseline_renonce": "nonce synchronization recovered",
        "candidate_tracking_as_sent": "rrelayer_send_nonce_used_tracking",
        "candidate_waiting_tracked_hash": "waiting for the tracked hash",
        "candidate_failed_not_resent": "was not re-sent at a new nonce",
        "candidate_resynced": "resynced ",
        "candidate_bounded_wait_failed": "needs review; not re-sent",
        "already_known_lookup_hit": "rrelayer_send_already_known",
        "insufficient_funds_failed": "failed due to insufficient funds",
        "intrinsic_gas_failed": "failed due to intrinsic gas too low",
        "panic": "panicked",
    }
    return {k: text.count(v) for k, v in keys.items()}


def payload_counts(chain):
    counts = {}
    for t in chain:
        if (t["to"] or "").lower() == SINK.lower():
            counts[t["value"]] = counts.get(t["value"], 0) + 1
    return counts


def by_value(chain):
    return {t["value"]: t for t in chain if (t["to"] or "").lower() == SINK.lower()}


def settle(ctx, pred, timeout):
    t0 = time.time()
    ok = wait_until(lambda: pred(ctx.rows()), timeout, 1)
    return round(time.time() - t0, 1), bool(ok)


def queue_behind_held_head(ctx, via, n=N_TXS):
    """Submits tx 0 while sends are held, waits until rrelayer has tried to send it, then submits
    1..n-1 behind it."""
    ctx.lab("/lab/hold", {"on": True})
    held0 = ctx.lab("/lab/state")["held_count"]
    subs = [ctx.submit(via)]
    if not wait_until(lambda: ctx.lab("/lab/state")["held_count"] > held0, 60):
        raise RuntimeError("rrelayer never attempted to send the head tx")
    for _ in range(1, n):
        subs.append(ctx.submit(via))
    return subs


def all_ok(subs):
    return lambda rows: all(rows.get(s["id"], {}).get("status") in OK_STATUSES for s in subs)


def summary(ctx, subs):
    rows = ctx.rows()
    return [dict(rows.get(s["id"], {}), via=s["via"], i=s["i"]) for s in subs]


# ---------------------------------------------------------------------------------------------
