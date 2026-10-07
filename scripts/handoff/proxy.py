#!/usr/bin/env python3
"""Fault-injecting JSON-RPC proxy in front of anvil (stdlib only).

Forwards every JSON-RPC call to the upstream node, with three lab behaviours:

1. Gap rejection (always on once a sender is configured): an eth_sendRawTransaction whose
   nonce is above the sender's upstream "pending" nonce is rejected with
   "nonce too high: next nonce P, tx nonce n". anvil queues future nonces instead, so without
   this the incident's freeze cannot happen locally. Robinhood's node rejected gaps.
2. Hold: while on, every eth_sendRawTransaction gets a transient, non-nonce error and is NOT
   forwarded. Used to keep the queue head unsent while more txs are queued behind it.
3. Arm (one shot): the next eth_sendRawTransaction is forwarded (so the tx is included), then
   the proxy answers "nonce too low: next nonce n+1, tx nonce n", and for `hide_seconds` it
   answers null to eth_getTransactionByHash / eth_getTransactionReceipt for that hash
   (the node "has not indexed it yet").

Control endpoints (HTTP, JSON body):
  POST /lab/sender {"address": "0x.."}
  POST /lab/hold   {"on": true|false}
  POST /lab/arm    {"hide_seconds": 10, "mode": "nonce_too_low"}
                   mode "swallow": forward the next send (it is included), answer a transient error
                   and switch hold on, so rrelayer later re-signs and retries the same payload.
                   mode "reject" + "message": do NOT forward the next send; answer -32000 <message>.
                   mode "stall" + "stall_seconds": forward the next send (it is included) and hold
                   the HTTP response for that long, so the caller can be killed mid-send.
  POST /lab/hide_receipts {"seconds": 30}  answer null to every eth_getTransactionReceipt for that
                   long (0 switches it off), so sent rows stay INMEMPOOL.
  POST /lab/tip    {"wei": 1000000000}  replace every eth_feeHistory reward with this tip. anvil
                   reports 0 tips, which puts rrelayer "at max gas price cap" so it never bumps.
  GET  /lab/state
Every eth_sendRawTransaction and every hidden lookup is appended as JSONL to --log.
"""
import argparse
import hashlib
import json
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCK = threading.Lock()
STATE = {
    "sender": None,
    "hold": False,
    "held_count": 0,
    "armed": None,  # {"hide_seconds": float, "mode": "nonce_too_low"|"swallow"}
    "hide_receipts_until": 0.0,
    "tip_wei": None,  # when set, every eth_feeHistory reward is replaced with this tip
    "fault": None,  # {"hash", "raw", "nonce", "at", "hide_until"}
    "send_count": 0,
    "gap_rejections": 0,
    "hidden_lookups": 0,
    "calls": 0,
}
ARGS = None


def log(event):
    event["t"] = time.time()
    with LOCK:
        with open(ARGS.log, "a") as f:
            f.write(json.dumps(event) + "\n")


# ---- minimal RLP decoding, enough to read a signed tx's nonce ----
def rlp_item(b, i):
    p = b[i]
    if p < 0x80:
        return b[i:i + 1], i + 1
    if p <= 0xB7:
        n = p - 0x80
        return b[i + 1:i + 1 + n], i + 1 + n
    if p <= 0xBF:
        ll = p - 0xB7
        n = int.from_bytes(b[i + 1:i + 1 + ll], "big")
        s = i + 1 + ll
        return b[s:s + n], s + n
    if p <= 0xF7:
        n = p - 0xC0
        return ("list", i + 1, i + 1 + n), i + 1 + n
    ll = p - 0xF7
    n = int.from_bytes(b[i + 1:i + 1 + ll], "big")
    s = i + 1 + ll
    return ("list", s, s + n), s + n


def rlp_list(b, start, end):
    out, i = [], start
    while i < end:
        item, i = rlp_item(b, i)
        out.append(item)
    return out


def decode_tx(raw_hex):
    """Returns (nonce, to, value) of a signed raw transaction."""
    b = bytes.fromhex(raw_hex[2:] if raw_hex.startswith("0x") else raw_hex)
    if b[0] >= 0xC0:  # legacy: [nonce, gasPrice, gas, to, value, ...]
        top, _ = rlp_item(b, 0)
        f = rlp_list(b, top[1], top[2])
        idx = (0, 3, 4)
    else:
        tx_type = b[0]
        top, _ = rlp_item(b, 1)
        f = rlp_list(b, top[1], top[2])
        if tx_type == 3 and isinstance(f[0], tuple):  # blob network wrapper
            f = rlp_list(b, f[0][1], f[0][2])
        idx = (1, 4, 5) if tx_type == 1 else (1, 5, 6)  # 2930 vs 1559/4844/7702
    nonce = int.from_bytes(f[idx[0]], "big")
    to = "0x" + f[idx[1]].hex() if f[idx[1]] else None
    value = int.from_bytes(f[idx[2]], "big")
    return nonce, to, value


def upstream(payload):
    req = urllib.request.Request(
        ARGS.upstream, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def digest(raw):
    return hashlib.sha256(raw.lower().encode()).hexdigest()[:16]


def err(rid, code, message):
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


def handle_call(call):
    method = call.get("method")
    rid = call.get("id")
    params = call.get("params") or []
    with LOCK:
        STATE["calls"] += 1

    if method in ("eth_getTransactionByHash", "eth_getTransactionReceipt") and params:
        with LOCK:
            fault = STATE["fault"]
            hide_all = method == "eth_getTransactionReceipt" and time.time() < STATE["hide_receipts_until"]
        if hide_all:
            return {"jsonrpc": "2.0", "id": rid, "result": None}
        if fault and fault["hash"] and params[0].lower() == fault["hash"].lower() and time.time() < fault["hide_until"]:
            with LOCK:
                STATE["hidden_lookups"] += 1
            log({"ev": "hidden_lookup", "method": method, "hash": params[0]})
            return {"jsonrpc": "2.0", "id": rid, "result": None}
        return upstream(call)

    if method == "eth_feeHistory":
        resp = upstream(call)
        with LOCK:
            tip = STATE["tip_wei"]
        if tip is not None and isinstance(resp.get("result"), dict) and resp["result"].get("reward") is not None:
            resp["result"]["reward"] = [[hex(tip) for _ in r] for r in resp["result"]["reward"]]
        return resp

    if method != "eth_sendRawTransaction":
        return upstream(call)

    raw = params[0]
    try:
        nonce, to, value = decode_tx(raw)
    except Exception as e:  # noqa: BLE001
        nonce, to, value = None, None, None
        log({"ev": "decode_error", "error": str(e)})
    with LOCK:
        STATE["send_count"] += 1
        hold = STATE["hold"]
        sender = STATE["sender"]
        armed = STATE["armed"]
        fault = STATE["fault"]

    if hold:
        with LOCK:
            STATE["held_count"] += 1
        log({"ev": "send_held", "nonce": nonce, "value": value, "to": to, "raw_digest": digest(raw)})
        return err(rid, -32603, "lab proxy: upstream temporarily unavailable (hold)")

    if sender is not None and nonce is not None:
        pending = int(
            upstream({"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionCount", "params": [sender, "pending"]})[
                "result"
            ],
            16,
        )
        if nonce > pending:
            with LOCK:
                STATE["gap_rejections"] += 1
            log({"ev": "send_rejected_gap", "nonce": nonce, "value": value, "pending": pending, "raw_digest": digest(raw)})
            return err(rid, -32000, f"nonce too high: next nonce {pending}, tx nonce {nonce}")

    if armed is not None and fault is None and armed.get("mode") == "reject":
        with LOCK:
            STATE["armed"] = None
            STATE["fault"] = {"mode": "reject", "hash": None, "raw": raw, "nonce": nonce, "at": time.time(),
                              "hide_until": 0, "message": armed["message"]}
        log({"ev": "send_faulted", "mode": "reject", "nonce": nonce, "value": value, "to": to, "raw_digest": digest(raw),
             "message": armed["message"]})
        return err(rid, -32000, armed["message"])

    if armed is not None and fault is None:
        resp = upstream(call)
        with LOCK:
            STATE["armed"] = None
        if "result" in resp and resp["result"]:
            now = time.time()
            mode = armed.get("mode", "nonce_too_low")
            with LOCK:
                if mode == "swallow":
                    STATE["hold"] = True  # keep the head unsent until the lab changes the fee and releases it
                STATE["fault"] = {
                    "mode": mode,
                    "hash": resp["result"],
                    "raw": raw,
                    "nonce": nonce,
                    "at": now,
                    "hide_until": now + armed["hide_seconds"],
                }
            log({"ev": "send_faulted", "mode": mode, "nonce": nonce, "value": value, "hash": resp["result"],
                 "hide_seconds": armed["hide_seconds"], "raw_digest": digest(raw), "raw": raw})
            if mode == "swallow":
                return err(rid, -32603, "lab proxy: upstream temporarily unavailable (swallowed after forwarding)")
            if mode == "stall":
                time.sleep(float(armed.get("stall_seconds") or 30))
                return resp
            return err(rid, -32000, f"nonce too low: next nonce {nonce + 1}, tx nonce {nonce}")
        log({"ev": "arm_forward_failed", "nonce": nonce, "upstream": resp})
        return resp

    resp = upstream(call)
    same_as_fault = bool(fault and raw.lower() == fault["raw"].lower())
    log({"ev": "send_forwarded", "nonce": nonce, "value": value, "to": to, "raw_digest": digest(raw), "same_raw_as_fault": same_as_fault,
         "result": resp.get("result"), "error": resp.get("error")})
    return resp


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _reply(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/lab/state":
            with LOCK:
                s = dict(STATE)
                s["fault"] = dict(s["fault"]) if s["fault"] else None
                if s["fault"]:
                    s["fault"].pop("raw", None)
            return self._reply(s)
        self._reply({"error": "not found"}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"null")
        if self.path == "/lab/sender":
            with LOCK:
                STATE["sender"] = body["address"]
            log({"ev": "ctl_sender", "address": body["address"]})
            return self._reply({"ok": True})
        if self.path == "/lab/hold":
            with LOCK:
                STATE["hold"] = bool(body["on"])
            log({"ev": "ctl_hold", "on": bool(body["on"])})
            return self._reply({"ok": True})
        if self.path == "/lab/arm":
            with LOCK:
                STATE["armed"] = {"hide_seconds": float(body.get("hide_seconds", 10)),
                                  "mode": body.get("mode", "nonce_too_low"), "message": body.get("message"),
                                  "stall_seconds": body.get("stall_seconds")}
            log({"ev": "ctl_arm", "hide_seconds": float(body.get("hide_seconds", 10)),
                 "mode": body.get("mode", "nonce_too_low")})
            return self._reply({"ok": True})
        if self.path == "/lab/hide_receipts":
            with LOCK:
                STATE["hide_receipts_until"] = time.time() + float(body.get("seconds", 0))
            log({"ev": "ctl_hide_receipts", "seconds": float(body.get("seconds", 0))})
            return self._reply({"ok": True})
        if self.path == "/lab/tip":
            with LOCK:
                STATE["tip_wei"] = body.get("wei")
            log({"ev": "ctl_tip", "wei": body.get("wei")})
            return self._reply({"ok": True})
        try:
            if isinstance(body, list):
                out = [handle_call(c) for c in body]
            else:
                out = handle_call(body)
        except Exception as e:  # noqa: BLE001
            log({"ev": "proxy_error", "error": repr(e)})
            out = err(body.get("id") if isinstance(body, dict) else None, -32603, f"lab proxy error: {e}")
        self._reply(out)


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--upstream", required=True)
    ap.add_argument("--log", required=True)
    ARGS = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", ARGS.port), Handler)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
