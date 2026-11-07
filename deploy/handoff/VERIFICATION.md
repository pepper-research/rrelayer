# Local verification — 2026-10-07

This is review evidence, **not an image acceptance receipt or deployment approval**.
The tested debug binary SHA256 is
`4294fc360861bbae1fed82d33c2d91e92ffde91820224bf31603887fa2acd3da`.
The runtime source hashes are in `evidence/runtime-source-manifest.json`.
Base: `fc554dedddcb7e39810cf209eaf8acfeb570068a`.

## Observed results

- 18 candidate scenarios passed using real PostgreSQL 16, two CLI processes,
  Anvil and a controlled RPC proxy. Four crash barriers, accepted-but-lost RPC
  responses, stale-owner revival, natural abandoned-session expiry, database
  restart, failed startup, replacement/cancel winners, attempted-payload safety,
  expiry, reverted receipt and actual signed EIP-7702 authorization were covered.
- Two legacy scenarios passed their expectations: the production-line binary
  reproduced an unsafe fresh-nonce repeat of a value transfer; drained/stopped
  legacy bootstrap into the candidate preserved exactly one payout per request.
- Warm handoff admitted 15/15 requests with HTTP 200. Maximum admission latency
  was 320.76 ms; maximum accepted-to-first-RPC-attempt was 1,141.34 ms; handoff
  through queue drain took 3.457 s. These are local observations, not ALB SLOs.
- The actual adjacent API journal/client/refund guard test ran 135.64 seconds.
  Cancellation at 5 and 125 seconds stayed blocked, including after the actual
  120-second client timeout. Recovery used the same hash; one journal row,
  relayer row and successful 424242-wei on-chain credit remained. This uses a
  native payout and persisted escrow fixture, **not full rollup settlement or
  deposit indexing**. All HTTP egress was restricted to loopback.
- 11 mocked deployment-controller tests passed, including interrupted promotion,
  pre-activation failure, recovery and rejection of incomplete production evidence.
- `cargo check --locked --workspace`, build and rustfmt passed. The core unit
  suite passed 29 tests including its real PostgreSQL test; the ignored Anvil
  provider regression passed separately. Existing keyed-submission smoke passed
  concurrent retries across two processes, restart, authorization and mismatch
  checks. Those existing tests preceded only the startup top-up guard; final
  real-process tests exercised that guard and all final runtime source.
- TypeScript with the reused installed dependency tree reports the same three
  `ox` dependency errors on base and candidate. Both were compared explicitly;
  candidate `tsc --noEmit --lib es2022,dom` passes. No dependency-parity or clean
  stock TypeScript build is claimed, and no dependencies were installed.

The API source was frozen read-only from the adjacent safety work at base
`52b73a4e5055c12edef882d66c070337b506ae51`, including its working changes.
Its 331-file source/test/migration manifest SHA256 is
`80510b6da0f1b7e2b4fc686385d01f793540dd25a8c6d2179324f062a8b97cd2`.
This is a source snapshot identity, not a built API image identity. The snapshot
and full local logs are retained privately; sanitized final verdicts are included.
Initial harness errors (wrong Cast command/status expectation, disabled interval
mining and a shared-port collision) were corrected and the affected cases rerun.

## Reproduce and acceptance still required

See [README](README.md#verification) for commands. Use separate `LAB_ANVIL_PORT`,
`LAB_PROXY_PORT`, `LAB_API_PORT`, `LAB_API_PORT_B` and `LAB_PG_PORT` values when
other local test processes run; the API boundary also uses `HANDOFF_API_PORT`.
The fixture verifies its owned process and RPC chain identity before proceeding.

Before release: build an immutable image; repeat the local protocol/API checks
against that image and a pinned compatible API image; execute dev ECS/ALB
warmup, continuous traffic, failure/restart and compatible rollback acceptance;
verify complete downstream no-refund/no-duplicate-credit settlement and indexing;
exercise the supported blob and restricted Base gateway lanes. EIP-7702 local
coverage does not replace the supported dev-chain check. Existing PR6 native E2E
cases and PR3 SDK/docs are retained; their release acceptance is carried forward.
No ECS execution, full rollup settlement, funded transaction, merge or deployment
was performed. Automatic top-up remains explicitly unsupported by protocol 1.

## Hetzner dev-host rehearsal — 2026-10-07

The exact `fd334cf13dcc3e54489f3b30ee448e33059324fd` runtime source was built on
the Hetzner dev host with Rust 1.96.1, `cargo build --locked --release --features
jemalloc -p rrelayer_cli`. Optimized binary SHA256:
`2b4e2952736e6118a6b8a4870cde44c8b0ef1881428e693dc51c0af1b76fd5a0`.
No runtime product code changed for this test.

Fourteen optimized-binary recovery scenarios passed, plus the actual legacy
dev binary duplicate-send control and drained bootstrap. The legacy baseline
SHA256 is `ecf594327ec682fe15553669ee8eddfa6dd75c90735551f564932a19240764c3`;
it is the running Hetzner binary, not a claim that it equals production source.
See `evidence/hetzner-process-verdicts.json`.

The isolated Nginx rehearsal passed 72/72 requests with HTTP 200, each producing
exactly one on-chain transfer. It exercised failed warmup while the old sender
served, standby intake, ownership transfer with pending/in-flight work, draining
and stopping the old backend, and compatible rollback. Both release labels use the same candidate binary; this
checks protocol ownership reversal, not compatibility between different code
versions. Max admission was
164.62 ms; max accepted-to-first-RPC-attempt was 371.51 ms. Original final rows,
receipts and events are in `evidence/hetzner-proxy-verdict.json`.

The actual API journal/client/refund-guard boundary was repeated against PR109
source `28d91ca86dcecb30271980591a92f7c051f0619d`, with the dev host's existing
Node 22.22.2 and installed dependency tree. Package dependency declarations
match; installed-lock/artifact parity was not fully verified. Its 135.84-second run crossed
the 120-second timeout, blocked cancellation and recovered one credit/same hash;
see `evidence/hetzner-api-boundary-verdict.json`.

These were separate processes, disposable databases, loopback ports and Anvil on
the real Linux dev host. They did not modify the shared Flow/Edge services or
use their live signers, rollup, escrows or funds. **This is not an ECS/ALB or full
dev-stack settlement pass.** Four debug-only crash barriers retain their earlier
debug evidence and are not claimed on this optimized binary.

Actual ECS rehearsal remains unavailable: the named `rrelayer-dev-cluster` does
not exist in ap-northeast-1. The dev candidate workflow run `37642467856` failed
before build/publication because `rrelayer-dev-gha` trusts only the
`feature/7702-support` branch and rejected the PR branch's OIDC identity. No
trust policy was changed and no branch was merged to bypass it. The API109 full
parent gate also retains five original non-7702 failures. Those limitations
remain separate prerequisites; local-chain results do not waive them.
