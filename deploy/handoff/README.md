# Safe sender handoff — protocol 1

This candidate replaces a deployment-time stop/start outage with a prewarmed
standby and an explicit sender handoff. It is **not production accepted** until
an exact immutable image passes dev ECS and downstream settlement acceptance.
The controller defaults to a read-only plan; only the separately authorized
release process runs `--execute`. No image or service was deployed by this PR.

## Safety contract

The existing `relayer.transaction` table is the queue of record. Process queues
are reconstructed caches. Every admission and queue step acquires a PostgreSQL
session advisory lock keyed by chain and signer, then installs a new durable
owner token. A trigger checks that token, holds the epoch row through commit,
and rejects stale writes, nonce changes and changes to attempted payloads.
Transactions reserve a nonce across all relayer records sharing that signer.
A failed unlock discards its database connection. Abandoned sessions expire
at 15 seconds; database statements and abandoned transactions have 10-second
limits. These limits bound takeover, not chain finality or RPC availability.

A lease/fence alone cannot retract an RPC from a paused process. Before every
ordinary RPC broadcast, the signed envelope, hash and fees commit against the
original transaction. Recovery replays those bytes at the original nonce and
checks every signed attempt for a receipt. Fee bumps retain the nonce. A stale
RPC can therefore only compete at the same nonce; it cannot execute the same
value again at a fresh nonce. Base's restricted lane retains its existing
durable signed envelope, nonce guard and prohibition on replacement/cancel.

Cancellation and in-mempool replacement first commit a competitor and its
explicit kind/link to the original. A receipt determines the winner; winner,
loser and audit statuses commit together. An attempted pending payload cannot
be edited. A genuinely unattempted expired request queues a durable no-op at
its nonce; it becomes EXPIRED only after that competitor wins. Expiration or
an RPC error never proves an attempted payout failed. Unresolved sends can
remain pending indefinitely and require diagnosis; they are never silently
renumbered, refunded, or labelled failed.

`/health` remains liveness. `/ready` reports protocol, release, configured queue
coverage, durable-intake readiness, processing eligibility, active release and
unresolved count. The authenticated `/handoff/activate` endpoint compares the
expected active release before changing it. Standby protocol-1 tasks may accept
durable requests; only the active release processes them. Requests with a
lost response must retain their identity. The new UUID-keyed `send-idempotent`
route returns the original request for exact retries and rejects changed
payloads (including changed authorizations). Legacy `send`/`send-random` remain
correlation-only APIs; blindly repeating those POSTs is not safe. The adjacent
API's durable send journal performs lookup before recovery instead of repeating
an ambiguous POST. Deploying this relayer alone does not fix older API timeout
or refund behavior.

`GET /transactions/:id/prebroadcast-failure` returns either `null` or
`{ "kind": "terminal_prebroadcast_v1", "transaction": <original transaction> }`.
It requires enabled sender fencing, a UUID-keyed request originally persisted
as FAILED before signing/queueing, no signed attempt or attempted audit history,
no competing external-ID row, and unchanged payload. It bypasses the status
cache. The existing keyed endpoint cannot restart such a rejection, including
after refill/restart. FAILED or an absent hash alone does not establish this
proof. Deploy this endpoint before the API's accelerated reservation recovery;
the API still owns exact request binding, stopped execution, payout checks and
the native signed cancellation window. No rollup/contract policy changes.

`scripts/test-idempotent-submission.mjs` covers the proof using real local
PostgreSQL/Anvil, including auth, refill/restart and attempted-history refusal.

Automatic top-up configurations are rejected at startup before database or
background work. Their current periodic producers create independent requests
per process and are not safe under overlap. The checked production/dev YAML
contains none. Supporting that optional producer requires a separate durable
idempotent producer change before enabling it with protocol 1; it must never be
silently run by both tasks.

## First deployment from legacy

Schema changes are additive and fencing initially stays disabled. The candidate
has no processing or intake authority before bootstrap. It refuses activation
while any PENDING, INMEMPOOL or MINED legacy row remains. Historical FAILED or
otherwise terminal legacy rows are not proof that every prior economic effect
is known: reconcile outstanding incidents before migration.

1. Record the existing service task definition, actual running image digest,
   source provenance, configuration and task inventory. Keep min=0/max=100.
2. Prove the compatible API image retains ambiguous work without refund or
   repeated value transfer. Pause producers with durable retry; record the
   ingress-quiescence evidence and drain accepted legacy work to CONFIRMED.
3. Prewarm the accepted candidate with the current task configuration. Check
   protocol, source, actual running digest and every configured queue.
4. Stop **all** legacy sender tasks and verify STOPPED. The controller rechecks
   unresolved state at activation; a racing legacy admission blocks bootstrap.
5. Disable ECS automatic rollback, activate protocol 1, register the warm target
   and wait for target health. Update the service to that exact digest while the
   warm task serves. After the service is healthy, drain and retire the warm task.

First bootstrap includes a controlled intake interruption and ALB health delay.
It is not claimed to be zero-downtime. Never overlap an active protocol-1 sender
with a legacy binary; the legacy binary can broadcast before its fenced DB
write fails. Never roll back to a pre-protocol image after activation, and never
turn fencing off to force a rollback. Preserve ambiguous rows and reconcile.

## Subsequent deployments and compatible rollback

The controller starts a temporary Fargate task with the existing network and
task configuration, verifies the accepted digest/readiness and registers it in
the existing IP target group. Old processing continues during warmup. Once the
warm target is healthy, activation changes the active release atomically. It
then replaces the service, still min=0/max=100. Both same-release tasks may run
briefly; the per-signer database fence serializes them. Retirement happens only
after the service's actual digest, processing readiness and ALB health pass.

Failure before activation leaves the old release active. Failure after
activation preserves the warm task; do not stop it or delete the journal.
`--resume` reconciles the same release and deployment target. A compatible
rollback uses a previously accepted protocol-1 digest/source with a new journal
and `--recover-journal` for the interrupted deployment. It performs the same
prewarm/activate/replace sequence; DB rows and attempts are retained. Automatic
ECS rollback is disabled because it bypasses this protocol. A completely failed
warm task after activation requires a new accepted recovery image/task; existing
requests remain durable while processing is unavailable.

Both dev and production build workflows only build and export an immutable candidate. Each tag is
SHA/run-specific; the rendered definition pins the image digest and
`RRELAYER_RELEASE_ID`. It does not update ECS.

For a pre-merge production image build, keep the workflow on its trusted branch
and select the reviewed source commit explicitly:

```sh
gh workflow run deploy-prod.yml --repo pepper-research/rrelayer \
  --ref feature/7702-support \
  -f source_revision=FULL_40_CHARACTER_LOWERCASE_SHA
```

The workflow ref supplies the workflow and its existing OIDC identity. The
optional source revision selects the checkout; the verified checkout SHA identifies
the image, release ID and candidate artifacts. Without that input, the workflow
uses its own commit. This only builds and exports a candidate with `accepted:false`;
it does not register a task definition, start a task or activate a sender.

Register the reviewed task definition through the release process and supply its ARN
to the controller:

```sh
python3 deploy/handoff/promote.py \
  --cluster <cluster> --service <service> --container <container> \
  --task-definition <registered-candidate-arn> \
  --source <full-commit> --digest sha256:<digest> \
  --acceptance <exact-image-acceptance.json> --journal <private-journal.json>
```

The named ECS dev cluster was absent when checked on 2026-10-07; the active
shared Flow/Edge dev relayer is a Hetzner systemd service. Provision and verify
an ECS dev target before claiming this controller has been rehearsed there.
The dev build role also rejected OIDC from the PR branch; no image was built.

For the initial dev acceptance run, add `--dev-validation`. That mode is
restricted to `rrelayer-dev-cluster` / `rrelayer-dev` / container `rrelayer-dev`
in ap-northeast-1. It still requires the exact-image local recovery checks and
API journal boundary evidence, but does not require the dev ECS/full settlement
results that the run is about to produce. A dev receipt is rejected for normal
production promotion. Dev must first use the same stop-before-start service
configuration; changing an existing dev service remains release work.

The first invocation saves its plan. The authorized execution uses the same
arguments plus `--resume --execute`, and `--bootstrap` only for the first
migration. AWS credentials are inherited from the approved secret mechanism.
ECS Exec authentication stays inside the container; secrets are never written
to the journal. Required access: describe/list tasks/service/task definitions,
run/stop tasks, update service, ECS Exec, and target group register/deregister/
health reads. Existing task roles, image credentials, SSM/Exec networking and
image-only task compatibility must be verified in dev. The controller refuses
unaccounted running tasks and task configuration changes beyond image/release.

Acceptance JSON must bind `source`, `image_digest`, `protocol:1`, and the exact
`downstream_image_digest`. Every key in `promote.REQUIRED` needs
`{"passed":true,"evidence":"<durable test report>"}`. Bootstrap also needs
`legacy_ingress_quiesced_evidence`. Local process results cannot stand in for
`dev_ecs_handoff` or full downstream credit/settlement acceptance. Do not create
an all-green receipt from the local results below.

## Verification

```sh
cargo build --locked -p rrelayer_cli
cargo test --locked -p rrelayer_core --lib -- --include-ignored
python3 -m unittest discover -s deploy/handoff -p 'test_*.py'
python3 scripts/handoff/test_handoff.py \
  --binary "$PWD/target/debug/rrelayer_cli" --out /tmp/handoff-evidence
```

Use `RRELAYER_TEST_DATABASE_URL` for the fixed-lane database unit test; without
it that test returns early. The ignored provider test needs Anvil on PATH.
The process suite needs PostgreSQL 16 (`LAB_PG_BIN`), Anvil, Cast and Python 3.
It uses only loopback RPC, disposable databases and Anvil's public development
mnemonic. Test barriers exist only in debug builds. No funded test is required.
To reproduce the red control and migration against the production-line source:

```sh
python3 scripts/handoff/test_handoff.py --binary <candidate> \
  --baseline <fc554ded-binary> --out /tmp/handoff-legacy \
  --scenarios legacy_control,legacy_bootstrap
```

For API boundary proof, freeze the adjacent API candidate's source, migrations,
tests and installed dependency tree without editing its active checkout. Save
a content manifest/base SHA. Run:

```sh
python3 scripts/handoff/test_api_boundary.py --binary <candidate> \
  --api-snapshot <frozen-api-checkout> --out /tmp/handoff-api
```

That harness runs the actual API journal, HTTP relayer client and cancellation
guard against real PostgreSQL, two relayer processes and Anvil. It holds the
receipt beyond 120 seconds, attempts cancellation at 5/125 seconds, then repeats
recovery and asserts one journal, one relayer transaction, one on-chain credit,
matching nonce/hash and no refund. It uses a native payout and a persisted escrow
fixture; it does **not** execute full rollup settlement or deposit indexing.
The explicit egress guard refuses non-loopback HTTP. The shared lab/proxy
fixtures originated in the existing rrelayer nonce-used reproduction harness.

Release acceptance must additionally measure ALB request success and dispatch
gaps with continuous traffic, pending and in-flight work, a killed old sender,
a paused stale sender, database reconnect, failed warmup, failed service
replacement, controller restart and compatible rollback, on the exact container
and actual dev ECS target group. Record maximum accepted-to-first-broadcast
latency separately from request latency and queue-drain time. Verify EIP-7702,
blob signing and the restricted Base gateway on their supported dev lanes.
Production is unchanged until that engineering evidence and scoped release
execution are complete.

## Isolated Hetzner rehearsal

On the Linux dev host, `scripts/handoff/test_dev_proxy.py` exercises an optimized
release binary behind its own Nginx listener, with disposable PostgreSQL/Anvil
and separate ports. It covers continuous admission through failed warmup,
standby registration, processing activation, old-worker drain and compatible
rollback. It does not modify the host's shared Nginx, Flow/Edge services or
signers. Nginx and PostgreSQL/Foundry must already be available on PATH.

```sh
LAB_PG_BIN=/usr/lib/postgresql/16/bin \
LAB_ANVIL_PORT=28957 LAB_PROXY_PORT=28958 \
LAB_API_PORT=28959 LAB_API_PORT_B=28960 LAB_PG_PORT=28961 \
HANDOFF_PROXY_PORT=28962 python3 scripts/handoff/test_dev_proxy.py \
  --binary <optimized-rrelayer-cli> --out <private-evidence-directory>
```

Run as an unprivileged user; all listeners are loopback. This is a dev-host
rehearsal using a controlled local chain, **not** ECS/ALB, the shared live dev
stack, or full downstream settlement acceptance. It cannot satisfy those
production-promotion checks.


## Isolated restricted Base qualification

The restricted lane remains Base 8453, normal wallet index 0 (not private-key
mode). By default its signer and gateway are the existing production values;
`RRELAYER_LIMIT_RPC_GATEWAY_ENABLED=true` is still required to start its queue.
The configuration is validated and frozen when providers load; changing it
requires a process restart.

For an isolated deployment set **both**:

- `RRELAYER_LIMIT_RPC_SIGNER_ADDRESS`: the controlled normal wallet 0 address.
- `RRELAYER_LIMIT_RPC_GATEWAY_URL`: the isolated API endpoint ending in
  `/internal/orders/base-rpc`.

A partial pair, production signer in an override, production gateway host in an
override, zero/invalid address, URL credentials/query/fragment or wrong path
fails startup. The URL must use HTTPS, except literal loopback IPs may use HTTP
for local process tests. Gateway requests do not follow redirects. This does not
replace network/custody isolation: operators must use a separate database,
controlled signer and dedicated credentials, and verify the actual endpoint and
signer's ownership. Do not redirect production DNS or copy production secrets.

Set the isolated API's existing `LIMIT_ORDER_BASE_RELAYER_ADDRESS` to the same
address and its relayer ID/chain settings to that actual relayer and 8453.
The existing relayer `RRELAYER_AUTH_USERNAME`/`RRELAYER_AUTH_PASSWORD` must match
the isolated API's `RELAYER_USER`/`RELAYER_PASS`. Use its supported pinned dev
`LIMIT_ORDER_BASE_RPC_URL` for real Base acceptance. Existing gateway envelope,
signer, budget and transaction-debit checks remain unchanged. Release spending
policy does not override gateway product policy.

The provider and cancel/replace/random-send checks share the configured signer.
A relayer record with a different address remains offline; ordinary Base RPC is
never a fallback for this lane. Use the explicit relayer-ID `/send` route, not
random or chain-scoped `/send-idempotent`, which intentionally exclude it.

### Local fault rehearsal

Use a frozen tx-submission-api source snapshot with its installed dependencies,
not another session's mutable source tree. Record the API commit and source
hashes. No package installation or live credentials are required when reusing
an existing matching dependency tree. With PostgreSQL 16, Anvil, cast and Bun:

```sh
cargo build --locked -p rrelayer_cli
cargo test --locked -p rrelayer_core provider:: --lib
python3 scripts/handoff/test_restricted_base.py \
  --binary "$PWD/target/debug/rrelayer_cli" \
  --api-snapshot /absolute/frozen-api-snapshot \
  --out /absolute/disposable-test-directory
```

The fixture runs a local 8453 chain and PostgreSQL with a public test mnemonic,
imports the real API gateway/guard/forwarder/meter, and prevents non-loopback
HTTP in that gateway process. Its budget reservation store is deterministic
fault injection, **not** acceptance of SQL budget accounting or the approved
Alchemy transport. It tests signer/auth/envelope rejection, queue readiness,
wrong signer isolation, route restrictions, no direct-provider fallback,
pre-forward rejection, restart recovery, uncertain forwarding with hidden
receipts, original signed bytes, exactly-once on-chain effects and redirects.
It stops its child processes and removes its temporary database on exit. Never
fund the public test mnemonic on a real chain.

Run the ordinary handoff restart-gap and failed-attempt-nonce-reservation cases
as well; those are a separate lane and must not be replaced by this rehearsal.

### Release and rollback integration

This is a runtime change on PR17's existing line. Publish the complete candidate
through that PR, then qualify the exact resulting immutable image. The prior
391025a checks are historical evidence, not acceptance of the new runtime.
PR7 may use a newly qualified protocol 1 artifact as its rollback; the old b6
artifact remains invalid for restart-gap recovery. PR9 must accept its actual
combined deployment tree, which inherits this lane from its target.

The existing release owner still owes exact-image ECS handoff/failure/rollback,
restricted Base live transport, supported blob/EIP7702 and downstream
settlement/latest-client checks. Keep production configuration at its defaults
and verify override variables are absent before promotion. Never roll back an
active protocol 1 deployment to protocol 0 or overlap senders. API167 requires
verified PR17 activation; API115/UI224 require verified nonce-safe activation.
A passing local fixture removes the configuration limitation on the new source;
it does not mark any dependency shipped or waive these acceptance obligations.
