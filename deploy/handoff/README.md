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
`RRELAYER_RELEASE_ID`. It does not update ECS. Register the reviewed task
definition through the release process and supply its ARN to the controller:

```sh
python3 deploy/handoff/promote.py \
  --cluster <cluster> --service <service> --container <container> \
  --task-definition <registered-candidate-arn> \
  --source <full-commit> --digest sha256:<digest> \
  --acceptance <exact-image-acceptance.json> --journal <private-journal.json>
```

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
