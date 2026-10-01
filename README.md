# shepherd

[![CI](https://github.com/magicorntech/shepherd/actions/workflows/ci.yml/badge.svg)](https://github.com/magicorntech/shepherd/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A small cluster janitor for Kubernetes. Its first job: **force-delete pods that
are stuck `Terminating`**, so a `Recreate` rollout (or anything else waiting for
the old pod object to disappear) isn't blocked forever.

- Event-driven (watches pods and nodes), no polling lag
- Works cluster-wide out of the box; a pod label overrides it per workload (exempt, or widen)
- Optional `--dry-run` to preview before acting
- Single static binary, distroless image, leader-elected, Prometheus metrics

## The problem

When a node dies hard (kernel panic, board failure, lost power), its kubelet
never confirms that the pods on it are gone. They sit in `Terminating`
indefinitely. A `Deployment` with `strategy: Recreate` waits for the old pods to
be *deleted* before creating new ones, so the whole workload stays down until a
human runs `kubectl delete pod --force`.

`terminationGracePeriodSeconds` does not help: it is enforced *by the kubelet*,
which is exactly the component that is gone. The same stall can also happen on a
healthy node when a container refuses to die.

## How it works

shepherd watches pods and nodes. A sweep runs immediately when a pod enters
`Terminating` or a node changes Ready state, and otherwise sleeps *exactly* until
the next pod's deadline (`--interval`, default 60s, is only a safety net against
a missed event). For each terminating pod it:

1. waits until `deletionTimestamp` (already = delete request + the pod's own
   grace period) **plus a buffer**: 30s if the node is dead, 5m if it is Ready;
2. force-deletes it (`gracePeriodSeconds=0`) with a UID precondition, so a
   recreated pod that reused the name is never hit.

A node is **dead** if its Node object is gone, or `Ready` is not `True`. Note
that Kubernetes itself only marks a silent node NotReady after
`node-monitor-grace-period` (40s by default), so total time-to-recovery is
roughly that + the pod's grace period + the buffer.

### Per-workload override

By default (`--default-mode=dead-node`) every pod in the cluster is covered. To
change that for one workload, put this label on its **pod template**:

```yaml
spec:
  template:
    metadata:
      labels:
        shepherd.magicorn.co/force-delete: dead-node
```

| value       | behaviour |
|-------------|-----------|
| `off`       | never touch: exempt this workload |
| `dead-node` | force-delete only when the node is dead; on a healthy node, alert only (the cluster default) |
| `any`       | also force-delete when stuck on a healthy node |

`any` is never the default, because it is a real trade-off: the
API object disappears at once while the kubelet may still be killing the
container, so for something holding an exclusive resource (a UDP port, a lock)
the old and new pod can briefly overlap. `--default-mode=off` flips shepherd to
true opt-in (only labelled pods are touched).

### What it will not force-delete (alert only)

| pod | why | override |
|-----|-----|----------|
| has **finalizers** | force delete does not remove it; the finalizer's owner must act | none |
| owned by a **StatefulSet** | would break at-most-one semantics | `--include-statefulset` |
| uses a **PVC / ephemeral volume** | RWO volumes stay attached to the dead node | `--include-pvc` |
| mirror pod / unscheduled | not shepherd's business | none |

### Safety rails

- **Optional dry-run.** `--dry-run=true` logs what it would delete and deletes nothing.
- **Circuit breaker.** If more than 30% of nodes (in clusters of 5+) are
  NotReady, that is more likely a network partition or control-plane problem
  than dead nodes. Dead-node deletes are suspended and alerted instead.
- **Per-sweep cap** (`--max-deletes-per-sweep`, default 50), oldest-stuck first.
- **Leader election** (Lease), so two replicas never both act.
- Every action produces a Kubernetes Event and a structured log line.

### Know the limits

Force-deleting a pod removes the *API object*; it cannot stop a container on a
node that is merely unreachable rather than dead. If a node is partitioned but
still running, its container keeps running while the controller starts a
replacement, i.e. a **brief duplicate**. For workloads where that is unsafe
(exclusive ports, single-writer data), pair shepherd with fencing
(power-cycle the node via BMC/cloud API before it is declared dead), or use the
`node.kubernetes.io/out-of-service` taint for non-graceful shutdown.

## Install

shepherd is deployed with Magicorn's `charts-deployment` Helm chart
(published to the [Magicorn ECR Public Gallery](https://gallery.ecr.aws/magicorn/charts-deployment));
this repo ships a ready-made [`deploy/values.yaml`](deploy/values.yaml) for it,
so there is no separate shepherd chart. Requires Helm 3.10+ and
`charts-deployment` **2.3.0 or newer** (the values use its PodDisruptionBudget
support).

```bash
helm upgrade --install shepherd oci://public.ecr.aws/magicorn/charts-deployment \
  --version 2.3.0 -n shepherd --create-namespace \
  -f https://raw.githubusercontent.com/magicorntech/shepherd/0.1.1/deploy/values.yaml
```

The values grant everything shepherd needs through the chart's own
`security.serviceAccount.rules`: pods, nodes, events and the Lease for leader
election. `charts-deployment` can only render a ClusterRole, so the Lease
permission is cluster-wide; to avoid that, run a single replica with
`--leader-elect=false`.

It acts immediately, cluster-wide. To preview first, install with
`--dry-run=true` and watch what it would do:

```bash
helm upgrade --install shepherd oci://public.ecr.aws/magicorn/charts-deployment \
  --version 2.3.0 -n shepherd --create-namespace \
  -f https://raw.githubusercontent.com/magicorntech/shepherd/0.1.1/deploy/values.yaml \
  --set 'global.deployment.image.args={--dry-run=true,--default-mode=dead-node,--dead-node-buffer=30s,--healthy-node-buffer=5m}'

kubectl -n shepherd logs deploy/shepherd -f | grep "would force-delete"
```

(`--set` replaces the whole `args` list, so repeat every flag you want; or keep
a local copy of `values.yaml`.) Re-run without the `--set` to go live.

To scrape metrics with the Prometheus Operator, point a ServiceMonitor at the
`shepherd` Service, port `metrics` (the chart does not render one).

### Uninstalling

`charts-deployment` renders the ServiceAccount, ClusterRole and ClusterRoleBinding
as Helm hooks, so `helm uninstall` leaves them behind:

```bash
helm uninstall shepherd -n shepherd
kubectl delete clusterrole,clusterrolebinding shepherd
kubectl -n shepherd delete serviceaccount shepherd
kubectl delete namespace shepherd   # optional
```

### Not using Helm?

The image is `public.ecr.aws/magicorn/shepherd:<version>` (linux/amd64 and
arm64). It needs a ServiceAccount with: `get,list,watch,delete` on `pods`,
`get,list,watch` on `nodes`, `create,patch` on `events`, and
`get,create,update` on `leases` (in its own namespace if you write a Role).

## Flags

| flag | default | |
|------|---------|--|
| `--dry-run` | `false` | log what would be deleted, delete nothing |
| `--default-mode` | `dead-node` | mode for pods without the label: `off`, `dead-node`, `any` |
| `--dead-node-buffer` | `30s` | extra wait after a pod's deletion deadline, node dead |
| `--healthy-node-buffer` | `5m` | extra wait after a pod's deletion deadline, node Ready |
| `--include-statefulset` | `false` | also force-delete StatefulSet pods |
| `--include-pvc` | `false` | also force-delete pods with PVC volumes |
| `--max-deletes-per-sweep` | `50` | blast-radius cap |
| `--breaker-not-ready-fraction` | `0.3` | breaker threshold; `0` disables |
| `--breaker-min-nodes` | `5` | breaker only applies at this cluster size or larger |
| `--namespace` | all | restrict to one namespace |
| `--interval` | `60s` | safety-net sweep interval |
| `--leader-elect` | `true` | |
| `--leader-election-namespace` | pod's own | |
| `--leader-election-name` | `shepherd` | |
| `--metrics-addr` | `:8080` | `/metrics`, `/healthz`, `/readyz` |
| `--log-json` | `true` | |
| `--log-level` | `info` | `debug` also logs each pod that is waiting for its deadline |
| `--kubeconfig` | in-cluster | for running locally |

## Logs

Structured JSON on stderr (`--log-json=false` for plain text).

| level | message | when |
|-------|---------|------|
| INFO | `shepherd started`, `became leader`, `stopped leading` | lifecycle |
| INFO | `force-deleted pod` (`would force-delete pod` with `--dry-run`) | the action itself; carries `pod`, `node`, `nodeState`, `mode`, `reason` |
| INFO | `pod already gone or replaced` | the UID precondition did its job |
| WARN | `pod stuck terminating, not force-deleting` | needs a human; carries `reason` (`finalizers`, `statefulset`, `pvc`, `healthy-node`, `circuit-open`) and `terminatingFor`. Logged once per pod per reason, then hourly as a reminder |
| WARN | `circuit breaker open` / INFO `circuit breaker closed` | on transitions only |
| WARN | `per-sweep delete cap reached, deferring` | more stuck pods than `--max-deletes-per-sweep` |
| ERROR | `force delete failed`, `list pods`, `list nodes` | unexpected API errors |
| DEBUG | `pod terminating, waiting for its deadline` | `--log-level=debug`; shows `remaining` |

`reason` on a deletion is `dead-node`, `node-missing` or `stuck-on-healthy-node`.
The same facts are also Kubernetes Events on the pod (`ShepherdForceDeleted`,
`ShepherdStuckTerminating`).

## Metrics

| metric | |
|--------|--|
| `shepherd_pods_force_deleted_total{reason,mode,dry_run}` | pods force-deleted (or that would have been) |
| `shepherd_terminating_pods{action,reason}` | terminating pods seen in the last sweep and what was decided |
| `shepherd_circuit_breaker_open` | 1 while dead-node deletes are suspended |
| `shepherd_delete_errors_total` | unexpected API errors on delete |
| `shepherd_sweep_duration_seconds` | |

Suggested alerts:

```yaml
- alert: ShepherdPodStuckTerminating
  # shepherd sees it but will not (or cannot) force-delete it: a human is needed
  expr: sum(shepherd_terminating_pods{action="alert"}) > 0
  for: 10m
- alert: ShepherdCircuitBreakerOpen
  expr: shepherd_circuit_breaker_open == 1
  for: 2m
```

## Development

```bash
make test     # gofmt + go vet + go test -race + version check + render deploy/values.yaml through charts-deployment 2.3.0
make build    # ./bin/shepherd
make image    # local docker build
./bin/shepherd --kubeconfig ~/.kube/config --leader-elect=false --dry-run=true   # preview against a cluster
```

The decision logic is a pure function in `internal/policy` (table-tested);
`internal/reaper` is the event/timer loop and the cross-pod safety rails.

## Releasing

One release number lives in the newest `CHANGELOG.md` heading and the image tag
(and header URLs) in `deploy/values.yaml`; `ci/check-versions.sh` enforces that
they agree, and equal the git tag on release. To release `X.Y.Z`: bump them,
add the CHANGELOG entry, merge, then push the tag `X.Y.Z`. The Release workflow
(GitHub-hosted runner) runs the full test suite and pushes
`public.ecr.aws/magicorn/shepherd:X.Y.Z` for amd64 and arm64. ECR Public tags are
mutable, so it refuses to publish a version that already exists.

The workflow authenticates to AWS with OIDC: set the repo variable
`AWS_ECR_ROLE_ARN` to a role that may push to the `shepherd` ECR Public repo.

## License

MIT, see [LICENSE](LICENSE).
