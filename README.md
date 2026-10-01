# shepherd

[![CI](https://github.com/magicorntech/shepherd/actions/workflows/ci.yml/badge.svg)](https://github.com/magicorntech/shepherd/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A small cluster janitor for Kubernetes. Its first job: **force-delete pods that
are stuck `Terminating`**, so a `Recreate` rollout (or anything else waiting for
the old pod object to disappear) isn't blocked forever.

- Event-driven (watches pods and nodes), no polling lag
- **Opt-in per workload** via a pod label; touches nothing by default
- **Dry-run by default**
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

### Opting a workload in

Nothing is touched unless the **pod template** carries this label:

```yaml
spec:
  template:
    metadata:
      labels:
        shepherd.magicorn.co/force-delete: dead-node
```

| value       | behaviour |
|-------------|-----------|
| `off`       | never touch (the default) |
| `dead-node` | force-delete only when the node is dead; on a healthy node, alert only |
| `any`       | also force-delete when stuck on a healthy node |

Use `dead-node` unless you have a reason not to. `any` is a real trade-off: the
API object disappears at once while the kubelet may still be killing the
container, so for something holding an exclusive resource (a UDP port, a lock)
the old and new pod can briefly overlap. `--default-mode` changes the default
for pods without the label.

### What it will not force-delete (alert only)

| pod | why | override |
|-----|-----|----------|
| has **finalizers** | force delete does not remove it; the finalizer's owner must act | none |
| owned by a **StatefulSet** | would break at-most-one semantics | `--include-statefulset` |
| uses a **PVC / ephemeral volume** | RWO volumes stay attached to the dead node | `--include-pvc` |
| mirror pod / unscheduled | not shepherd's business | none |

### Safety rails

- **Dry-run by default.** Pass `--dry-run=false` to act.
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
so there is no separate shepherd chart. Requires Helm 3.10+.

The release name and namespace must both be `shepherd` (the leader-election RBAC
in `extras.yaml` is written for exactly that).

```bash
# 1. namespace + leader-election Role + PodDisruptionBudget
kubectl apply -f https://raw.githubusercontent.com/magicorntech/shepherd/0.1.0/deploy/extras.yaml

# 2. shepherd itself
helm upgrade --install shepherd oci://public.ecr.aws/magicorn/charts-deployment \
  --version 2.2.0 -n shepherd \
  -f https://raw.githubusercontent.com/magicorntech/shepherd/0.1.0/deploy/values.yaml
```

`extras.yaml` exists because `charts-deployment` can only grant a ClusterRole,
and Lease create/update cluster-wide would let shepherd touch control-plane
leases in `kube-system`. Leases are therefore a Role scoped to the `shepherd`
namespace.

It starts in **dry-run**. Watch what it would do:

```bash
kubectl -n shepherd logs deploy/shepherd -f | grep "would force-delete"
```

When the decisions look right, switch it on. Either edit a local copy of
`values.yaml` (`--dry-run=false` in `global.deployment.image.args`), or:

```bash
helm upgrade shepherd oci://public.ecr.aws/magicorn/charts-deployment \
  --version 2.2.0 -n shepherd --reuse-values \
  --set 'global.deployment.image.args={--dry-run=false,--default-mode=off,--dead-node-buffer=30s,--healthy-node-buffer=5m}'
```

(`--set` replaces the whole `args` list, so repeat every flag you want.)

To scrape metrics with the Prometheus Operator, point a ServiceMonitor at the
`shepherd` Service, port `metrics` (the chart does not render one).

### Uninstalling

`charts-deployment` renders the ServiceAccount, ClusterRole and ClusterRoleBinding
as Helm hooks, so `helm uninstall` leaves them behind:

```bash
helm uninstall shepherd -n shepherd
kubectl delete clusterrole,clusterrolebinding shepherd
kubectl delete namespace shepherd   # also removes the ServiceAccount, Role, PDB
```

### Not using Helm?

The image is `public.ecr.aws/magicorn/shepherd:<version>` (linux/amd64 and
arm64). It needs a ServiceAccount with: `get,list,watch,delete` on `pods`,
`get,list,watch` on `nodes`, `create,patch` on `events`, and
`get,create,update` on `leases` in its own namespace.

## Flags

| flag | default | |
|------|---------|--|
| `--dry-run` | `true` | log what would be deleted, delete nothing |
| `--default-mode` | `off` | mode for pods without the label: `off`, `dead-node`, `any` |
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
| `--kubeconfig` | in-cluster | for running locally |

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
make test     # gofmt + go vet + go test -race + version check + render deploy/values.yaml through charts-deployment 2.2.0
make build    # ./bin/shepherd
make image    # local docker build
./bin/shepherd --kubeconfig ~/.kube/config --leader-elect=false   # dry-run against a cluster
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
