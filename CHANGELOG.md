# Changelog

## [0.2.0]

Second job (`evicted-pods`) and a modular job framework.

### Added

- **`evicted-pods` job**: deletes pods the kubelet evicted (`Failed` / reason
  `Evicted`, nothing else) once they are older than `--evicted-ttl-minutes`
  (default 1440 = 24h). Age counts from when the pod became Failed. Opt a pod
  out with the label `shepherd.magicorn.co/evicted-cleanup: "off"`. Deletes are
  UID-preconditioned and capped per sweep (`--evicted-max-deletes-per-sweep`,
  default 200, oldest first); the eviction message is logged before the pod goes.
  New metrics `shepherd_evicted_pods_deleted_total`, `shepherd_evicted_pods`,
  `shepherd_evicted_delete_errors_total`.
- **Modular jobs**: `--exclude-jobs=<name>[,<name>]` turns jobs off (all run by
  default; unknown names are an error). `--dry-run` applies to every job. Log
  lines now carry a `job` attribute.

### Changed

- **Behaviour change on upgrade:** the new job is on by default, so an upgraded
  install starts deleting evicted pods older than 24h cluster-wide. Add
  `--exclude-jobs=evicted-pods` to keep the old behaviour. No RBAC change: the
  `pods` delete permission is already granted.

### Internal

- Jobs live under `internal/job/<name>`, sharing a `Job` interface and a
  `Runner`; `internal/policy` and `internal/reaper` moved to
  `internal/job/stuckpods/policy` and `internal/job/stuckpods`.
- Startup wiring moved from `cmd/shepherd` into `internal/shepherd` so it can be
  run by tests.
- Integration tests against a real kube-apiserver (envtest): live triggers,
  leader election, UID preconditions, and the exact RBAC shipped in
  `deploy/values.yaml`. No behaviour change from these.

## [0.1.1]

Logging fixes.

- Durations in log lines are human-readable strings (`"terminatingFor":"10m0s"`)
  instead of raw nanoseconds.
- A pod that stays stuck is reported once (log line + Kubernetes Event), not on
  every sweep: again only if the reason changes, or as an hourly reminder. The
  circuit breaker logs on open/close transitions instead of every sweep.
- New `--log-level` flag (`debug|info|warn|error`, default `info`). At `debug`,
  every pod that is still waiting for its deadline is logged with the time
  remaining.

## [0.1.0]

First release.

- Force-deletes pods stuck `Terminating` on dead nodes (`dead-node` mode) or on
  healthy nodes (`any` mode). Cluster-wide by default (`dead-node`); the pod
  label `shepherd.magicorn.co/force-delete` overrides it per workload.
- Event-driven: sweeps on pod/node watch events and sleeps until the next
  pod's exact deadline; periodic sweep is only a safety net.
- Safety: optional `--dry-run`, UID-preconditioned deletes, circuit breaker on
  NotReady-node fraction, per-sweep cap, finalizer/StatefulSet/PVC pods are
  alert-only, Lease leader election.
- Prometheus metrics, Kubernetes Events, structured logs.
- Ready-made `deploy/values.yaml` for `charts-deployment` 2.3.0 (RBAC incl.
  leader-election Lease, PodDisruptionBudget). Image
  `public.ecr.aws/magicorn/shepherd` for linux/amd64 and linux/arm64.
