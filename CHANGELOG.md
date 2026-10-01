# Changelog

## [Unreleased]

- Integration tests against a real kube-apiserver (envtest); no behaviour change.
- Internal: startup wiring moved from `cmd/shepherd` into `internal/shepherd`
  so it can be run by tests.

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
