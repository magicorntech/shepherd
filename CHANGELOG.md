# Changelog

## [0.1.0]

First release.

- Force-deletes pods stuck `Terminating` on dead nodes (`dead-node` mode) or on
  healthy nodes (`any` mode), opt-in per pod via
  `shepherd.magicorn.co/force-delete`.
- Event-driven: sweeps on pod/node watch events and sleeps until the next
  pod's exact deadline; periodic sweep is only a safety net.
- Safety: dry-run by default, UID-preconditioned deletes, circuit breaker on
  NotReady-node fraction, per-sweep cap, finalizer/StatefulSet/PVC pods are
  alert-only, Lease leader election.
- Prometheus metrics, Kubernetes Events, structured logs.
- Ready-made `deploy/values.yaml` for `charts-deployment` 2.2.0 plus
  `deploy/extras.yaml` (namespaced Lease Role, PDB). Image
  `public.ecr.aws/magicorn/shepherd` for linux/amd64 and linux/arm64.
