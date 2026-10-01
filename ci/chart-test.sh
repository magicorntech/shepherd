#!/usr/bin/env bash
# Renders deploy/values.yaml through the real, published charts-deployment
# chart and asserts on the result. Plain helm + grep on purpose: no plugin to
# install, runs identically on a laptop and in CI. Needs network (pulls the
# chart from the ECR Public gallery).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CHART=oci://public.ecr.aws/magicorn/charts-deployment
CHART_VERSION=2.2.0

render() { helm template shepherd "$CHART" --version "$CHART_VERSION" -n shepherd -f deploy/values.yaml "$@"; }
fail=0
must()    { if ! grep -qE -- "$2" <<<"$OUT"; then echo "FAIL [$1]: expected /$2/" >&2; fail=1; fi; }
mustnot() { if grep -qE -- "$2" <<<"$OUT"; then echo "FAIL [$1]: unexpected /$2/" >&2; fail=1; fi; }

OUT="$(render)"
must    defaults 'kind: Deployment'
must    defaults 'replicas: 2'
must    defaults '--dry-run=true'            # safe by default
must    defaults '--default-mode=off'        # opt-in by default
must    defaults 'kind: "ClusterRole"'
must    defaults 'serviceAccountName: shepherd'
must    defaults 'readOnlyRootFilesystem: true'
must    defaults 'runAsNonRoot: true'
must    defaults 'image: "public.ecr.aws/magicorn/shepherd:'
mustnot defaults 'kind: Ingress'
# Least privilege: no wildcards, and no Lease access in the ClusterRole.
RULES="$(awk '/kind: "ClusterRole"/{f=1} f' <<<"$OUT" | awk '/^---/{exit} {print}')"
grep -qE '"\*"' <<<"$RULES" && { echo "FAIL [rbac]: wildcard in ClusterRole" >&2; fail=1; }
grep -q 'leases' <<<"$RULES" && { echo "FAIL [rbac]: leases must be namespaced (extras.yaml), not cluster-wide" >&2; fail=1; }
grep -q '"delete"' <<<"$RULES" || { echo "FAIL [rbac]: pods/delete missing" >&2; fail=1; }

# Flipping dry-run off through --set works as the README documents.
OUT="$(render --set 'global.deployment.image.args={--dry-run=false}')"
mustnot live '--dry-run=true'
must    live '--dry-run=false'

# extras.yaml's names must match what the chart renders (SA and selector
# labels), or leader election / the PDB silently stop working.
OUT="$(render)"
must    extras 'app.kubernetes.io/name: shepherd'
must    extras 'app.kubernetes.io/instance: shepherd'
grep -q 'name: shepherd$' <<<"$(grep -A3 'subjects:' deploy/extras.yaml)" || { echo "FAIL [extras]: RoleBinding subject must be SA shepherd" >&2; fail=1; }

[ "$fail" = 0 ] || exit 1
echo "chart tests passed"
