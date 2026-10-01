#!/usr/bin/env bash
# Renders deploy/values.yaml through the real, published charts-deployment
# chart and asserts on the result. Plain helm + grep on purpose: no plugin to
# install, runs identically on a laptop and in CI. Needs network (pulls the
# chart from the ECR Public gallery).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# CHART can point at a local checkout (CHART=../charts/deployment) to test
# against an unreleased chart; --version only applies to the OCI ref.
CHART="${CHART:-oci://public.ecr.aws/magicorn/charts-deployment}"
CHART_VERSION=2.3.0
VERSION_ARGS=(--version "$CHART_VERSION")
[[ "$CHART" == oci://* ]] || VERSION_ARGS=()

render() { helm template shepherd "$CHART" ${VERSION_ARGS[@]+"${VERSION_ARGS[@]}"} -n shepherd -f deploy/values.yaml "$@"; }
fail=0
must()    { if ! grep -qE -- "$2" <<<"$OUT"; then echo "FAIL [$1]: expected /$2/" >&2; fail=1; fi; }
mustnot() { if grep -qE -- "$2" <<<"$OUT"; then echo "FAIL [$1]: unexpected /$2/" >&2; fail=1; fi; }

OUT="$(render)"
must    defaults 'kind: Deployment'
must    defaults 'replicas: 2'
mustnot defaults '--dry-run=true'            # acts by default
must    defaults '--default-mode=dead-node'  # cluster-wide, dead nodes only
mustnot defaults '--default-mode=any'        # never the default
must    defaults 'kind: "ClusterRole"'
must    defaults 'serviceAccountName: shepherd'
must    defaults 'kind: PodDisruptionBudget'
must    defaults 'minAvailable: 1'
must    defaults 'readOnlyRootFilesystem: true'
must    defaults 'runAsNonRoot: true'
must    defaults 'image: "public.ecr.aws/magicorn/shepherd:'
mustnot defaults 'kind: Ingress'
# Least privilege: no wildcards; leases present (leader election needs them).
RULES="$(awk '/kind: "ClusterRole"/{f=1} f' <<<"$OUT" | awk '/^---/{exit} {print}')"
grep -qE '"\*"' <<<"$RULES" && { echo "FAIL [rbac]: wildcard in ClusterRole" >&2; fail=1; }
grep -q 'leases' <<<"$RULES" || { echo "FAIL [rbac]: leases missing; leader election would fail" >&2; fail=1; }
grep -q '"delete"' <<<"$RULES" || { echo "FAIL [rbac]: pods/delete missing" >&2; fail=1; }

# Previewing through --set works as the README documents.
OUT="$(render --set 'global.deployment.image.args={--dry-run=true,--default-mode=dead-node}')"
must    preview '--dry-run=true'

[ "$fail" = 0 ] || exit 1
echo "chart tests passed"
