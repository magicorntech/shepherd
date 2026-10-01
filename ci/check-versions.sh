#!/usr/bin/env bash
# One release number, three places: the newest CHANGELOG heading, the image
# tag in deploy/values.yaml (and the URLs in its header), and, on a release
# tag, the git tag itself. A user who copies values.yaml from tag X.Y.Z must
# get image X.Y.Z.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

top="$(grep -m1 -oE '^## \[[0-9]+\.[0-9]+\.[0-9]+\]' CHANGELOG.md | tr -d '#[] ')"
image="$(grep -E 'uri: public.ecr.aws/magicorn/shepherd:' deploy/values.yaml | sed -E 's/.*shepherd://')"
urls="$(grep -oE 'magicorntech/shepherd/[0-9]+\.[0-9]+\.[0-9]+/deploy' deploy/values.yaml | sed -E 's#.*/([0-9.]+)/deploy#\1#' | sort -u || true)"

fail=0
check() { [ "$2" = "$top" ] || { echo "FAIL: $1 is '$2', CHANGELOG top is '$top'" >&2; fail=1; }; }
check "values.yaml image tag" "$image"
for v in $urls; do check "values.yaml header URL" "$v"; done
if tag="$(git describe --exact-match --tags HEAD 2>/dev/null)"; then
  check "git tag" "$tag"
fi

[ "$fail" = 0 ] || exit 1
echo "versions agree: $top"
