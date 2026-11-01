#!/usr/bin/env bash
# Verify an already-built Linux archive in a fresh runtime with no Node/source.
set -euo pipefail
archive=${1:?expected Linux standalone archive}
report=${2:?expected new report path}
test "$#" -eq 2
test -f "$archive"
test ! -e "$report"
image=debian@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3
scratch=$(mktemp -d "${TMPDIR:-/tmp}/git-installed.XXXXXXXX")
container="git-installed-$(basename "$scratch")"
started=false
cleanup() {
  if "$started"; then docker rm -f "$container" >/dev/null; fi
  rm -rf "$scratch"
}
trap cleanup EXIT
docker run -d --name "$container" --workdir /rehearsal "$image" sleep 900 >/dev/null
started=true
docker exec "$container" sh -c 'apt-get update && apt-get install -y --no-install-recommends git openssh-client ca-certificates libc6 libstdc++6 libgcc-s1 libatomic1 python3'
docker cp "$archive" "$container:/rehearsal/release.tar.gz"
docker exec "$container" sh -c 'mkdir install && tar -xzf release.tar.gz --strip-components=1 -C install'
# Generate short-lived test credentials only after runtime installation finishes.
node tools/prepare-standalone-fixture.ts "$scratch/fixture"
docker cp "$scratch/fixture" "$container:/rehearsal/fixture"
docker cp tools/verify-installed.py "$container:/rehearsal/verify-installed.py"
docker exec -e "REHEARSAL_IMAGE=$image" "$container" python3 verify-installed.py install fixture /rehearsal/report.json /rehearsal/release.tar.gz
docker cp "$container:/rehearsal/report.json" "$scratch/report.json"
node --input-type=module -e '
  import { copyFile, constants } from "node:fs";
  copyFile(process.argv[1], process.argv[2], constants.COPYFILE_EXCL, error => {
    if (error) throw error;
  });
' "$scratch/report.json" "$report"
