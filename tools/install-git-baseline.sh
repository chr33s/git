#!/usr/bin/env bash
set -euo pipefail

version=2.56.1
prefix="${RUNNER_TEMP:?This installer is for GitHub Actions}/git-${version}"
archive="${RUNNER_TEMP}/git-${version}.tar.xz"
source_dir="${RUNNER_TEMP}/git-${version}"

# Ubuntu mirrors drop connections often enough to fail whole CI runs; retry
# each fetch inside apt and the whole update+install a few times around it.
packages=(autoconf build-essential cargo curl gettext libcurl4-openssl-dev libexpat1-dev xz-utils zlib1g-dev)
for attempt in 1 2 3; do
  if sudo apt-get -o Acquire::Retries=5 update &&
    sudo apt-get -o Acquire::Retries=5 install --yes "${packages[@]}"; then
    break
  fi
  if [ "${attempt}" -eq 3 ]; then exit 1; fi
  sleep $((attempt * 15))
done
curl --fail --location --retry 5 --retry-all-errors --silent --show-error \
  "https://www.kernel.org/pub/software/scm/git/git-${version}.tar.xz" \
  --output "${archive}"
tar --extract --file "${archive}" --directory "${RUNNER_TEMP}"
(
  cd "${source_dir}"
  make configure
  ./configure --prefix="${prefix}"
  make --jobs=2 NO_TCLTK=YesPlease
  make install NO_TCLTK=YesPlease
)
echo "${prefix}/bin" >> "${GITHUB_PATH}"
