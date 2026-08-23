#!/usr/bin/env bash
set -euo pipefail

version=2.55.0
prefix="${RUNNER_TEMP:?This installer is for GitHub Actions}/git-${version}"
archive="${RUNNER_TEMP}/git-${version}.tar.xz"
source_dir="${RUNNER_TEMP}/git-${version}"

sudo apt-get update
sudo apt-get install --yes build-essential curl gettext libcurl4-openssl-dev libexpat1-dev xz-utils zlib1g-dev
curl --fail --location --silent --show-error \
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
