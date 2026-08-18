#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
vendor_root="${repo_root}/vendor"
patch_root="${repo_root}/patches"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

patch_digest="$(
  sha256_file "$0"
  for patch_file in "${patch_root}"/*.patch; do
    sha256_file "${patch_file}"
  done | sha256_stream
)"

if [[ -f "${vendor_root}/.patch-digest" ]] \
  && [[ "$(<"${vendor_root}/.patch-digest")" == "${patch_digest}" ]] \
  && [[ -d "${vendor_root}/object-store" ]] \
  && [[ -d "${vendor_root}/slatedb" ]] \
  && [[ -d "${vendor_root}/slatedb-common" ]] \
  && [[ -d "${vendor_root}/slatedb-txn-obj" ]]; then
  exit 0
fi

work_root="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-patched-vendor.XXXXXX")"
trap 'rm -rf "${work_root}"' EXIT
mkdir -p "${work_root}/vendor"

prepare_crate() {
  local registry_name="$1"
  local archive_name="$2"
  local crate_dir="$3"
  local checksum="$4"
  local patch_file="$5"
  local archive="${work_root}/${archive_name}.crate"
  local source_url="https://static.crates.io/crates/${registry_name}/${archive_name}.crate"

  curl --fail --location --silent --show-error --retry 3 \
    "${source_url}" --output "${archive}"
  [[ "$(sha256_file "${archive}")" == "${checksum}" ]]
  tar -xzf "${archive}" -C "${work_root}/vendor"
  mv "${work_root}/vendor/${archive_name}" "${work_root}/vendor/${crate_dir}"
  patch -s -V none -d "${work_root}/vendor/${crate_dir}" -p1 \
    < "${patch_root}/${patch_file}"
}

prepare_crate \
  "object_store" \
  "object_store-0.14.1" \
  "object-store" \
  "d354792e39fa5f0009e47623cf8b15b099bf9a652fa55c6f817fe28ac84fea50" \
  "object_store-0.14.1.patch"
prepare_crate \
  "slatedb" \
  "slatedb-0.15.0" \
  "slatedb" \
  "35ca56b01922b15aa69fe3abb62cadc985d86032c9647e4606e211c4da751a76" \
  "slatedb-0.15.0.patch"
prepare_crate \
  "slatedb-common" \
  "slatedb-common-0.15.0" \
  "slatedb-common" \
  "0aa8de522ff46a0f9b5a66f45e650d125421d4d91990933591092f9d010c40d1" \
  "slatedb-common-0.15.0.patch"
prepare_crate \
  "slatedb-txn-obj" \
  "slatedb-txn-obj-0.15.0" \
  "slatedb-txn-obj" \
  "d85fd9c0c86dd4954524fa8238d0548bd80f4a9a66983cbde3728402d339993e" \
  "slatedb-txn-obj-0.15.0.patch"

printf '%s\n' "${patch_digest}" > "${work_root}/vendor/.patch-digest"

previous_vendor="${work_root}/previous-vendor"
if [[ -e "${vendor_root}" ]]; then
  mv "${vendor_root}" "${previous_vendor}"
fi
if ! mv "${work_root}/vendor" "${vendor_root}"; then
  if [[ -e "${previous_vendor}" ]]; then
    mv "${previous_vendor}" "${vendor_root}"
  fi
  exit 1
fi
