#!/usr/bin/env bash
set -euo pipefail

tag="${1:?release tag is required}"
archive_url="${2:?archive URL is required}"
checksum_url="${3:?checksum URL is required}"
archive="firmament-x86_64-unknown-linux-gnu.tar.gz"
release_dir="/opt/firmament/releases/$tag"
temporary_dir="$(mktemp -d)"
cleanup() { rm -rf "$temporary_dir"; }
trap cleanup EXIT

curl --fail --location --retry 5 --retry-all-errors --output "$temporary_dir/$archive" "$archive_url"
curl --fail --location --retry 5 --retry-all-errors --output "$temporary_dir/$archive.sha256" "$checksum_url"
cd "$temporary_dir"
sha256sum --check "$archive.sha256"
mkdir package
tar -xzf "$archive" -C package
install -d -m 0755 "$release_dir"
install -m 0755 package/firmament "$release_dir/firmament"
ln -sfn "$release_dir" /opt/firmament/current
systemctl restart firmament.service
for _ in $(seq 1 30); do
  if curl --fail --silent http://127.0.0.1:8080/api/health >/dev/null; then
    exit 0
  fi
  sleep 2
done
systemctl status firmament.service --no-pager
exit 1
