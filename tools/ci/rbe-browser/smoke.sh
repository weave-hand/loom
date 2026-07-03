#!/usr/bin/env bash
# Build the custom RBE image and prove its libs satisfy chrome-headless-shell.
# Downloads the browser on the HOST (needs curl+unzip here, not in the image),
# mounts it read-only into a container run FROM the image, and runs --version.
# Exit 0 iff the version prints — i.e. the lib closure is complete.
#
# Usage: tools/ci/rbe-browser/smoke.sh [IMAGE_TAG]
#   IMAGE_TAG defaults to loom-rbe-browser:local (built from ./Dockerfile).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$here/../../.." && pwd)"
image="${1:-loom-rbe-browser:local}"

# Single source of truth for the browser version.
ver="$(grep -oP 'CHROME_FOR_TESTING_VERSION = "\K[^"]+' "$repo_root/third-party/browser/BUCK")"
echo "chrome-for-testing version: $ver"

# Build the image if the caller didn't pre-build a tag that exists.
if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "building $image from $here/Dockerfile"
  docker build -t "$image" "$here"
fi

# Fetch the pinned browser on the host.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
url="https://storage.googleapis.com/chrome-for-testing-public/$ver/linux64/chrome-headless-shell-linux64.zip"
echo "downloading $url"
curl -fsSL -o "$work/chs.zip" "$url"
unzip -q "$work/chs.zip" -d "$work"
chs_dir="$work/chrome-headless-shell-linux64"

# Run --version inside the image, using the image's libs.
echo "=== chrome-headless-shell --version (inside $image) ==="
docker run --rm -v "$chs_dir:/chs:ro" "$image" \
  /chs/chrome-headless-shell --version
echo "=== smoke OK: lib closure is complete ==="
