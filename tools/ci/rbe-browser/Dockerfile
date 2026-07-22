# Custom BuildBuddy RBE image for loom: the flame-public rbe-ubuntu24-04 base
# plus the chrome-headless-shell runtime libraries, so //src/ui/e2e:login can
# start its vendored browser on the RE worker (see
# git history: 2026-07-02-ui-e2e-hermetic-rbe-image-design).
#
# STRICT SUPERSET of the base: this ONLY adds an apt layer. Do not change USER,
# ENTRYPOINT, WORKDIR, or the buildbuddy toolchain layout — every loom RE action
# runs on this image once platforms/defs.bzl is repointed at it.
#
# The browser BINARY is NOT in this image; buck vendors it (//third-party/browser).
# This image supplies only the shared libraries the binary loads at runtime.
#
# Base pinned by digest for reproducibility. To refresh the base, re-resolve:
#   docker buildx imagetools inspect gcr.io/flame-public/rbe-ubuntu24-04:latest \
#     --format '{{.Manifest.Digest}}'
FROM gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a

# chrome-headless-shell (Chrome for Testing) runtime deps on Ubuntu 24.04.
# The publish-time smoke (tools/ci/rbe-browser/smoke.sh) is the ground truth for
# this list; add any lib the smoke reports as "error while loading shared
# libraries" and re-run.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      fonts-liberation \
      libasound2t64 \
      libatk-bridge2.0-0t64 \
      libatk1.0-0t64 \
      libatspi2.0-0t64 \
      libcairo2 \
      libcups2t64 \
      libdbus-1-3 \
      libdrm2 \
      libexpat1 \
      libgbm1 \
      libglib2.0-0t64 \
      libnspr4 \
      libnss3 \
      libpango-1.0-0 \
      libx11-6 \
      libxcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxkbcommon0 \
      libxrandr2 \
    && rm -rf /var/lib/apt/lists/*
