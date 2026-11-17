#!/bin/bash
# =====================================================================
# playwright_init.sh: cluster init script of the scraper job cluster
# (Ubuntu 22.04 based Databricks Runtime). Installs the OS libraries for a headless
# browser, the Playwright driver and Microsoft Edge Stable.
#
# Why apt-get: Microsoft Edge and its system libraries are Ubuntu packages. Init scripts
# run as root on each cluster node before Spark starts, which is the only point where
# OS packages can be installed on a Databricks cluster.
#
# What it needs (also listed in the TSD and the README)
#   1. A classic job cluster (init scripts do not run on serverless compute).
#   2. This file stored in a Unity Catalog volume that the workspace admin has added
#      to the allowlist for init scripts (Catalog Explorer, Metastore, Allowed JARs/Init Scripts).
#   3. The job cluster in dedicated (single user) access mode.
#   4. Outbound network access during cluster start:
#        Online mode (lab):  archive.ubuntu.com, security.ubuntu.com (port 80),
#                            pypi.org, files.pythonhosted.org, packages.microsoft.com,
#                            playwright.azureedge.net
#        Offline mode (bank): only the approved Ubuntu apt mirror. Playwright wheels come
#                            from a volume, with a checksum check.
#   5. No secrets are used.
#
# Production recommendation: a reviewed custom container image (Databricks Container
# Services) with Edge and Playwright already installed, so no package is installed at start.
#
# Cluster environment variables (set by the bundle, spark_env_vars)
#   PLAYWRIGHT_VERSION        pinned Playwright version (default below)
#   PLAYWRIGHT_ARTIFACTS_DIR  optional: volume folder with approved wheels, enables offline mode
#   EDGE_VERSION              optional: pinned microsoft-edge-stable package version (offline mode)
# =====================================================================

set -euo pipefail

PLAYWRIGHT_VERSION="${PLAYWRIGHT_VERSION:-1.47.0}"
ARTIFACTS_DIR="${PLAYWRIGHT_ARTIFACTS_DIR:-}"
EDGE_VERSION="${EDGE_VERSION:-}"
PIP=/databricks/python3/bin/pip
PY=/databricks/python3/bin/python
EDGE=/usr/bin/microsoft-edge-stable

# Print a log line with a time stamp
log() { echo "[playwright_init] $(date '+%Y%m%d %H:%M:%S') $*"; }

log "Installing OS libraries for headless Microsoft Edge"
apt-get update -q
apt-get install -y -q libgbm1 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libasound2 libpango-1.0-0 libcairo2

if [ -n "$ARTIFACTS_DIR" ]; then
    log "Offline mode, Playwright wheels from $ARTIFACTS_DIR"
    test -d "$ARTIFACTS_DIR/wheels" || { log "Missing $ARTIFACTS_DIR/wheels"; exit 1; }
    if [ -f "$ARTIFACTS_DIR/SHA256SUMS" ]; then
        (cd "$ARTIFACTS_DIR" && sha256sum --check --quiet SHA256SUMS) || { log "Artifact checksum failed"; exit 1; }
    fi
    $PIP install --no-index --find-links "$ARTIFACTS_DIR/wheels" "playwright==${PLAYWRIGHT_VERSION}"
    if [ -n "$EDGE_VERSION" ]; then
        log "Installing Microsoft Edge Stable $EDGE_VERSION from the approved apt mirror"
        apt-get install -y -q "microsoft-edge-stable=$EDGE_VERSION"
    else
        log "Installing Microsoft Edge Stable (latest on the approved apt mirror)"
        apt-get install -y -q microsoft-edge-stable
    fi
else
    log "Online mode, installing playwright ${PLAYWRIGHT_VERSION}"
    $PIP install "playwright==${PLAYWRIGHT_VERSION}"
    log "Installing Microsoft Edge Stable from the Microsoft repository"
    $PY -m playwright install msedge
fi

test -x "$EDGE" || { log "Microsoft Edge was not installed"; exit 1; }
log "Installed: $($EDGE --version)"

log "Smoke test: launching headless Microsoft Edge"
$PY - <<'PYEOF'
import asyncio
from playwright.async_api import async_playwright

# Start headless Edge once, print its version, and close it
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, channel='msedge')
        print('Microsoft Edge', browser.version, 'OK')
        await browser.close()

asyncio.run(main())
PYEOF

log "Done"
