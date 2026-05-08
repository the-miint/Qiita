#!/usr/bin/env bash
# Runs on the deploy server after artifacts are rsync'd to /opt/qiita/incoming/.
#
# The deploy user needs passwordless sudo for:
#   systemctl daemon-reload
#   systemctl restart qiita-control-plane qiita-compute-orchestrator qiita-data-plane@*
#   systemctl reload nginx
#   cp .../deploy/nginx/* /etc/nginx/conf.d/
#   cp .../deploy/systemd/* /etc/systemd/system/
#
# Directory layout on the server:
#   /opt/qiita/incoming/          rsync target (this script reads from here)
#   /opt/qiita/qiita-common/      installed source (path dep for Python services)
#   /opt/qiita/control-plane/     installed source + .venv
#   /opt/qiita/compute-orchestrator/  installed source + .venv
#   /opt/qiita/data-plane/        Rust binary

set -euo pipefail

INCOMING=/opt/qiita/incoming

# Sync Python service source. qiita-common goes to /opt/qiita/qiita-common/ so that
# the path dep "../qiita-common" resolves correctly from /opt/qiita/control-plane/.
rsync -a --delete "$INCOMING/qiita-common/"             /opt/qiita/qiita-common/
rsync -a --delete "$INCOMING/qiita-control-plane/"      /opt/qiita/control-plane/
rsync -a --delete "$INCOMING/qiita-compute-orchestrator/" /opt/qiita/compute-orchestrator/

# Sync Python venvs (--no-dev: no test/lint tools in production)
( cd /opt/qiita/control-plane && uv sync --no-dev )
( cd /opt/qiita/compute-orchestrator && uv sync --no-dev )

# Install Rust binary atomically (install(1) does an atomic rename)
install -m 755 "$INCOMING/qiita-data-plane" /opt/qiita/data-plane/qiita-data-plane

# Install system config. The nginx conf carries a __QIITA_HOSTNAME__ placeholder
# that is substituted at deploy time from the QIITA_HOSTNAME env var (e.g.
# qiita-miint.ucsd.edu) so the same template works across deployments.
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita-miint.ucsd.edu)}"
sudo cp "$INCOMING/deploy/nginx/qiita.conf" /etc/nginx/conf.d/qiita.conf
sudo sed -i "s/__QIITA_HOSTNAME__/${QIITA_HOSTNAME}/g" /etc/nginx/conf.d/qiita.conf
sudo cp "$INCOMING/deploy/systemd/"*.service /etc/systemd/system/
sudo systemctl daemon-reload

# Restart services
sudo systemctl restart qiita-control-plane qiita-compute-orchestrator "qiita-data-plane@50051"
sudo systemctl reload nginx
