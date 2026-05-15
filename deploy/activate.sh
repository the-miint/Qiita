#!/usr/bin/env bash
# Stages /opt/qiita/incoming/ into /opt/qiita/, then reloads services.
# Safe on first deploy: skips restarts when env files / TLS files are absent.
# Invoked under sudo — directly by CI, or by deploy/local-deploy.sh.
# See docs/runbooks/first-deploy.md for the surrounding flow.

set -euo pipefail

[ "$EUID" -eq 0 ] || { echo "ERROR: deploy/activate.sh must be run as root (sudo)." >&2; exit 1; }

INCOMING=/opt/qiita/incoming

# sudo's secure_path excludes /usr/local/bin on RHEL-family; use the full path.
UV=/usr/local/bin/uv

# Without UV_PYTHON_INSTALL_DIR, uv writes Python into $HOME/.local/share/uv/python/.
# Under sudo $HOME is /root (mode 0700), unreachable by non-root service users at
# runtime — venv symlinks resolve to a file qiita-api etc. can't execute.
export UV_PYTHON_INSTALL_DIR=/opt/uv-python
install -d -o root -g root -m 0755 "$UV_PYTHON_INSTALL_DIR"

rsync -a --delete "$INCOMING/qiita-common/"              /opt/qiita/qiita-common/
rsync -a --delete "$INCOMING/qiita-control-plane/"       /opt/qiita/control-plane/
rsync -a --delete "$INCOMING/qiita-compute-orchestrator/" /opt/qiita/compute-orchestrator/

( cd /opt/qiita/control-plane        && "$UV" sync --no-dev )
( cd /opt/qiita/compute-orchestrator && "$UV" sync --no-dev )

# Fail loud if uv put Python somewhere a service user can't read it.
for venv in /opt/qiita/control-plane/.venv /opt/qiita/compute-orchestrator/.venv; do
    target=$(readlink -f "$venv/bin/python")
    case "$target" in
        "$UV_PYTHON_INSTALL_DIR"/*) ;;
        *) echo "ERROR: $venv/bin/python resolves to $target — expected under $UV_PYTHON_INSTALL_DIR. Service users will not be able to execute it." >&2; exit 1 ;;
    esac
done

install -m 755 "$INCOMING/qiita-data-plane" /opt/qiita/data-plane/qiita-data-plane

: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita.example.org)}"
cp "$INCOMING/deploy/nginx/qiita.conf" /etc/nginx/conf.d/qiita.conf
sed -i "s/__QIITA_HOSTNAME__/${QIITA_HOSTNAME}/g" /etc/nginx/conf.d/qiita.conf
cp "$INCOMING/deploy/systemd/"*.service /etc/systemd/system/
systemctl daemon-reload

# Skip restart when env file is absent (first deploy; operator writes envs in runbook steps 1/8b/9a).
restart_if_env_present() {
    if [ -r "$2" ]; then
        systemctl restart "$1"
    else
        echo "skipping restart $1 — $2 not present" >&2
    fi
}
restart_if_env_present qiita-control-plane         /etc/qiita/control-plane.env
restart_if_env_present qiita-compute-orchestrator  /etc/qiita/compute-orchestrator.env
restart_if_env_present qiita-data-plane@50051      /etc/qiita/data-plane.env

# Skip reload when TLS files are absent (nginx -t would fail and refuse reload).
if [ -r /etc/ssl/certs/qiita.crt ] && [ -r /etc/ssl/private/qiita.key ]; then
    systemctl reload nginx
else
    echo "skipping nginx reload — TLS files at /etc/ssl/{certs,private}/qiita.{crt,key} not present" >&2
fi
