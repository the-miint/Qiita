#!/usr/bin/env bash
# Stages /opt/qiita/incoming/ into /opt/qiita/, then reloads services.
# Safe on first deploy: skips restarts when env files / TLS files are absent.
# Invoked under sudo — directly by CI, or by deploy/local-deploy.sh.
# See docs/runbooks/first-deploy.md for the surrounding flow.

set -euo pipefail

[ "$EUID" -eq 0 ] || { echo "ERROR: deploy/activate.sh must be run as root (sudo)." >&2; exit 1; }
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita.example.org)}"

INCOMING=/opt/qiita/incoming
# Direct invocation guard: $INCOMING is normally populated by deploy/local-deploy.sh
# or a CI rsync. If empty, the rsync below would fail with a confusing
# "source missing" — fail fast with a useful message instead.
[ -d "$INCOMING/qiita-control-plane" ] || {
    echo "ERROR: $INCOMING is empty — run deploy/local-deploy.sh or rsync artifacts to $INCOMING first" >&2
    exit 1
}

# sudo's secure_path excludes /usr/local/bin on RHEL-family. Always invoke
# uv via $UV, never bare `uv` — bare lookup fails under sudo/systemd.
UV=/usr/local/bin/uv

# Without UV_PYTHON_INSTALL_DIR, uv writes Python into $HOME/.local/share/uv/python/.
# Under sudo $HOME is /root (mode 0700), unreachable by non-root service users at
# runtime — venv symlinks resolve to a file qiita-api etc. can't execute.
export UV_PYTHON_INSTALL_DIR=/opt/uv-python
install -d -o root -g root -m 0755 "$UV_PYTHON_INSTALL_DIR"

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"   # populates RSYNC_EXCLUDES

rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-common/"              /opt/qiita/qiita-common/
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-control-plane/"       /opt/qiita/control-plane/
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-compute-orchestrator/" /opt/qiita/compute-orchestrator/
# workflows/ is YAML the CP runner reads at request time (qiita-admin
# actions sync below upserts the YAML-authoritative columns into
# qiita.action). Out-of-tree from the three Python packages above so
# it ships independently of qiita-control-plane source.
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/workflows/"                 /opt/qiita/workflows/

# --reinstall-package qiita-common forces uv to rebuild the path-dep when
# qiita-common's source changes without a version bump; without this,
# redeploys leave stale qiita-common in dependents' site-packages
# (see CLAUDE.md "Cross-package staleness").
( cd /opt/qiita/control-plane        && "$UV" sync --no-dev --reinstall-package qiita-common )
( cd /opt/qiita/compute-orchestrator && "$UV" sync --no-dev --reinstall-package qiita-common )

# Upsert YAML-authoritative action columns into qiita.action. Runs as
# qiita-api so DATABASE_URL is sourced from the CP env file the user
# already reads. Idempotent: re-runs converge to the YAML state without
# touching operational columns (enabled / first_seen_at / disabled_*).
# Skipped on first deploy when /etc/qiita/control-plane.env doesn't yet
# exist — same gate as the systemd restarts below.
if [ -r /etc/qiita/control-plane.env ]; then
    sudo -u qiita-api bash -c '
        set -euo pipefail
        set -a
        # shellcheck disable=SC1091
        source /etc/qiita/control-plane.env
        set +a
        /opt/qiita/control-plane/.venv/bin/qiita-admin \
            actions sync --workflows-dir /opt/qiita/workflows
    '
else
    echo "skipping qiita-admin actions sync — /etc/qiita/control-plane.env not present" >&2
fi

# Fail loud if uv put Python somewhere a service user can't read it.
for venv in /opt/qiita/control-plane/.venv /opt/qiita/compute-orchestrator/.venv; do
    target=$(readlink -f "$venv/bin/python")
    case "$target" in
        "$UV_PYTHON_INSTALL_DIR"/*) ;;
        *) echo "ERROR: $venv/bin/python resolves to $target — expected under $UV_PYTHON_INSTALL_DIR. Service users will not be able to execute it." >&2; exit 1 ;;
    esac
done

install -d -o root -g root -m 0755 /opt/qiita/data-plane
install -m 0755 "$INCOMING/qiita-data-plane" /opt/qiita/data-plane/qiita-data-plane

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
# If you scale out the data plane (additional qiita-data-plane@NNNN instances),
# add a restart_if_env_present line for each here.

# Skip reload when TLS files are absent (nginx -t would fail and refuse reload).
if [ -r /etc/ssl/certs/qiita.crt ] && [ -r /etc/ssl/private/qiita.key ]; then
    systemctl reload nginx
else
    echo "skipping nginx reload — TLS files at /etc/ssl/{certs,private}/qiita.{crt,key} not present" >&2
fi
