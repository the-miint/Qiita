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

# Migration-pending guard. The code we're about to deploy assumes the schema
# produced by every file in db/migrations/; restarting services against a DB
# missing any of them surfaces as runtime 500s, not a boot failure. Applying
# migrations (`make migrate`) is a separate manual operator step that this
# script intentionally does NOT run — auto-applying is unsafe for
# expand/contract changes — so we only REFUSE to deploy onto a stale schema and
# point the operator at the runbook. Skipped on first deploy (no env file yet),
# the same gate as the actions sync / restarts below.
assert_migrations_applied() {
    # Explicit, logged escape hatch (same SKIP_* convention as local-deploy.sh)
    # for the rare host that has dbmate but no psql client, or any case where the
    # operator has verified migrations out-of-band. Bypassing is a deliberate,
    # visible act — never a silent fall-through.
    [ -n "${SKIP_MIGRATION_GUARD:-}" ] && { echo "migration guard: skipped via SKIP_MIGRATION_GUARD=1" >&2; return 0; }

    # We run as root (asserted at the top of this script), so the 0440
    # root:qiita-api env file is readable here. Absent only on first deploy —
    # the same gate as the actions sync / restarts below, where the DB is
    # bootstrapped out-of-band by the runbook's `make migrate`.
    local env_file=/etc/qiita/control-plane.env
    [ -r "$env_file" ] || { echo "skipping migration guard — $env_file not present (first deploy)" >&2; return 0; }

    # The states below are anomalous on an established host (env file present).
    # The guard's whole purpose is to fail loudly rather than restart onto a
    # stale schema, so each one REFUSES the deploy instead of waving it through.
    command -v psql >/dev/null 2>&1 || {
        echo "ERROR: psql not found — cannot verify migrations are applied; refusing to deploy blind." >&2
        echo "       Install the postgres client, or re-run with SKIP_MIGRATION_GUARD=1 after confirming 'make migrate' ran." >&2
        exit 1
    }

    local db_url
    db_url=$( set -a; # shellcheck disable=SC1090,SC1091
              source "$env_file"; set +a; printf '%s' "${DATABASE_URL:-}" )
    [ -n "$db_url" ] || {
        echo "ERROR: DATABASE_URL unset in $env_file — cannot verify migrations; refusing to deploy blind." >&2
        exit 1
    }

    # dbmate records each migration's version (the numeric filename prefix
    # before the first '_') in this table.
    local applied
    applied=$(psql "$db_url" -Atc 'SELECT version FROM public.schema_migrations' 2>/dev/null) || {
        echo "ERROR: could not query public.schema_migrations on the target DB — refusing to deploy blind." >&2
        exit 1
    }

    local f base version pending=()
    for f in "$INCOMING"/qiita-control-plane/db/migrations/*.sql; do
        [ -e "$f" ] || continue
        base=$(basename "$f"); version=${base%%_*}
        # Every dbmate migration is `<numeric-version>_name.sql`. A file whose
        # prefix isn't numeric isn't a tracked migration — it would never match
        # an applied version and so falsely read as "pending". Warn and skip
        # rather than abort the deploy over a stray .sql.
        [[ "$version" =~ ^[0-9]+$ ]] || { echo "migration guard: skipping non-migration file $base" >&2; continue; }
        grep -qxF "$version" <<<"$applied" || pending+=("$base")
    done

    if [ "${#pending[@]}" -gt 0 ]; then
        echo "ERROR: ${#pending[@]} migration(s) not applied to the target DB:" >&2
        printf '         %s\n' "${pending[@]}" >&2
        # A totally empty result on an established host (env file present) almost
        # never means "you missed every migration" — it means we queried the
        # wrong or un-migrated DB. Point there instead of at `make migrate`.
        if [ -z "$applied" ]; then
            echo "(public.schema_migrations returned 0 rows — DATABASE_URL likely points at the wrong or un-migrated database, not a single missed migration.)" >&2
        fi
        echo "Run 'make migrate' (see docs/runbooks/redeploy.md) before deploying — aborting before any service restart." >&2
        exit 1
    fi
    echo "migration guard: no pending migrations." >&2
}
assert_migrations_applied

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
# Install systemd dropin directories. Each dropin lives under
# deploy/systemd/<unit>.service.d/*.conf and is materialized at
# /etc/systemd/system/<unit>.service.d/. Loop over every present dropin
# tree so new dropins drop in without editing this script.
for dropin_src in "$INCOMING/deploy/systemd/"*.service.d; do
    [ -d "$dropin_src" ] || continue
    unit_dropin_name=$(basename "$dropin_src")
    install -d -o root -g root -m 0755 "/etc/systemd/system/$unit_dropin_name"
    for conf in "$dropin_src"/*.conf; do
        [ -e "$conf" ] || continue
        install -m 0644 -o root -g root "$conf" "/etc/systemd/system/$unit_dropin_name/"
    done
done
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
