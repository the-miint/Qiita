#!/usr/bin/env bash
# Stages /opt/qiita/incoming/ into /opt/qiita/, then reloads services.
# Safe on first deploy: skips restarts when env files / TLS files are absent.
# Invoked under sudo — the install half a deploy exec's after staging
# $INCOMING; deploy/local-deploy.sh is the normal caller.
# See docs/runbooks/first-deploy.md for the surrounding flow.

set -euo pipefail

# shellcheck source=deploy/_common.sh
source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"  # require_root, read_env_var, CP_ENV, RSYNC_EXCLUDES

require_root "deploy/activate.sh must be run as root (sudo)."
: "${QIITA_HOSTNAME:?QIITA_HOSTNAME must be set (e.g. qiita.example.org)}"

INCOMING=/opt/qiita/incoming
# Direct invocation guard: $INCOMING is normally populated by deploy/local-deploy.sh
# (or another stage-then-activate rsync). If empty, the rsync below would fail with
# a confusing "source missing" — fail fast with a useful message instead.
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
    local env_file="$CP_ENV"
    [ -r "$env_file" ] || { echo "skipping migration guard — $env_file not present (first deploy)" >&2; return 0; }

    # The states below are anomalous on an established host (env file present).
    # The guard's whole purpose is to fail loudly rather than restart onto a
    # stale schema, so each one REFUSES the deploy instead of waving it through.
    command -v psql >/dev/null 2>&1 || {
        echo "ERROR: psql not found — cannot verify migrations are applied; refusing to deploy blind." >&2
        echo "       Install the postgres client, or re-run with SKIP_MIGRATION_GUARD=1 after confirming 'make migrate' ran." >&2
        exit 1
    }

    # read_env_var (deploy/_common.sh) reads DATABASE_URL in a +eu subshell so a
    # value referencing another var can't abort and silently blank it (which would
    # misreport as "DATABASE_URL unset" instead of the real cause).
    local db_url
    db_url=$(read_env_var "$env_file" DATABASE_URL)
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

rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-common/"              /opt/qiita/qiita-common/
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-control-plane/"       /opt/qiita/control-plane/
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/qiita-compute-orchestrator/" /opt/qiita/compute-orchestrator/
# workflows/ is YAML the CP runner reads at request time (qiita-admin
# actions sync below upserts the YAML-authoritative columns into
# qiita.action). Out-of-tree from the three Python packages above so
# it ships independently of qiita-control-plane source.
rsync -a --delete "${RSYNC_EXCLUDES[@]}" "$INCOMING/workflows/"                 /opt/qiita/workflows/

# Build stamp for the CP landing footer. QIITA_BUILD_SHA is the FULL
# deployed commit SHA, set by deploy/local-deploy.sh (from the git clone);
# this is the single site that truncates it to the 7-char short form, so the
# footer SHA is consistently shaped.
# build.env is in RSYNC_EXCLUDES (deploy/_common.sh), so the control-plane
# rsync --delete above never wipes it and this write needn't be ordered
# after the rsync; recreated every deploy, so it can never go stale.
# Optional: when unset, write an empty file so the unit's
# `EnvironmentFile=-` finds nothing and the footer stays version-only —
# never fail the deploy over a missing stamp. Mode 0644: read by the
# qiita-api service user via systemd; no secrets here.
build_sha=${QIITA_BUILD_SHA:-}
build_sha=${build_sha:0:7}
if [ -n "$build_sha" ]; then
    printf 'BUILD_SHA=%s\n' "$build_sha" > /opt/qiita/control-plane/build.env
else
    : > /opt/qiita/control-plane/build.env
    echo "build stamp: QIITA_BUILD_SHA unset — landing footer will show version only" >&2
fi
chmod 0644 /opt/qiita/control-plane/build.env

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

# Build/verify container-workflow SIFs before the restarts (a SLURM step depends
# on its image being present and current). Runs as root and chowns each produced
# SIF to qiita-orch. Idempotent: unchanged images cost only a fast verify. Absent
# prerequisites (no apptainer, no PATH_DERIVED, unstaged licensed source) degrade
# to a clean skip; a genuine build failure exits non-zero and aborts the deploy
# here, before any service restarts onto a broken image. See build-sifs.sh.
"$(dirname "${BASH_SOURCE[0]}")/build-sifs.sh"

# Data-plane instance set — one list drives the nginx upstream AND the systemd
# units below, so they cannot disagree (see qiita_data_plane_ports in _common.sh).
# A malformed QIITA_DATA_PLANE_PORTS aborts here, before any config is written.
DP_PORTS=$(qiita_data_plane_ports)
echo "data plane instances: $DP_PORTS"
# Remote instances, if any. Assigned separately (and BEFORE any config is written)
# so a malformed entry aborts here like a malformed port does. Peers join the nginx
# upstream only — they are another host's systemd units, not ours.
DP_PEERS=$(qiita_data_plane_peers)
if [ -n "$DP_PEERS" ]; then
    echo "data plane peers (remote, not managed here): $DP_PEERS"
fi

cp "$INCOMING/deploy/nginx/qiita.conf" /etc/nginx/conf.d/qiita.conf
sed -i "s/__QIITA_HOSTNAME__/${QIITA_HOSTNAME}/g" /etc/nginx/conf.d/qiita.conf
sed -i "s/__QIITA_DATA_PLANE_LB_PORT__/${QIITA_DATA_PLANE_LB_PORT}/g" /etc/nginx/conf.d/qiita.conf

# Render the upstream member lines from the instance list. Built as a file and
# spliced with `sed -e /pat/r` rather than an in-place substitution because the
# replacement is multi-line; the placeholder line is then deleted.
DP_UPSTREAM=$(mktemp)
for port in $DP_PORTS; do
    printf '    server 127.0.0.1:%s;\n' "$port" >>"$DP_UPSTREAM"
done
# Remote members last, so a `nginx -T` reads local-then-remote. Written verbatim —
# qiita_data_plane_peers already validated the shape (see _common.sh), which is what
# keeps a stray `;` out of the generated config.
for peer in $DP_PEERS; do
    printf '    server %s;\n' "$peer" >>"$DP_UPSTREAM"
done
sed -i -e "/__QIITA_DATA_PLANE_UPSTREAM__/r $DP_UPSTREAM" \
       -e "/__QIITA_DATA_PLANE_UPSTREAM__/d" /etc/nginx/conf.d/qiita.conf
rm -f "$DP_UPSTREAM"
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
# Every configured data-plane instance, from the same list that rendered the
# nginx upstream above. `enable` before `restart` so an instance added by growing
# QIITA_DATA_PLANE_PORTS starts on this deploy AND survives a reboot — previously
# only @50051 was restarted, so a hand-added instance silently ran stale code.
# Enabling is idempotent, so this is a no-op for instances already enabled.
for port in $DP_PORTS; do
    if [ -r /etc/qiita/data-plane.env ]; then
        systemctl enable "qiita-data-plane@${port}" >/dev/null
    fi
    restart_if_env_present "qiita-data-plane@${port}" /etc/qiita/data-plane.env
done

# Validate the rendered config before reloading. Skip both when TLS files are
# absent (nginx -t would fail on the missing cert/key and refuse reload). With
# set -e, a bad config fails the deploy here loudly instead of a silently-failed
# reload leaving the old config live.
if [ -r /etc/ssl/certs/qiita.crt ] && [ -r /etc/ssl/private/qiita.key ]; then
    nginx -t
    systemctl reload nginx
else
    echo "skipping nginx reload — TLS files at /etc/ssl/{certs,private}/qiita.{crt,key} not present" >&2
fi
