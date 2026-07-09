# Flight signing-key & login-cookie rotation

> **Applies today.** The control plane signs Arrow Flight tickets with an
> **Ed25519 private seed** (`FLIGHT_TICKET_SIGNING_KEY`); the data plane holds
> only the matching **public key** (`FLIGHT_TICKET_PUBLIC_KEY`) and verifies. The
> control plane also HMAC-signs the `/auth/login` → `/auth/handoff` cookie with a
> separate `LOGIN_COOKIE_SECRET_KEY`. This runbook rotates each of those.
> Provisioning of the initial values is in
> [`first-deploy.md`](first-deploy.md); the trust model is in
> [`../auth.md`](../auth.md).

**Purpose.** Replace the Flight-ticket signing keypair or the login-cookie
secret — routinely, or in response to a suspected leak. A leaked Flight
**public** key is harmless (verify-only); the sensitive material is the CP's
private seed (`FLIGHT_TICKET_SIGNING_KEY`) and the cookie key
(`LOGIN_COOKIE_SECRET_KEY`), both control-plane-only.

## Why restart-based, not zero-downtime

The data plane holds exactly **one** verification key and reads it once at boot
(`config.rs` → `Settings`); there is no accepted-key *set* and no reload handler.
So rotating the Flight keypair means the CP (new private seed) and DP (new public
key) must be restarted **together**, and there is a brief window during the
restart where a ticket signed under one key is checked under the other and fails.
This is acceptable because Flight tickets are **short-lived** (minted just before
use; ~1 h ceiling) and the control-plane runner retries transient Flight errors —
a masked-read export or reference load that races the restart is re-driven, not
lost. A zero-downtime path (a DP-side accepted-key set, so old+new verify
concurrently across the window) is future work; until it lands, rotation is
restart-based, like [`orchestrator-token-rotation.md`](orchestrator-token-rotation.md).

The login-cookie key is control-plane-only (the CP both signs and verifies it),
so rotating it is a **CP-only restart** — it invalidates only in-flight login
cookies, whose freshness window is `AUTH_HANDOFF_FRESHNESS_SECONDS` (default 60 s),
so the blast radius is "a user mid-login re-clicks login."

## Prerequisites

- Run on the deploy host. Editing `/etc/qiita/*.env` needs root (the files are
  `root:qiita-api` / `root:qiita-data`, mode `0440`).
- `python3` with `cryptography` for the Ed25519 keygen — the control-plane venv
  has it (`<checkout>/qiita-control-plane/.venv/bin/python3`); use that if a bare
  `python3` lacks the module.
- Know your `<checkout>` path and the systemd units: `qiita-control-plane`,
  `qiita-data-plane@50051` (add each extra `@NNNN` instance you run).

## A. Rotate the Flight signing keypair

**1. Generate a new keypair.** Capture both values (private seed for the CP,
public key for the DP):

```bash
python3 -c "from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey as K; import base64; k=K.generate(); print('SIGNING', base64.b64encode(k.private_bytes_raw()).decode()); print('PUBLIC', base64.b64encode(k.public_key().public_bytes_raw()).decode())"
```

**2. Write the new values** (the sed updates the existing line in place; the
`.previous` copy is your rollback):

```bash
# Control plane — the PRIVATE seed:
sudo cp -a /etc/qiita/control-plane.env /etc/qiita/control-plane.env.previous
sudo sed -i "s|^FLIGHT_TICKET_SIGNING_KEY=.*|FLIGHT_TICKET_SIGNING_KEY=<SIGNING>|" /etc/qiita/control-plane.env

# Data plane — the matching PUBLIC key:
sudo cp -a /etc/qiita/data-plane.env /etc/qiita/data-plane.env.previous
sudo sed -i "s|^FLIGHT_TICKET_PUBLIC_KEY=.*|FLIGHT_TICKET_PUBLIC_KEY=<PUBLIC>|" /etc/qiita/data-plane.env
```

**3. Preflight — confirm the keypair matches BEFORE restarting.** This derives
the public key from the CP's seed (using the CP venv's `cryptography`) and
compares it to the DP's public key, so a copy-paste slip is caught before it
takes the Flight path down. If that interpreter can't run the derivation,
preflight prints `skip` for this line — *not* a green pass — so a `skip` means
"not verified here," and step 5's live DoGet is then the definitive gate:

```bash
sudo make preflight        # expect: flight-keypair … DP public key matches CP signing seed
                           # a `skip` means the check couldn't run — rely on step 5, not this
```

**4. Restart both services together** (the brief cross-key window above):

```bash
sudo systemctl restart qiita-control-plane qiita-data-plane@50051   # + any extra @NNNN instances
```

**5. Verify** end-to-end — a real Flight call must succeed under the new key:

```bash
make verify-health         # DP gRPC health SERVING
# and one live DoGet (as a system_admin with a fresh PAT):
qiita-admin masked-read-export --sequenced-pool-idx <P> --mask-idx <M> \
    --output-dir /tmp/rot-check --data-plane-url grpc+tls://<fqdn>:443
```

`Unauthenticated: invalid signature` on a Flight call means the CP and DP keys
don't correspond — re-check step 3 / roll back (below).

**6. Clean up** the `.previous` copies once you're confident (keep them one cycle
for rollback):

```bash
sudo shred -u /etc/qiita/control-plane.env.previous /etc/qiita/data-plane.env.previous
```

## B. Rotate the login-cookie key

Control-plane only:

```bash
sudo cp -a /etc/qiita/control-plane.env /etc/qiita/control-plane.env.previous
NEW=$(openssl rand -base64 32)
sudo sed -i "s|^LOGIN_COOKIE_SECRET_KEY=.*|LOGIN_COOKIE_SECRET_KEY=$NEW|" /etc/qiita/control-plane.env
sudo make preflight        # expect: login-cookie-key … present and distinct from the Flight signing key
sudo systemctl restart qiita-control-plane
```

Verify with a fresh `qiita-admin login` (or `qiita login`) round-trip — the
handoff must complete. In-flight login cookies from before the restart are
invalidated (users re-click login); nothing else is affected.

## If it doesn't work — rollback

The `.previous` copies restore the prior key. For a Flight-keypair rollback,
restore **both** files and restart **both** services together:

```bash
sudo mv /etc/qiita/control-plane.env.previous /etc/qiita/control-plane.env
sudo mv /etc/qiita/data-plane.env.previous /etc/qiita/data-plane.env
sudo make preflight
sudo systemctl restart qiita-control-plane qiita-data-plane@50051
```

For a cookie-key rollback, restore `control-plane.env` and restart only the
control plane. `.previous` lives one cycle — re-snapshot before the next rotation.
