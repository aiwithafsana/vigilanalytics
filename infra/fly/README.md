# Deploying Vigil to Fly.io

This is the step-by-step playbook to get `demo.vigilfraud.com` live for design-partner demos.  Total time: **~2 hours of focused work**, ~$30/month ongoing.

## Architecture

```
Cloudflare DNS                   Fly.io (region: iad)
─────────────                    ───────────────────
demo.vigilfraud.com  ──CNAME──►  vigil-frontend.fly.dev  (Next.js)
api.vigilfraud.com   ──CNAME──►  vigil-backend.fly.dev   (FastAPI) ──► vigil-db (Postgres)
```

## Cost estimate

| Service | Configuration | Cost |
|---|---|---|
| Backend VM | shared-cpu-1x, 512MB, auto-stop when idle | ~$5/mo |
| Frontend VM | shared-cpu-1x, 512MB, auto-stop when idle | ~$5/mo |
| Postgres | shared-cpu-1x, 1GB, 1× volume | ~$15/mo |
| Bandwidth | first 160GB free | $0 |
| Domain | vigilfraud.com on Namecheap or Cloudflare Registrar | ~$1/mo amortized |
| **Total** | | **~$26/mo** |

---

## One-time setup (do once, ~90 minutes)

### Step 0 — Prereqs (15 min)

- Sign up at https://fly.io (use credit card; first $5/mo of usage covered by free tier)
- Buy `vigilfraud.com` (or your chosen domain) — Cloudflare Registrar is cheapest at-cost pricing
- Install the Fly CLI:
  ```bash
  curl -L https://fly.io/install.sh | sh
  fly auth login
  ```

### Step 1 — Create the apps (5 min)

From the project root:

```bash
fly apps create vigil-backend  --org personal
fly apps create vigil-frontend --org personal
```

`personal` is the default organization name on a new Fly account.  Substitute if you've created a different org.

### Step 2 — Create the Postgres cluster (5 min)

```bash
fly postgres create \
  --name vigil-db \
  --region iad \
  --vm-size shared-cpu-1x \
  --volume-size 1 \
  --initial-cluster-size 1
```

When prompted, save the connection string Fly prints — you don't *need* it (the next step wires it automatically) but it's good to have.

### Step 3 — Attach Postgres to the backend (2 min)

```bash
fly postgres attach vigil-db --app vigil-backend
```

This automatically sets `DATABASE_URL` as a secret on `vigil-backend`.

### Step 4 — Create the documents volume (2 min)

For uploaded case documents (case files, evidence PDFs):

```bash
fly volumes create vigil_data --app vigil-backend --size 3 --region iad
```

### Step 5 — Set production secrets (10 min)

```bash
# Generate a strong SECRET_KEY for JWT signing
SECRET_KEY=$(openssl rand -hex 32)

fly secrets set \
  SECRET_KEY="$SECRET_KEY" \
  SAM_GOV_API_KEY="<your-sam-gov-key>" \
  COURTLISTENER_API_KEY="<your-courtlistener-key>" \
  --app vigil-backend
```

You already have those API keys in `backend/.env` from earlier setup — copy them over.

### Step 6 — First deploy (15 min)

```bash
./infra/fly/deploy.sh all
```

This script:
1. Builds the backend Docker image on Fly's remote builder
2. Deploys backend; on first boot, `entrypoint.sh` runs Alembic migrations against the empty Postgres
3. Builds the frontend with `NEXT_PUBLIC_API_URL=https://api.vigilfraud.com` baked in
4. Deploys frontend

After a successful deploy, smoke-test:

```bash
curl https://vigil-backend.fly.dev/api/health
# expected: {"status":"ok",...}

open https://vigil-frontend.fly.dev
# expected: login page renders
```

### Step 7 — Create the demo admin user (5 min)

You need an account to log into the deployed app.  SSH into the backend and run the same admin-creation script we use locally:

```bash
fly ssh console --app vigil-backend
# (now inside the container)
python -c "
import asyncio, uuid
from app.database import AsyncSessionLocal
from app.models import User
from app.auth import hash_password

async def main():
    async with AsyncSessionLocal() as db:
        user = User(
            id=uuid.uuid4(),
            email='demo@vigilfraud.com',
            hashed_password=await hash_password('DemoPass2026!'),
            name='Demo User',
            role='admin',
            state_access=[],
            is_active=True,
        )
        db.add(user)
        await db.commit()
        print('Created: demo@vigilfraud.com / DemoPass2026!')

asyncio.run(main())
"
exit
```

You'll change this password for real prospects before each demo.

### Step 8 — Configure custom domains (20 min)

#### On Fly:

```bash
fly certs add demo.vigilfraud.com  --app vigil-frontend
fly certs add api.vigilfraud.com   --app vigil-backend
```

Fly will print the DNS records you need.  Note them.

#### On Cloudflare:

1. Add `vigilfraud.com` as a Cloudflare site (if not already)
2. Add CNAME records:
   - `demo` → `vigil-frontend.fly.dev` (Proxy: off — Fly handles TLS itself)
   - `api`  → `vigil-backend.fly.dev` (Proxy: off)
3. Also add the AAAA records Fly printed (IPv6)

Wait ~5 minutes for DNS propagation, then verify:

```bash
fly certs show demo.vigilfraud.com --app vigil-frontend
# expected: "Status: Ready"

open https://demo.vigilfraud.com
# expected: Vigil login page on your custom domain
```

### Step 9 — Load real data (30 min)

The empty Postgres has the schema (from migrations) but no providers.  Two options:

**Option A — Run the ML pipeline against production DB (slow but realistic):**

Locally, point the ML pipeline at the production DB and run it.  Takes ~30 minutes for the full 1.23M-provider score.  Requires the `DATABASE_URL` to be the production connection string temporarily.

**Option B — Restore from a local snapshot (faster):**

```bash
# Locally: dump your dev DB
pg_dump -U vigil vigil > /tmp/vigil-snapshot.sql

# Restore to production via Fly proxy
fly proxy 5432 --app vigil-db
# (in another terminal)
psql "postgres://postgres:<password>@localhost:5432/vigil" < /tmp/vigil-snapshot.sql
```

Either works.  Option B is faster if you trust your local data.

---

## Ongoing deploys (do every time you ship)

After the one-time setup, every deploy is just:

```bash
./infra/fly/deploy.sh all
```

…which takes ~3 minutes.  If you only changed backend code: `./infra/fly/deploy.sh backend`.  If only frontend: `./infra/fly/deploy.sh frontend`.

---

## Common operations

| Task | Command |
|---|---|
| Check status of all VMs | `fly status --app vigil-backend` |
| Tail backend logs | `fly logs --app vigil-backend` |
| SSH into backend | `fly ssh console --app vigil-backend` |
| Connect to production Postgres | `fly postgres connect --app vigil-db` |
| Rotate the SECRET_KEY (forces re-login) | `fly secrets set SECRET_KEY=$(openssl rand -hex 32) --app vigil-backend` |
| Restart everything (rolling) | `fly apps restart vigil-backend && fly apps restart vigil-frontend` |
| See current monthly spend | `fly orgs show personal` |

---

## Troubleshooting

### "Application Error" on the frontend
- 90% of the time it's a build-time env var: `NEXT_PUBLIC_API_URL` got rebuilt with the wrong value.  Check `infra/fly/frontend.fly.toml` build.args, redeploy.

### Backend 502s
- Usually the Postgres connection died.  Restart with `fly apps restart vigil-backend`.

### "Cannot reach api.vigilfraud.com"
- Run `fly certs show api.vigilfraud.com --app vigil-backend` — if status is anything other than "Ready", check the Cloudflare CNAME record matches what Fly suggested.

### Out of memory
- Bump the backend VM in `backend.fly.toml`: `memory = "1gb"`, redeploy.  +$5/mo.

---

## Security notes for production

Before sending the URL to a real prospect:

- [ ] Change `demo@vigilfraud.com` password to something not in this README
- [ ] Set `CORS_ORIGINS` to only your demo domain
- [ ] Disable user-signup endpoints in the frontend (we don't want random people creating accounts)
- [ ] Verify `force_https = true` in both fly.toml files
- [ ] Confirm `MFA_ENABLED` defaults are sensible for demo accounts (probably skip MFA on demo user)

The first three are checked into the configs above.  The MFA decision is per-demo.

---

## What this does NOT cover (deferred)

- **Custom-domain email** (e.g., afsana@vigilfraud.com) — use Fastmail or Google Workspace, $6/mo
- **CDN caching** — Cloudflare is in the path but proxy is off; turn on for marketing pages later
- **Multi-region** — single region (`iad`) is fine until you have customers on the west coast
- **Staging environment** — deferred until customer #2.  Use a feature branch + local testing for now.
- **Monitoring beyond Fly's defaults** — Datadog / Sentry once revenue justifies the $15-30/mo
- **Backups beyond Fly's defaults** — Fly Postgres has daily snapshots; sufficient for design-partner stage
