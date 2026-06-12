#!/usr/bin/env bash
# deploy.sh — one-shot deploy script for Vigil's Fly.io infrastructure.
#
# Run from project root:
#   ./infra/fly/deploy.sh [backend|frontend|all]
#
# The first time you deploy, run the bootstrap commands in infra/fly/README.md
# to create the apps + Postgres cluster + volumes + secrets.  After that,
# this script handles ongoing deploys.

set -euo pipefail

# Resolve project root regardless of where the script was invoked from
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TARGET="${1:-all}"

# ── Sanity checks ──────────────────────────────────────────────────────────────

if ! command -v fly &>/dev/null; then
  echo "✗ fly CLI not installed.  Install with: curl -L https://fly.io/install.sh | sh"
  exit 1
fi

if ! fly auth whoami &>/dev/null; then
  echo "✗ Not logged into fly.io.  Run: fly auth login"
  exit 1
fi

echo "✓ Logged in as: $(fly auth whoami)"
echo ""

# ── Deploy backend ─────────────────────────────────────────────────────────────

deploy_backend() {
  echo "── Deploying backend ─────────────────────────────────────────────────"
  # Deploy from backend/ so the Docker build context matches what the
  # existing Dockerfile's COPY commands expect.  --remote-only does the
  # actual build on Fly's builder (faster + works even without local Docker).
  cd "$PROJECT_ROOT/backend"
  fly deploy \
    --config ../infra/fly/backend.fly.toml \
    --app vigil-backend \
    --remote-only
  echo ""
  echo "✓ Backend deployed.  Check status: fly status --app vigil-backend"
  echo ""
}

# ── Deploy frontend ────────────────────────────────────────────────────────────

deploy_frontend() {
  echo "── Deploying frontend ────────────────────────────────────────────────"
  # Deploy from frontend/ so the Docker build context is correct.  The
  # NEXT_PUBLIC_API_URL build arg in frontend.fly.toml gets baked into the
  # static bundle at build time.
  cd "$PROJECT_ROOT/frontend"
  fly deploy \
    --config ../infra/fly/frontend.fly.toml \
    --app vigil-frontend \
    --remote-only
  echo ""
  echo "✓ Frontend deployed.  Check status: fly status --app vigil-frontend"
  echo ""
}

# ── Dispatch ───────────────────────────────────────────────────────────────────

case "$TARGET" in
  backend)
    deploy_backend
    ;;
  frontend)
    deploy_frontend
    ;;
  all)
    deploy_backend
    deploy_frontend
    ;;
  *)
    echo "Usage: $0 [backend|frontend|all]"
    exit 1
    ;;
esac

echo "── Live URLs ─────────────────────────────────────────────────────────"
echo "  Backend:  https://vigil-backend.fly.dev/api/health"
echo "  Frontend: https://vigil-frontend.fly.dev"
echo ""
echo "  Custom domains (once DNS is configured per infra/fly/README.md):"
echo "  Backend:  https://api.vigilfraud.com/api/health"
echo "  Frontend: https://demo.vigilfraud.com"
