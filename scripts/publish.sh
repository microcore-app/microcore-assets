#!/usr/bin/env bash
# publish.sh — Build databases and create a GitHub Release.
#
# Usage:
#   bash scripts/publish.sh [YYYY-MM]
#
# Examples:
#   bash scripts/publish.sh 2026-04
#   bash scripts/publish.sh          # defaults to current year-month
#
# Requirements:
#   - python3 + zstd installed
#   - gh CLI authenticated with write access to this repo
#   - source data in /tmp/usda_raw/ (run download_sources.sh first)
set -euo pipefail

MONTH="${1:-$(date +%Y-%m)}"
TAG="db-$MONTH"
REPO="microcore-app/microcore-assets"
ASSETS_DIR="$(cd "$(dirname "$0")/.." && pwd)/assets"

echo "╔══════════════════════════════════════════════╗"
echo "  microcore-assets build & publish"
echo "  Tag: $TAG"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── 1. Build databases ─────────────────────────────────────────────────────
echo "Step 1/3 — Building databases..."
python3 "$(dirname "$0")/build_food_db.py"

# Check expected outputs exist
for db in foods_generic.db.gz foods_branded.db.gz foods_global.db.gz foods_swedish.db.gz foods_manifest.json; do
  if [ ! -f "$ASSETS_DIR/$db" ]; then
    echo "ERROR: expected output not found: $ASSETS_DIR/$db"
    exit 1
  fi
done
echo "  Outputs:"
ls -lh "$ASSETS_DIR"/*.gz 2>/dev/null || true

# ── 2. Generate checksums ──────────────────────────────────────────────────
echo ""
echo "Step 2/3 — Generating SHA-256 checksums..."
(cd "$ASSETS_DIR" && sha256sum *.gz > SHA256SUMS.txt)
cat "$ASSETS_DIR/SHA256SUMS.txt"

# ── 3. Create GitHub Release ───────────────────────────────────────────────
echo ""
echo "Step 3/3 — Creating GitHub release $TAG..."

NOTES=$(cat <<EOF
## Microcore database release $MONTH

### Included databases

| File | Contents | License |
|---|---|---|
| \`foods_generic.db.gz\` | ~13 k USDA generic foods | Public Domain |
| \`foods_branded.db.gz\` | ~2 M USDA branded products | Public Domain |
| \`foods_global.db.gz\` | ~2 M Open Food Facts | ODbL v1.0 |
| \`foods_swedish.db.gz\` | ~2 500 Swedish foods (Livsmedelsverket) | CC BY 4.0 |

### Open Food Facts attribution

\`foods_global.db.gz\` is derived from [Open Food Facts](https://world.openfoodfacts.org/)
and is released under the [Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/1-0/).

### Swedish Food Agency attribution

\`foods_swedish.db.gz\` is derived from [Livsmedelsverkets Livsmedelsdatabas](https://www.livsmedelsverket.se/om-oss/psidata/livsmedelsdatabasen/)
and is released under [Creative Commons Attribution 4.0 (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
Source: Livsmedelsverkets Livsmedelsdatabas.

### Checksums

See \`SHA256SUMS.txt\` attached to this release.
EOF
)

gh release create "$TAG" \
  "$ASSETS_DIR/foods_generic.db.gz" \
  "$ASSETS_DIR/foods_branded.db.gz" \
  "$ASSETS_DIR/foods_global.db.gz" \
  "$ASSETS_DIR/foods_swedish.db.gz" \
  "$ASSETS_DIR/foods_manifest.json" \
  "$ASSETS_DIR/SHA256SUMS.txt" \
  --repo "$REPO" \
  --title "Databases $MONTH" \
  --notes "$NOTES"

echo ""
echo "✓ Release published: https://github.com/$REPO/releases/tag/$TAG"
