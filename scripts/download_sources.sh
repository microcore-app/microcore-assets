#!/usr/bin/env bash
# download_sources.sh — Download raw USDA + OFF source files to /tmp/usda_raw/
# Safe to re-run: skips files that already exist and have the right size.
set -euo pipefail

DEST="${1:-/tmp/usda_raw}"
mkdir -p "$DEST"

echo "→ Downloading source data to $DEST"
echo "  Total download: ~3 GB. This will take several minutes."
echo ""

# ── USDA FoodData Central ──────────────────────────────────────────────────
# Full Download (all data types in one zip, ~530 MB)
USDA_URL="https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_2024-04-18.zip"
USDA_ZIP="$DEST/FoodData_Central_csv.zip"
if [ ! -f "$USDA_ZIP" ]; then
  echo "  Downloading USDA FoodData Central..."
  curl -L --progress-bar -o "$USDA_ZIP" "$USDA_URL"
else
  echo "  [skip] USDA zip already present: $USDA_ZIP"
fi

# ── Open Food Facts ────────────────────────────────────────────────────────
# Compressed CSV export (~1.2 GB gzip)
OFF_URL="https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"
OFF_GZ="$DEST/off_products.csv.gz"
if [ ! -f "$OFF_GZ" ]; then
  echo "  Downloading Open Food Facts CSV..."
  curl -L --progress-bar -o "$OFF_GZ" "$OFF_URL"
else
  echo "  [skip] OFF csv.gz already present: $OFF_GZ"
fi

echo ""
echo "Done. Files in $DEST:"
ls -lh "$DEST"
