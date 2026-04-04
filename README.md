# microcore-assets

Build scripts, data pipelines, and pre-built database releases for the
[Microcore](https://github.com/mc-health/microcore) health tracking app.

## Repository layout

```
scripts/
  build_food_db.py      # Main SQLite DB builder (USDA + OFF sources)
  download_sources.sh   # Download raw source data to /tmp/usda_raw/
  publish.sh            # Create a GitHub release with new DB artifacts
assets/                 # Placeholder — large files are never committed;
                        # they live in GitHub Releases
.github/workflows/
  build-release.yml     # Manual / scheduled CI pipeline
DATA_LICENSES.md        # Full license analysis for redistributed data
```

## Databases

| Database | Foods | Compressed | Source |
|---|---|---|---|
| `foods_generic.db.gz` | ~13 k | ~0.6 MB | USDA SR Legacy + Foundation + FNDDS |
| `foods_branded.db.gz` | ~2 M | ~100 MB | USDA Branded Food Products |
| `foods_global.db.gz` | ~2 M | ~97 MB | Open Food Facts (multilingual) |

Latest release: [Releases →](https://github.com/mc-health/microcore-assets/releases)

## Building locally

### 1. Install dependencies

```bash
# Python 3, zstd
sudo apt install python3 zstd   # Debian/Ubuntu
brew install python zstd        # macOS
```

### 2. Download raw source data

```bash
bash scripts/download_sources.sh
# Downloads to /tmp/usda_raw/ (~2 GB total, takes a few minutes)
```

### 3. Build all three databases

```bash
python3 scripts/build_food_db.py
# Outputs to assets/  (overrides any existing files)
```

### 4. Publish a release (maintainers only)

```bash
# Requires: gh CLI authenticated with write access to this repo
bash scripts/publish.sh 2026-04
```

Or trigger the **Build & Release** GitHub Actions workflow manually via the
Actions tab (set the `tag` input, e.g. `db-2026-04`).

## Licenses

- **Build scripts** (this repo's code): MIT — see [LICENSE](LICENSE)
- **Database content**: see [DATA_LICENSES.md](DATA_LICENSES.md)

TL;DR: USDA data is public domain. Open Food Facts data is ODbL v1.0 —
attribution required, share-alike on derived databases.
