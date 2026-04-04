#!/usr/bin/env python3
"""
tool/build_food_db.py
=====================
Builds three offline food SQLite databases, compresses them.

  foods_generic.db.zst   ~0.6 MB   →  ships INSIDE the app bundle
  foods_branded.db.zst   ~80 MB    →  downloaded on first launch (CDN)
  foods_global.db.zst    ~20 MB    →  downloaded on first launch (CDN)

Sources (all public domain / open licence):
  1. USDA SR Legacy + Foundation + FNDDS  → generic
  2. USDA Branded (~2M products, barcodes) → branded
  3. Open Food Facts (global crowd-sourced, multilingual) → global

Nutrients stored as a 56-byte BLOB (28 × uint16 fixed-point).
  - Saves ~40% vs 28 REAL columns for sparse nutrient data.
  - Decode: value = stored_uint16 / scale  (0xFFFF = NULL).

Monthly update:
  python3 tool/build_food_db.py
  wget -c resumes partial downloads automatically.
  App reads `meta.build_date` to detect a newer database.

Requirements:
  pip install zstandard
  wget  (system package)
"""

import argparse, csv, gzip, io, os, sqlite3, struct, sys, time, urllib.request, zipfile
from pathlib import Path

# OFF rows can have very large ingredient/description fields
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False
    print("WARNING: zstandard not installed → only gzip. pip install zstandard")

# ── Directories ───────────────────────────────────────────────────────────────
ROOT   = Path(__file__).parent.parent
ASSETS = ROOT / 'assets'
RAW    = Path('/tmp/usda_raw')
ASSETS.mkdir(exist_ok=True)
RAW.mkdir(exist_ok=True)

# ── USDA source URLs ───────────────────────────────────────────────────────────
URLS = {
    'sr':        'https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip',
    'foundation':'https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2025-12-18.zip',
    'fndds':     'https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_survey_food_csv_2024-10-31.zip',
    'branded':   'https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_branded_food_csv_2025-12-18.zip',
    'off':       'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
}

# ── Nutrient schema (28 nutrients, BLOB layout) ───────────────────────────────
# Each entry: (column_name, usda_nutrient_id, uint16_scale_factor)
# Stored value = round(real_value × scale), clamped 0–65534.
# 0xFFFF (65535) = NULL.  Decode: real_value ≈ stored / scale.
NUTRIENTS = [
    ('cal',       1008,    1),   # kcal          — integers, max ~9000
    ('cal_kj',    1062,    1),   # kJ            — integers
    ('pro',       1003,   10),   # g protein      — × 10  → 0.1 g precision
    ('fat',       1004,   10),   # g total fat
    ('carb',      1005,   10),   # g carbs
    ('fiber',     1079,   10),   # g fiber
    ('sugar',     2000,   10),   # g total sugars (fallback: nutrient 1063)
    ('sodium',    1093,    1),   # mg sodium      — integers, max ~50000
    ('sat_fat',   1258,   10),   # g sat fat
    ('trans_fat', 1257,   10),   # g trans fat
    ('chol',      1253,    1),   # mg cholesterol
    ('mono_fat',  1292,   10),   # g mono fat
    ('poly_fat',  1293,   10),   # g poly fat
    ('vit_a',     1106,    1),   # µg RAE vit A
    ('vit_c',     1162,   10),   # mg vit C
    ('vit_d',     1114,  100),   # µg vit D
    ('vit_e',     1109,   10),   # mg vit E
    ('vit_k',     1185,   10),   # µg vit K
    ('vit_b6',    1175,  100),   # mg B6
    ('vit_b12',   1178, 1000),   # µg B12
    ('folate',    1190,    1),   # µg folate DFE
    ('niacin',    1167,   10),   # mg niacin
    ('calcium',   1087,    1),   # mg calcium
    ('iron',      1089,   10),   # mg iron
    ('magnesium', 1090,    1),   # mg magnesium
    ('potassium', 1092,    1),   # mg potassium
    ('zinc',      1095,   10),   # mg zinc
    ('choline',   1180,    1),   # mg choline
]
N_NUT      = len(NUTRIENTS)
BLOB_FMT   = f'<{N_NUT}H'        # 56 bytes, little-endian unsigned shorts
BLOB_BYTES = struct.calcsize(BLOB_FMT)
NUT_COLS   = [n[0] for n in NUTRIENTS]
NUT_ID_MAP = {str(n[1]): i for i, n in enumerate(NUTRIENTS)}   # nutrient_id → slot index
SUGAR_FALLBACK = '1063'   # use if 2000 absent

assert BLOB_BYTES == N_NUT * 2, f"BLOB size mismatch: {BLOB_BYTES}"


def encode_blob(nd: dict) -> bytes:
    """Encode {nutrient_id_str: float} → 56-byte BLOB."""
    vals = []
    for col, nid, scale in NUTRIENTS:
        key = str(nid)
        v = nd.get(key)
        if v is None and key == '2000':
            v = nd.get(SUGAR_FALLBACK)
        vals.append(0xFFFF if v is None else min(65534, max(0, int(round(v * scale)))))
    return struct.pack(BLOB_FMT, *vals)


def decode_blob(blob: bytes) -> dict:
    """Decode BLOB → {col_name: float|None}."""
    return {
        col: (None if v == 0xFFFF else v / scale)
        for (col, _, scale), v in zip(NUTRIENTS, struct.unpack(BLOB_FMT, blob))
    }


# ── I/O helpers ───────────────────────────────────────────────────────────────
def download(url: str, dest: Path, label: str):
    if dest.exists() and dest.stat().st_size > 1_000:
        print(f"  ✓ {label}  ({dest.stat().st_size/1e6:.1f} MB, cached)")
        return
    print(f"  ↓ {label}  {url}")
    t = time.time()
    urllib.request.urlretrieve(url, dest)
    print(f"    done  {dest.stat().st_size/1e6:.1f} MB  {time.time()-t:.0f}s")


def new_db(path: Path) -> sqlite3.Connection:
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    con.execute('PRAGMA journal_mode=OFF')   # faster bulk insert
    con.execute('PRAGMA synchronous=OFF')
    con.execute('PRAGMA cache_size=-128000')
    return con


def finalize_db(con: sqlite3.Connection, source: str, count: int):
    con.execute('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)')
    con.execute('INSERT OR REPLACE INTO meta VALUES ("build_date",?)', [time.strftime('%Y-%m-%d')])
    con.execute('INSERT OR REPLACE INTO meta VALUES ("source",?)',     [source])
    con.execute('INSERT OR REPLACE INTO meta VALUES ("food_count",?)', [str(count)])
    con.commit()
    con.isolation_level = None
    con.execute('VACUUM')
    con.close()


def compress_db(path: Path) -> dict:
    sz = {'db': path.stat().st_size}
    gz = path.with_suffix('.db.gz')
    with open(path, 'rb') as fi, gzip.open(gz, 'wb', compresslevel=9) as fo:
        fo.write(fi.read())
    sz['gz'] = gz.stat().st_size
    zst = path.with_suffix('.db.zst')
    if HAS_ZSTD:
        cctx = zstd.ZstdCompressor(level=19, threads=-1)
        with open(path, 'rb') as fi, open(zst, 'wb') as fo:
            cctx.copy_stream(fi, fo)
        sz['zst'] = zst.stat().st_size
    else:
        import subprocess
        result = subprocess.run(
            ['zstd', '-19', '--force', '-q', str(path), '-o', str(zst)],
            capture_output=True
        )
        if result.returncode == 0:
            sz['zst'] = zst.stat().st_size
    return sz


def print_sizes(label: str, sz: dict, food_count: int):
    db_mb  = sz['db'] / 1e6
    gz_mb  = sz.get('gz', 0) / 1e6
    zst_mb = sz.get('zst', 0) / 1e6
    print(f"\n  ── {label}  ({food_count:,} foods) ──")
    print(f"     SQLite : {db_mb:>8.2f} MB")
    print(f"     gzip-9 : {gz_mb:>8.2f} MB  ({100*sz.get('gz',sz['db'])//sz['db']}%)")
    if 'zst' in sz:
        print(f"     zstd-19: {zst_mb:>8.2f} MB  ({100*sz['zst']//sz['db']}%)")


def zip_find(z: zipfile.ZipFile, basename: str) -> str:
    """Find first zip entry whose filename (after last /) exactly matches basename."""
    return next(n for n in z.namelist() if n.split('/')[-1].lower() == basename.lower())


def load_nutrients_from_zip(zpath: Path, food_ids: set) -> dict:
    """Stream food_nutrient.csv from a zip → {fdc_id: {nid_str: float}}."""
    result = {fid: {} for fid in food_ids}
    with zipfile.ZipFile(zpath) as z:
        try:
            fn = zip_find(z, 'food_nutrient.csv')
        except StopIteration:
            return result
        with z.open(fn) as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
                fid = row.get('fdc_id', '')
                if fid not in result:
                    continue
                nid = row.get('nutrient_id', '')
                if nid not in NUT_ID_MAP:
                    continue
                try:
                    result[fid][nid] = float(row['amount'])
                except (ValueError, KeyError):
                    pass
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 1. GENERIC (SR Legacy + Foundation + FNDDS)
# ══════════════════════════════════════════════════════════════════════════════
def build_generic(skip_download=False):
    print("\n" + "="*60)
    print("GENERIC  (SR Legacy + Foundation + FNDDS)")
    print("="*60)
    t0 = time.time()

    zips = {
        'sr':        RAW / 'sr_legacy.zip',
        'foundation':RAW / 'foundation.zip',
        'fndds':     RAW / 'fndds.zip',
    }
    if not skip_download:
        for key, path in zips.items():
            download(URLS[key], path, key)

    con = new_db(ASSETS / 'foods_generic.db')
    con.executescript('''
        CREATE TABLE category (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE food (
            id        INTEGER PRIMARY KEY,
            name      TEXT NOT NULL,
            cat_id    INTEGER REFERENCES category(id),
            serving_g REAL,
            source    TEXT,
            nutrients BLOB
        );
        CREATE INDEX food_cat ON food(cat_id);
    ''')

    categories = {}        # id_str → name
    all_foods  = {}        # fdc_id → {name, cat_id, serving_g, source, nd}

    def ingest(zpath: Path, source: str, data_type_filter: str):
        """Load foods of a specific data_type from a USDA zip archive."""
        with zipfile.ZipFile(zpath) as z:
            # Load categories (food_category.csv or wweia_food_category.csv)
            for cat_file in ('food_category.csv', 'wweia_food_category.csv'):
                try:
                    cf = zip_find(z, cat_file)
                    with z.open(cf) as f:
                        for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
                            cid   = (row.get('id') or row.get('wweia_food_category_code','') or '').strip()
                            cname = (row.get('description') or row.get('wweia_food_category_description','')).strip()
                            if cid and cname:
                                categories[cid] = cname
                    break
                except StopIteration:
                    pass

            # Load foods — filter by data_type
            new_fids = set()
            ff = zip_find(z, 'food.csv')
            with z.open(ff) as f:
                for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
                    if row.get('data_type','') != data_type_filter:
                        continue
                    fid  = row.get('fdc_id','').strip()
                    name = (row.get('description') or '').strip()
                    if not name:
                        continue
                    cat = row.get('food_category_id','') or row.get('wweia_food_category_code','')
                    sv  = None
                    for sv_col in ('serving_size', 'gram_weight'):
                        try:
                            sv = float(row.get(sv_col,'') or '')
                            break
                        except (ValueError, TypeError):
                            pass
                    if fid not in all_foods:
                        all_foods[fid] = {'name':name,'cat':cat,'sv':sv,'source':source,'nd':{}}
                    else:
                        # Foundation has higher-quality names — overwrite
                        if source == 'foundation':
                            all_foods[fid]['name']   = name
                            all_foods[fid]['source'] = source
                    new_fids.add(fid)

        # Load nutrients for this zip's food IDs only
        nd_map = load_nutrients_from_zip(zpath, new_fids)
        for fid, nd in nd_map.items():
            existing = all_foods[fid]['nd']
            for k, v in nd.items():
                if k not in existing:
                    existing[k] = v

        print(f"  {source}: {len(all_foods):,} cumulative foods ({len(new_fids):,} new)")

    # Load in ascending priority — Foundation is last so it can overwrite SR names
    ingest(zips['sr'],          'sr',         'sr_legacy_food')
    ingest(zips['fndds'],       'fndds',      'survey_fndds_food')
    ingest(zips['foundation'],  'foundation', 'foundation_food')

    # Insert categories
    for cid, cname in categories.items():
        try:
            con.execute('INSERT OR IGNORE INTO category(id,name) VALUES (?,?)',
                        [int(cid), cname])
        except (ValueError, sqlite3.Error):
            pass
    con.commit()

    # Insert foods
    rows = []
    for fid, food in all_foods.items():
        cat_id = None
        try:
            cat_id = int(food['cat']) if food['cat'] else None
        except (ValueError, TypeError):
            pass
        rows.append([food['name'], cat_id, food['sv'], food['source'], encode_blob(food['nd'])])

    con.executemany(
        'INSERT INTO food(name,cat_id,serving_g,source,nutrients) VALUES (?,?,?,?,?)',
        rows
    )
    con.commit()

    # FTS5 on food names
    con.execute('''
        CREATE VIRTUAL TABLE food_fts USING fts5(
            name, content=food, content_rowid=id,
            tokenize="unicode61 remove_diacritics 2"
        )
    ''')
    con.execute('INSERT INTO food_fts(food_fts) VALUES ("rebuild")')
    con.commit()

    finalize_db(con, 'usda_generic', len(rows))
    sz = compress_db(ASSETS / 'foods_generic.db')
    print_sizes('foods_generic.db', sz, len(rows))
    print(f"  Elapsed: {time.time()-t0:.1f}s")
    return len(rows)


# ══════════════════════════════════════════════════════════════════════════════
# 2. BRANDED (USDA ~2M products with barcodes)
# ══════════════════════════════════════════════════════════════════════════════
def build_branded(skip_download=False):
    print("\n" + "="*60)
    print("BRANDED  (USDA ~2M products)")
    print("="*60)
    t0 = time.time()

    zpath = RAW / 'branded_2025-12.zip'
    if not skip_download:
        download(URLS['branded'], zpath, 'branded')

    z = zipfile.ZipFile(zpath)

    con = new_db(ASSETS / 'foods_branded.db')
    con.executescript('''
        CREATE TABLE brand (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
        CREATE TABLE food (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            brand_id    INTEGER REFERENCES brand(id),
            barcode     TEXT,
            serving_g   REAL,
            serving_txt TEXT,
            category    TEXT,
            nutrients   BLOB
        );
        CREATE INDEX food_barcode  ON food(barcode);
        CREATE INDEX food_brand_id ON food(brand_id);
    ''')

    print("  Loading branded_food.csv metadata...")
    branded    = {}    # fdc_id → dict
    brand_ids  = {}    # brand_name → int id
    with z.open(zip_find(z, 'branded_food.csv')) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
            fid   = row['fdc_id']
            brand = (row.get('brand_name') or row.get('brand_owner') or '').strip()[:80]
            if brand and brand not in brand_ids:
                brand_ids[brand] = len(brand_ids) + 1
            branded[fid] = {
                'brand_id':    brand_ids.get(brand),
                'barcode':     row.get('gtin_upc') or None,
                'serving_g':   row.get('serving_size') or None,
                'serving_txt': (row.get('household_serving_fulltext') or '')[:50] or None,
                'category':    (row.get('branded_food_category') or '')[:60] or None,
            }
    print(f"  {len(branded):,} entries, {len(brand_ids):,} unique brands")

    # Bulk-insert brand lookup table
    con.executemany('INSERT INTO brand(id,name) VALUES (?,?)',
                    [(bid, bn) for bn, bid in brand_ids.items()])
    con.commit()

    print("  Loading food.csv names...")
    names = {}
    with z.open(zip_find(z, 'food.csv')) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
            fid = row['fdc_id']
            if fid in branded:
                names[fid] = row['description'][:150]

    print(f"  Streaming food_nutrient.csv (~26M rows)...")
    nd_all  = {fid: {} for fid in branded}
    tick    = 0
    with z.open(zip_find(z, 'food_nutrient.csv')) as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding='utf-8')):
            fid = row['fdc_id']
            if fid not in nd_all:
                continue
            nid = row['nutrient_id']
            if nid not in NUT_ID_MAP:
                continue
            try:
                nd_all[fid][nid] = float(row['amount'])
            except (ValueError, KeyError):
                pass
            tick += 1
            if tick % 5_000_000 == 0:
                print(f"    {tick//1_000_000}M rows...  {time.time()-t0:.0f}s")

    print("  Inserting into SQLite (batches of 50k)...")
    BATCH = 50_000
    buf   = []
    n_in  = 0
    con.execute('BEGIN')
    for fid, b in branded.items():
        nm = names.get(fid)
        if not nm:
            continue
        sg = None
        try:
            if b['serving_g']:
                sg = float(b['serving_g'])
        except (ValueError, TypeError):
            pass
        buf.append([nm, b['brand_id'], b['barcode'], sg,
                    b['serving_txt'], b['category'], encode_blob(nd_all.get(fid, {}))])
        if len(buf) >= BATCH:
            con.executemany(
                'INSERT INTO food(name,brand_id,barcode,serving_g,serving_txt,category,nutrients)'
                ' VALUES (?,?,?,?,?,?,?)', buf)
            n_in += len(buf)
            buf   = []
            if n_in % 200_000 == 0:
                print(f"    {n_in:,} inserted...")
    if buf:
        con.executemany(
            'INSERT INTO food(name,brand_id,barcode,serving_g,serving_txt,category,nutrients)'
            ' VALUES (?,?,?,?,?,?,?)', buf)
        n_in += len(buf)
    con.execute('COMMIT')
    print(f"  {n_in:,} foods inserted")

    # Name-only FTS (content= so no duplication of text in FTS table)
    print("  Building name FTS5 index...")
    con.execute('''
        CREATE VIRTUAL TABLE food_name_fts USING fts5(
            name,
            content=food, content_rowid=id,
            tokenize="unicode61 remove_diacritics 2"
        )
    ''')
    con.execute('INSERT INTO food_name_fts(food_name_fts) VALUES ("rebuild")')
    con.commit()

    finalize_db(con, 'usda_branded', n_in)
    sz = compress_db(ASSETS / 'foods_branded.db')
    print_sizes('foods_branded.db', sz, n_in)
    print(f"  Elapsed: {time.time()-t0:.1f}s")
    return n_in


# ══════════════════════════════════════════════════════════════════════════════
# 3. GLOBAL (Open Food Facts — multilingual)
# ══════════════════════════════════════════════════════════════════════════════
def build_global(skip_download=False):
    print("\n" + "="*60)
    print("GLOBAL  (Open Food Facts — multilingual including Serbian/Cyrillic)")
    print("="*60)
    t0 = time.time()

    off_path = RAW / 'off_world.csv.gz'
    if not skip_download:
        download(URLS['off'], off_path, 'Open Food Facts')

    con = new_db(ASSETS / 'foods_global.db')
    con.executescript('''
        CREATE TABLE food (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            name_sr     TEXT,
            name_ru     TEXT,
            name_uk     TEXT,
            name_pl     TEXT,
            name_hr     TEXT,
            name_bs     TEXT,
            name_fr     TEXT,
            name_de     TEXT,
            name_es     TEXT,
            brand       TEXT,
            barcode     TEXT,
            serving_g   REAL,
            serving_txt TEXT,
            category    TEXT,
            country     TEXT,
            nutrients   BLOB
        );
        CREATE INDEX food_barcode ON food(barcode);
    ''')

    # OFF CSV nutrient columns → our nutrient_ids
    # OFF values are per 100g, in grams (except energy)
    def off_to_ndict(row: dict) -> dict:
        def flt(k):
            v = (row.get(k) or '').strip()
            if not v or v in ('-','unknown','not-applicable','none'): return None
            try: return float(v)
            except ValueError: return None

        # Energy
        cal = flt('energy-kcal_100g')
        if cal is None:
            kj = flt('energy_100g')
            if kj: cal = kj / 4.184

        # Sodium: OFF stores g/100g, we need mg/100g
        sodium = flt('sodium_100g')
        if sodium is not None:
            sodium = sodium * 1000
        else:
            salt = flt('salt_100g')
            if salt: sodium = salt * 390

        def g_to_mg(k):   # g/100g → mg/100g
            v = flt(k); return v * 1000 if v is not None else None

        def g_to_ug(k):   # g/100g → µg/100g
            v = flt(k); return v * 1_000_000 if v is not None else None

        return {
            '1008': cal,
            '1003': flt('proteins_100g'),              # g — already in g
            '1004': flt('fat_100g'),                   # g
            '1005': flt('carbohydrates_100g'),          # g
            '1079': flt('fiber_100g'),                  # g
            '2000': flt('sugars_100g'),                 # g
            '1093': sodium,                             # mg
            '1258': flt('saturated-fat_100g'),          # g
            '1257': flt('trans-fat_100g'),              # g
            '1253': g_to_mg('cholesterol_100g'),        # g→mg
            '1292': flt('monounsaturated-fat_100g'),    # g
            '1293': flt('polyunsaturated-fat_100g'),    # g
            '1106': g_to_ug('vitamin-a_100g'),          # g→µg
            '1162': g_to_mg('vitamin-c_100g'),          # g→mg
            '1114': g_to_ug('vitamin-d_100g'),          # g→µg
            '1109': g_to_mg('vitamin-e_100g'),          # g→mg
            '1185': g_to_ug('vitamin-k_100g'),          # g→µg
            '1175': g_to_mg('vitamin-b6_100g'),         # g→mg
            '1178': g_to_ug('vitamin-b12_100g'),        # g→µg
            '1190': g_to_ug('folates_100g'),            # g→µg
            '1167': g_to_mg('niacin_100g'),             # g→mg
            '1087': g_to_mg('calcium_100g'),            # g→mg
            '1089': g_to_mg('iron_100g'),               # g→mg
            '1090': g_to_mg('magnesium_100g'),          # g→mg
            '1092': g_to_mg('potassium_100g'),          # g→mg
            '1095': g_to_mg('zinc_100g'),               # g→mg
        }

    # Quality gate: must have at least N non-empty nutrient columns
    OFF_NUT_COLS = [
        'energy-kcal_100g','energy_100g','proteins_100g','fat_100g',
        'carbohydrates_100g','fiber_100g','sugars_100g','sodium_100g',
        'saturated-fat_100g',
    ]
    MIN_NUTRIENTS = 3

    print("  Streaming OFF CSV (may take several minutes)...")
    BATCH  = 20_000
    buf    = []
    n_in   = 0
    n_skip = 0

    def flush():
        nonlocal n_in
        con.execute('BEGIN')
        con.executemany('''
            INSERT INTO food(name,name_sr,name_ru,name_uk,name_pl,name_hr,name_bs,
                             name_fr,name_de,name_es,
                             brand,barcode,serving_g,serving_txt,category,country,nutrients)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', buf)
        con.execute('COMMIT')
        n_in += len(buf)
        buf.clear()

    def s(row, key, maxlen=120):
        v = (row.get(key) or '').strip()
        return v[:maxlen] if v else None

    with gzip.open(off_path, 'rt', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for i, row in enumerate(reader):
            if i % 1_000_000 == 0 and i:
                print(f"    {i/1e6:.1f}M rows  {n_in:,} inserted  {time.time()-t0:.0f}s")

            # Primary English name
            name = (
                (row.get('product_name_en') or '').strip() or
                (row.get('product_name') or '').strip()    or
                (row.get('generic_name') or '').strip()
            )
            if not name or len(name) < 2:
                n_skip += 1
                continue

            # Nutrient quality gate
            n_nut = sum(1 for c in OFF_NUT_COLS if (row.get(c) or '').strip() not in ('','-'))
            if n_nut < MIN_NUTRIENTS:
                n_skip += 1
                continue

            sg = None
            try:
                sq = (row.get('serving_quantity') or '').strip()
                if sq: sg = float(sq)
            except (ValueError, TypeError):
                pass

            # Brand: take first brand if comma-separated
            brand = (row.get('brands') or '').split(',')[0].strip()[:80] or None
            cat   = (row.get('categories_en') or '').split(',')[0].strip()[:60] or None
            cntry = (row.get('countries_en') or '').split(',')[0].strip()[:40] or None

            buf.append([
                name[:150],
                s(row,'product_name_sr'),
                s(row,'product_name_ru'),
                s(row,'product_name_uk'),
                s(row,'product_name_pl'),
                s(row,'product_name_hr'),
                s(row,'product_name_bs'),
                s(row,'product_name_fr'),
                s(row,'product_name_de'),
                s(row,'product_name_es'),
                brand,
                (row.get('code') or '').strip() or None,
                sg,
                s(row,'serving_size',50),
                cat,
                cntry,
                encode_blob(off_to_ndict(row)),
            ])
            if len(buf) >= BATCH:
                flush()

    if buf:
        flush()
    print(f"  {n_in:,} inserted, {n_skip:,} skipped")

    # Multi-language FTS (content= references food table, no text duplication)
    print("  Building multilingual FTS5 index...")
    con.execute('''
        CREATE VIRTUAL TABLE food_fts USING fts5(
            name, name_sr, name_ru, name_uk, name_pl, name_hr, name_bs,
            name_fr, name_de, name_es,
            content=food, content_rowid=id,
            tokenize="unicode61 remove_diacritics 2"
        )
    ''')
    con.execute('INSERT INTO food_fts(food_fts) VALUES ("rebuild")')
    con.commit()

    finalize_db(con, 'openfoodfacts', n_in)
    sz = compress_db(ASSETS / 'foods_global.db')
    print_sizes('foods_global.db', sz, n_in)
    print(f"  Elapsed: {time.time()-t0:.1f}s")
    return n_in


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(description='Build MicroCore food databases')
    ap.add_argument('--skip-generic',  action='store_true')
    ap.add_argument('--skip-branded',  action='store_true')
    ap.add_argument('--skip-global',   action='store_true')
    ap.add_argument('--no-download',   action='store_true',
                    help='Use cached zip/gz files only (skip wget)')
    args = ap.parse_args()

    print(f"MicroCore Food DB Builder  {time.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Assets → {ASSETS}")
    print(f"  Blob   → {BLOB_BYTES} bytes/food  ({N_NUT} nutrients × 2 bytes)")

    t0 = time.time()
    if not args.skip_generic:
        build_generic(args.no_download)
    if not args.skip_branded:
        build_branded(args.no_download)
    if not args.skip_global:
        build_global(args.no_download)

    print("\n" + "="*60)
    print("DONE")
    print("="*60)
    for fname in ('foods_generic.db','foods_branded.db','foods_global.db'):
        db = ASSETS / fname
        if not db.exists():
            continue
        zst = db.with_suffix('.db.zst')
        gz  = db.with_suffix('.db.gz')
        zst_s = f"  zstd {zst.stat().st_size/1e6:.1f}MB" if zst.exists() else ''
        gz_s  = f"  gzip {gz.stat().st_size/1e6:.1f}MB"  if gz.exists()  else ''
        print(f"  {fname:<30} {db.stat().st_size/1e6:>8.1f}MB{gz_s}{zst_s}")
    print(f"\n  Total: {time.time()-t0:.0f}s")
    print()
    print("Next steps:")
    print("  1. Run: bash scripts/publish.sh YYYY-MM  to create a GitHub Release")
    print("  2. Or: trigger the Build & Release workflow in GitHub Actions")


if __name__ == '__main__':
    main()
