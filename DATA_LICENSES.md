# Data Licenses

This repository distributes pre-built SQLite databases derived from the following sources.
Each database has its own license terms described below.

---

## `foods_generic.db.gz` — USDA SR Legacy / Foundation / FNDDS

**Source:** [USDA FoodData Central](https://fdc.nal.usda.gov/)  
**Maintained by:** USDA Agricultural Research Service (ARS)  
**License:** **Public Domain** — 17 U.S.C. § 105 (US government works)

> "USDA data are in the public domain and may be used for any purpose
> without permission or fee." — USDA ARS

✅ No attribution required. No restrictions on use or redistribution.

---

## `foods_branded.db.gz` — USDA Branded Food Products

**Source:** [USDA FoodData Central — Branded Foods](https://fdc.nal.usda.gov/food-search?type=Branded)  
**Maintained by:** USDA, in partnership with food manufacturers  
**License:** **Public Domain** — 17 U.S.C. § 105

> Branded food data submitted to USDA FDC by industry partners is made
> publicly available by USDA without restrictions.

✅ No attribution required. No restrictions on use or redistribution.

---

## `foods_global.db.gz` — Open Food Facts

**Source:** [Open Food Facts](https://world.openfoodfacts.org/)  
**Database License:** [Open Database License (ODbL) v1.0](https://opendatacommons.org/licenses/odbl/1-0/)  
**Content License:** [Creative Commons CC-BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) (images, text)

### What ODbL v1.0 requires from redistributors:

1. **Attribution** — You must clearly credit Open Food Facts as the source and
   include a link to the ODbL license in any redistribution or product that
   uses this database.

2. **Share-Alike** — If you redistribute a derivative of this database (which
   this file is — a compiled SQLite derived from the OFF CSV export), the
   derived database must also be released under ODbL v1.0 or a compatible
   license.

3. **Keep Open** — You may not apply technological measures (e.g., DRM, encryption
   with a secret key) that restrict others from exercising the rights granted by
   the license.

### How this release complies:

- This file (`foods_global.db.gz`) is released under **ODbL v1.0**.
- Source data: https://world.openfoodfacts.org/data
- Build script: [`scripts/build_food_db.py`](scripts/build_food_db.py) (MIT licensed, see LICENSE)
- Attribution notice is included in the Microcore app's About screen.

✅ Legal to redistribute. Attribution included. Derivative released under ODbL.

---

## `foods_exercises.db.gz` *(planned)*

Sources and licenses TBD when this database is added.

---

## Summary Table

| File | Source | License | Redistribute? | Attribution required? |
|---|---|---|---|---|
| `foods_generic.db.gz` | USDA FDC | Public Domain | ✅ Yes | ❌ No |
| `foods_branded.db.gz` | USDA FDC | Public Domain | ✅ Yes | ❌ No |
| `foods_global.db.gz` | Open Food Facts | ODbL 1.0 | ✅ Yes | ✅ Yes |
