import os
import pandas as pd
from config import WAREHOUSE_DIR

TABLES = {
    "DimDate": "sk_date",
    "DimClient": "sk_client",
    "DimEmployee": "sk_employee",
    "FactSales": "fact_id",
}

def validate_warehouse():
    print("\n--- Warehouse Validation ---")

    ok = True

    for table, pk in TABLES.items():
        path = os.path.join(WAREHOUSE_DIR, f"{table}.csv")
        if not os.path.exists(path):
            print(f"❌ Missing warehouse table file: {table}.csv")
            ok = False
            continue

        df = pd.read_csv(path)

        print(f"\n📌 {table}: {len(df)} rows, {df.shape[1]} cols")

        # PK checks
        if pk not in df.columns:
            print(f"❌ PK column missing: {pk}")
            ok = False
        else:
            null_pk = df[pk].isna().sum()
            dup_pk = df[pk].duplicated().sum()
            if null_pk > 0:
                print(f"❌ Null PK values: {null_pk}")
                ok = False
            if dup_pk > 0:
                print(f"❌ Duplicate PK values: {dup_pk}")
                ok = False
            if null_pk == 0 and dup_pk == 0:
                print("✅ PK OK")

        # basic null scan
        nulls = df.isna().sum().sort_values(ascending=False)
        top_nulls = nulls[nulls > 0].head(5)
        if len(top_nulls) > 0:
            print("⚠️ Top null columns:")
            for c, n in top_nulls.items():
                print(f"   - {c}: {n}")
        else:
            print("✅ No nulls detected")

    if ok:
        print("\n✅ Warehouse Validation PASSED")
    else:
        print("\n❌ Warehouse Validation FAILED (see errors above)")
