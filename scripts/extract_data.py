import os
import pandas as pd
from database_connect import get_sql_engine, get_access_connection
from config import RAW_DIR

os.makedirs(RAW_DIR, exist_ok=True)

# ⚠️ Northwind contient "Order Details" (avec espace)
TABLES = [
    "Orders",
    "Customers",
    "Employees",
    "Order Details",
    "Shippers",
    "Territories",
    "EmployeeTerritories",
    "Region",
]

def _safe_name(table: str) -> str:
    return table.replace(" ", "_").lower()

def extract_from_sql_server():
    engine = get_sql_engine()
    if engine is None:
        return

    for table in TABLES:
        try:
            query = f"SELECT * FROM [{table}]"   # ALWAYS bracket (safe)
            df = pd.read_sql(query, engine)

            file_name = f"sql_{_safe_name(table)}.csv"
            out_path = os.path.join(RAW_DIR, file_name)
            df.to_csv(out_path, index=False)

            print(f"SQL Server: {table} -> {file_name} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ SQL Server Error ({table}): {e}")

def extract_from_access():
    conn = get_access_connection()
    if conn is None:
        return

    for table in TABLES:
        try:
            query = f"SELECT * FROM [{table}]"
            df = pd.read_sql(query, conn)

            file_name = f"access_{_safe_name(table)}.csv"
            out_path = os.path.join(RAW_DIR, file_name)
            df.to_csv(out_path, index=False)

            print(f"Access:     {table} -> {file_name} ({len(df)} rows)")
        except Exception as e:
            print(f"❌ Access Error ({table}): {e}")

    conn.close()

if __name__ == "__main__":
    extract_from_sql_server()
    extract_from_access()
    print("\n✅ Extraction Complete!")
