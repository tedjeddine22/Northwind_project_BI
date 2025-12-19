import os
import pandas as pd
from config import STAGING_DIR, WAREHOUSE_DIR

os.makedirs(WAREHOUSE_DIR, exist_ok=True)

def generate_schema_sql(tables: dict[str, pd.DataFrame]):
    pks = {
        "DimDate": "sk_date",
        "DimClient": "sk_client",
        "DimEmployee": "sk_employee",
        "FactSales": "fact_id",
    }

    sql_statements = []
    print("... Generating schema.sql")

    for table_name, df in tables.items():
        cols_sql = []
        for col, dtype in df.dtypes.items():
            dt = str(dtype).lower()
            if "int" in dt:
                sql_type = "INT"
            elif "float" in dt:
                sql_type = "DECIMAL(10,2)"
            elif "datetime" in dt:
                sql_type = "DATE"
            else:
                sql_type = "VARCHAR(255)"

            if col == pks.get(table_name):
                sql_type += " PRIMARY KEY"

            cols_sql.append(f"    {col} {sql_type}")

        sql_statements.append(
            f"CREATE TABLE {table_name} (\n" + ",\n".join(cols_sql) + "\n);\n"
        )

    with open(os.path.join(WAREHOUSE_DIR, "schema.sql"), "w", encoding="utf-8") as f:
        f.write("\n".join(sql_statements))

def load_to_warehouse():
    print("\n--- Starting Load (Staging -> Warehouse) ---")

    file_mapping = {
        "cleaned_date": "DimDate",
        "cleaned_clients": "DimClient",
        "cleaned_employees": "DimEmployee",
        "cleaned_sales": "FactSales",
    }

    loaded_tables = {}

    for csv_name, table_name in file_mapping.items():
        path = os.path.join(STAGING_DIR, f"{csv_name}.csv")
        if not os.path.exists(path):
            print(f"⚠️ Missing staging file: {csv_name}.csv")
            continue

        df = pd.read_csv(path)
        loaded_tables[table_name] = df

        df.to_csv(os.path.join(WAREHOUSE_DIR, f"{table_name}.csv"), index=False)
        df.to_parquet(os.path.join(WAREHOUSE_DIR, f"{table_name}.parquet"), index=False)

        print(f"✅ {csv_name} -> {table_name} ({len(df)} rows)")

    if loaded_tables:
        generate_schema_sql(loaded_tables)

    print("✅ Load Complete! Warehouse ready.")
