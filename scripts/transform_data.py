import os
import glob
import pandas as pd
from config import RAW_DIR, STAGING_DIR

os.makedirs(STAGING_DIR, exist_ok=True)

def clean_col_names(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df.columns = [c.lower().strip().replace(" ", "").replace("_", "") for c in df.columns]
    return df

def load_raw_data(table_name: str, pk_cols: list[str]) -> pd.DataFrame:
    """
    table_name must match the extracted file suffix:
      Orders -> *_orders.csv
      Order Details -> *_order_details.csv
    """
    dfs = []
    pattern = os.path.join(RAW_DIR, f"*_{table_name}.csv")
    files = glob.glob(pattern)
    print(f"   - Found {len(files)} source files for '{table_name}'")

    for f in files:
        try:
            df = pd.read_csv(f)
            df = clean_col_names(df)
            dfs.append(df)
        except Exception as e:
            print(f"❌ Error loading {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)

    # drop duplicates by PK if possible
    clean_pks = [c.lower().replace(" ", "").replace("_", "") for c in pk_cols]
    if set(clean_pks).issubset(set(merged.columns)):
        merged = merged.drop_duplicates(subset=clean_pks, keep="first")

    return merged

def transform_dim_date(orders: pd.DataFrame) -> pd.DataFrame:
    print("... Creating DimDate")
    if orders.empty or "orderdate" not in orders.columns:
        return pd.DataFrame()

    orders["orderdate"] = orders["orderdate"].astype(str)
    try:
        dates = pd.to_datetime(orders["orderdate"], format="mixed", errors="coerce").dropna().unique()
    except Exception:
        dates = pd.to_datetime(orders["orderdate"], infer_datetime_format=True, errors="coerce").dropna().unique()

    d = pd.DataFrame({"full_date": dates}).sort_values("full_date")
    d["sk_date"] = d["full_date"].dt.strftime("%Y%m%d").astype(int)
    d["year"] = d["full_date"].dt.year
    d["month"] = d["full_date"].dt.month
    d["month_name"] = d["full_date"].dt.month_name()
    d["quarter"] = d["full_date"].dt.quarter
    return d

def transform_dim_client(customers: pd.DataFrame) -> pd.DataFrame:
    print("... Creating DimClient")
    if customers.empty:
        return pd.DataFrame()

    dim = customers.copy()
    dim = dim.rename(columns={
        "customerid": "bk_customer_id",
        "companyname": "company_name",
    })

    if "bk_customer_id" in dim.columns:
        dim["bk_customer_id"] = dim["bk_customer_id"].astype(str).str.upper().str.strip()

    for col in ["city", "country", "region"]:
        if col not in dim.columns:
            dim[col] = "Unknown"
        else:
            dim[col] = dim[col].astype(str).fillna("Unknown").str.strip()

    dim["sk_client"] = range(1, len(dim) + 1)
    cols = ["sk_client", "bk_customer_id", "company_name", "city", "country", "region"]
    return dim[[c for c in cols if c in dim.columns]]

def enrich_employee_with_territories(dim_emp: pd.DataFrame,
                                    emp_terr: pd.DataFrame,
                                    terr: pd.DataFrame,
                                    region: pd.DataFrame) -> pd.DataFrame:
    if emp_terr.empty or terr.empty:
        dim_emp["territories"] = "Unknown"
        dim_emp["sales_region"] = "Unknown"
        return dim_emp

    merged = pd.merge(emp_terr, terr, on="territoryid", how="left")

    # region join (optional)
    if (not region.empty) and ("regionid" in merged.columns) and ("regionid" in region.columns):
        merged = pd.merge(merged, region, on="regionid", how="left")
        region_col = "regiondescription"
    else:
        merged["regiondescription"] = "Unknown"
        region_col = "regiondescription"

    if "territorydescription" in merged.columns:
        merged["territorydescription"] = merged["territorydescription"].astype(str).str.strip()
    merged[region_col] = merged[region_col].astype(str).str.strip()

    terr_grouped = merged.groupby("employeeid")["territorydescription"].apply(
        lambda x: ", ".join([v for v in x.dropna().astype(str).tolist() if v.strip() != ""])
    ).reset_index(name="territories")

    region_grouped = merged.groupby("employeeid")[region_col].apply(
        lambda x: ", ".join(sorted(set([v for v in x.dropna().astype(str).tolist() if v.strip() != ""])))
    ).reset_index(name="sales_region")

    # join to dim employee on bk_employee_id
    dim_emp = pd.merge(dim_emp, terr_grouped, left_on="bk_employee_id", right_on="employeeid", how="left")
    dim_emp = pd.merge(dim_emp, region_grouped, left_on="bk_employee_id", right_on="employeeid", how="left")

    dim_emp["territories"] = dim_emp["territories"].fillna("No Territory")
    dim_emp["sales_region"] = dim_emp["sales_region"].fillna("No Region")

    return dim_emp.drop(columns=["employeeid_x", "employeeid_y"], errors="ignore")

def transform_dim_employee(employees: pd.DataFrame,
                           emp_terr: pd.DataFrame,
                           terr: pd.DataFrame,
                           region: pd.DataFrame) -> pd.DataFrame:
    print("... Creating DimEmployee")
    if employees.empty:
        return pd.DataFrame()

    dim = employees.copy()
    dim = dim.rename(columns={
        "employeeid": "bk_employee_id",
        "firstname": "first_name",
        "lastname": "last_name",
    })

    if "first_name" in dim.columns and "last_name" in dim.columns:
        dim["employee_name"] = dim["first_name"].astype(str).str.strip() + " " + dim["last_name"].astype(str).str.strip()
    else:
        dim["employee_name"] = "Unknown"

    dim["sk_employee"] = range(1, len(dim) + 1)

    dim = enrich_employee_with_territories(dim, emp_terr, terr, region)

    for col in ["title", "city", "country"]:
        if col not in dim.columns:
            dim[col] = "Unknown"
        else:
            dim[col] = dim[col].astype(str).fillna("Unknown").str.strip()

    cols = ["sk_employee", "bk_employee_id", "employee_name", "title", "city", "country", "sales_region", "territories"]
    return dim[[c for c in cols if c in dim.columns]]

def transform_fact_sales(orders: pd.DataFrame,
                         details: pd.DataFrame,
                         dim_client: pd.DataFrame,
                         dim_emp: pd.DataFrame) -> pd.DataFrame:
    print("... Creating FactSales")
    if orders.empty or details.empty:
        return pd.DataFrame()

    fact = pd.merge(details, orders, on="orderid", how="inner")

    # unit price column
    if "unitprice_x" in fact.columns:
        price_col = "unitprice_x"
    elif "unitprice" in fact.columns:
        price_col = "unitprice"
    else:
        return pd.DataFrame()

    # numeric safety
    for c in ["quantity", "discount", price_col]:
        if c in fact.columns:
            fact[c] = pd.to_numeric(fact[c], errors="coerce").fillna(0)

    fact["total_amount"] = (fact[price_col] * fact["quantity"]) * (1 - fact["discount"])

    # delivery status (shippeddate can be empty)
    if "shippeddate" in fact.columns:
        fact["delivery_status"] = fact["shippeddate"].apply(
            lambda x: "Livrée" if pd.notnull(x) and str(x).strip() != "" else "Non Livrée"
        )
    else:
        fact["delivery_status"] = "Unknown"

    # clean customerid (FK)
    if "customerid" in fact.columns:
        fact["customerid"] = fact["customerid"].astype(str).str.upper().str.strip()

    # link dimensions
    if not dim_client.empty and {"bk_customer_id", "sk_client"}.issubset(dim_client.columns):
        fact = pd.merge(
            fact,
            dim_client[["bk_customer_id", "sk_client"]],
            left_on="customerid",
            right_on="bk_customer_id",
            how="left"
        )

    if not dim_emp.empty and {"bk_employee_id", "sk_employee"}.issubset(dim_emp.columns):
        fact = pd.merge(
            fact,
            dim_emp[["bk_employee_id", "sk_employee"]],
            left_on="employeeid",
            right_on="bk_employee_id",
            how="left"
        )

    # date key
    if "orderdate" in fact.columns:
        fact["orderdate"] = fact["orderdate"].astype(str)
        try:
            fact["dt"] = pd.to_datetime(fact["orderdate"], format="mixed", errors="coerce")
        except Exception:
            fact["dt"] = pd.to_datetime(fact["orderdate"], infer_datetime_format=True, errors="coerce")
        fact["sk_date"] = fact["dt"].dt.strftime("%Y%m%d").fillna("19000101").astype(int)
    else:
        fact["sk_date"] = 19000101

    fact = fact.rename(columns={"orderid": "bk_order_id", price_col: "unit_price"})
    fact["fact_id"] = range(1, len(fact) + 1)

    final_cols = [
        "fact_id", "bk_order_id", "sk_client", "sk_employee", "sk_date",
        "quantity", "unit_price", "discount", "total_amount", "delivery_status"
    ]
    return fact[[c for c in final_cols if c in fact.columns]]

def run_transformation():
    print("\n--- Starting Transformation (DimDate, DimClient, DimEmployee, FactSales) ---")

    orders = load_raw_data("orders", ["orderid"])
    details = load_raw_data("order_details", ["orderid", "productid"])
    customers = load_raw_data("customers", ["customerid"])
    employees = load_raw_data("employees", ["employeeid"])

    emp_terr = load_raw_data("employeeterritories", ["employeeid", "territoryid"])
    territories = load_raw_data("territories", ["territoryid"])
    region = load_raw_data("region", ["regionid"])

    dim_date = transform_dim_date(orders)
    dim_client = transform_dim_client(customers)
    dim_emp = transform_dim_employee(employees, emp_terr, territories, region)
    fact_sales = transform_fact_sales(orders, details, dim_client, dim_emp)

    print("\n--- Saving Staging Files ---")
    dim_date.to_csv(os.path.join(STAGING_DIR, "cleaned_date.csv"), index=False)
    dim_client.to_csv(os.path.join(STAGING_DIR, "cleaned_clients.csv"), index=False)
    dim_emp.to_csv(os.path.join(STAGING_DIR, "cleaned_employees.csv"), index=False)
    fact_sales.to_csv(os.path.join(STAGING_DIR, "cleaned_sales.csv"), index=False)

    print("✅ Transformation Complete!")
