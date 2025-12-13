import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.raw_dir = os.path.join(base_dir, 'data', 'raw')
        self.processed_dir = os.path.join(base_dir, 'data', 'processed')
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def transform_all(self):
        logger.info("⚙️ Début de la transformation...")
        
        # 1. Chargement des données brutes
        try:
            orders = pd.read_csv(os.path.join(self.raw_dir, 'Orders.csv'))
            
            # Gestion du nom de fichier variable pour Details
            details_path = os.path.join(self.raw_dir, 'Order_Details.csv')
            if not os.path.exists(details_path):
                details_path = os.path.join(self.raw_dir, 'Order Details.csv')
            details = pd.read_csv(details_path)
            
            customers = pd.read_csv(os.path.join(self.raw_dir, 'Customers.csv'))
            employees = pd.read_csv(os.path.join(self.raw_dir, 'Employees.csv'))
            products = pd.read_csv(os.path.join(self.raw_dir, 'Products.csv'))
            
            # ---------------------------------------------------------
            # ETAPE CRITIQUE : Normalisation des noms de colonnes
            # ---------------------------------------------------------
            
            # A. Supprimer les espaces (ex: "Order Date" -> "OrderDate")
            for df in [orders, details, customers, employees, products]:
                df.columns = df.columns.str.replace(' ', '')

            # B. Renommer les colonnes "ID" génériques en noms spécifiques
            # (Access nomme souvent la clé primaire juste "ID")
            
            # Customers : ID -> CustomerID
            if 'ID' in customers.columns:
                logger.info("Correction: ID -> CustomerID dans Customers")
                customers.rename(columns={'ID': 'CustomerID'}, inplace=True)
            
            # Employees : ID -> EmployeeID
            if 'ID' in employees.columns:
                logger.info("Correction: ID -> EmployeeID dans Employees")
                employees.rename(columns={'ID': 'EmployeeID'}, inplace=True)
                
            # Products : ID -> ProductID
            if 'ID' in products.columns:
                logger.info("Correction: ID -> ProductID dans Products")
                products.rename(columns={'ID': 'ProductID'}, inplace=True)

            # Debug : Afficher les colonnes finales pour vérification
            logger.info(f"Colonnes Customers finales : {customers.columns.tolist()}")

        except FileNotFoundError as e:
            logger.error(f"❌ Fichier manquant : {e}")
            return

        # 2. Nettoyage et Harmonisation
        # Dates
        for col in ['OrderDate', 'RequiredDate', 'ShippedDate']:
            if col in orders.columns:
                orders[col] = pd.to_datetime(orders[col], errors='coerce')
            
        # Textes
        if 'City' in customers.columns:
            customers['City'] = customers['City'].str.upper().str.strip()
        if 'Country' in customers.columns:
            customers['Country'] = customers['Country'].str.upper().str.strip()
        
        # Calcul Total Amount
        # On s'assure que UnitPrice existe (parfois "Price")
        if 'UnitPrice' not in details.columns and 'Price' in details.columns:
             details.rename(columns={'Price': 'UnitPrice'}, inplace=True)
             
        details['TotalAmount'] = details['UnitPrice'] * details['Quantity'] * (1 - details['Discount'])

        # 3. Création des DIMENSIONS (Star Schema)
        
        # --- DimDate ---
        dates = pd.concat([orders['OrderDate'], orders['ShippedDate']]).dropna().unique()
        dim_date = pd.DataFrame({'Date': pd.to_datetime(dates)})
        dim_date['DateKey'] = dim_date['Date'].dt.strftime('%Y%m%d').astype(int)
        dim_date['Year'] = dim_date['Date'].dt.year
        dim_date['Month'] = dim_date['Date'].dt.month
        dim_date['MonthName'] = dim_date['Date'].dt.month_name()
        dim_date['Quarter'] = dim_date['Date'].dt.quarter
        
        # --- DimCustomer ---
        # On utilise une compréhension de liste sécurisée
        cols_client = ['CustomerID', 'CompanyName', 'City', 'Country']
        # On ne prend que les colonnes qui existent vraiment
        existing_cols = [c for c in cols_client if c in customers.columns]
        
        if 'CustomerID' not in existing_cols:
            logger.error(f"ERREUR CRITIQUE: CustomerID introuvable dans Customers. Colonnes dispos: {customers.columns}")
            return

        dim_customer = customers[existing_cols].copy()
        dim_customer['CustomerKey'] = range(1, len(dim_customer) + 1)
        
        # --- DimEmployee ---
        cols_emp = ['EmployeeID', 'LastName', 'FirstName', 'Title']
        existing_emp_cols = [c for c in cols_emp if c in employees.columns]
        dim_employee = employees[existing_emp_cols].copy()
        dim_employee['EmployeeKey'] = range(1, len(dim_employee) + 1)
        
        # Gestion du FullName (si FirstName/LastName existent)
        if 'FirstName' in employees.columns and 'LastName' in employees.columns:
            dim_employee['FullName'] = employees['FirstName'] + ' ' + employees['LastName']
        else:
            dim_employee['FullName'] = "Unknown"

        # --- DimProduct ---
        cols_prod = ['ProductID', 'ProductName', 'CategoryID']
        existing_prod_cols = [c for c in cols_prod if c in products.columns]
        dim_product = products[existing_prod_cols].copy()
        dim_product['ProductKey'] = range(1, len(dim_product) + 1)

        # 4. Création de la FACT TABLE (FactSales)
        # Jointures
        # On s'assure que les colonnes de jointure existent dans Orders
        order_cols = ['OrderID', 'CustomerID', 'EmployeeID', 'OrderDate']
        valid_order_cols = [c for c in order_cols if c in orders.columns]
        
        fact_sales = pd.merge(details, orders[valid_order_cols], on='OrderID')
        
        # Merge avec Dimensions
        if 'CustomerID' in fact_sales.columns:
            fact_sales = pd.merge(fact_sales, dim_customer[['CustomerID', 'CustomerKey']], on='CustomerID', how='left')
        
        if 'EmployeeID' in fact_sales.columns:
            fact_sales = pd.merge(fact_sales, dim_employee[['EmployeeID', 'EmployeeKey']], on='EmployeeID', how='left')
            
        if 'ProductID' in fact_sales.columns:
            fact_sales = pd.merge(fact_sales, dim_product[['ProductID', 'ProductKey']], on='ProductID', how='left')
        
        # Création DateKey
        if 'OrderDate' in fact_sales.columns:
            fact_sales['DateKey'] = fact_sales['OrderDate'].dt.strftime('%Y%m%d').fillna(0).astype(int)
        
        # Sélection colonnes finales (seulement celles qui ont pu être créées)
        target_cols = ['OrderID', 'DateKey', 'CustomerKey', 'EmployeeKey', 'ProductKey', 'Quantity', 'TotalAmount']
        final_cols = [c for c in target_cols if c in fact_sales.columns]
        
        fact_sales = fact_sales[final_cols]
        
        # 5. Sauvegarde
        dim_date.to_csv(os.path.join(self.processed_dir, 'DimDate.csv'), index=False)
        dim_customer.to_csv(os.path.join(self.processed_dir, 'DimClient.csv'), index=False)
        dim_employee.to_csv(os.path.join(self.processed_dir, 'DimEmployee.csv'), index=False)
        dim_product.to_csv(os.path.join(self.processed_dir, 'DimProduct.csv'), index=False)
        fact_sales.to_csv(os.path.join(self.processed_dir, 'FactSales.csv'), index=False)
        
        logger.info("✅ Transformation terminée. Fichiers disponibles dans /processed/")

if __name__ == "__main__":
    trans = DataTransformer()
    trans.transform_all()