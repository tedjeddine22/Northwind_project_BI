import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import logging
import os
import sqlalchemy
import sys
import warnings

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ignorer les avertissements Plotly
warnings.filterwarnings("ignore")

class OLAPVisualizer:
    def __init__(self):
        # 1. Gestion robuste des chemins
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        # Si on est dans 'scripts', on remonte d'un cran
        if self.base_dir.endswith('scripts'):
            self.base_dir = os.path.dirname(self.base_dir)
            
        self.db_path = os.path.join(self.base_dir, 'data', 'warehouse', 'northwind_dwh.db')
        self.figures_dir = os.path.join(self.base_dir, 'figures')
        self.raw_dir = os.path.join(self.base_dir, 'data', 'raw')
        
        # Création du dossier figures si inexistant
        os.makedirs(self.figures_dir, exist_ok=True)

    def get_engine(self):
        """Connexion à la base de données SQLite"""
        if not os.path.exists(self.db_path):
            logger.error(f"❌ Base de données introuvable : {self.db_path}")
            return None
        return sqlalchemy.create_engine(f'sqlite:///{self.db_path}')

    def detect_columns(self, engine):
        """Détecte les noms de tables et colonnes réels"""
        # Trouver la table Client (DimClient ou DimCustomer)
        client_table = 'DimClient'
        try:
            pd.read_sql("SELECT * FROM DimClient LIMIT 1", engine)
        except:
            client_table = 'DimCustomer'
            
        # Trouver la colonne Géo (Country, City, CompanyName...)
        try:
            cols = pd.read_sql(f"SELECT * FROM {client_table} LIMIT 0", engine).columns.tolist()
            geo_col = next((c for c in ['Country', 'Pays', 'City', 'Ville', 'CompanyName'] if c in cols), 'City')
        except:
            geo_col = 'City' # Fallback
            
        logger.info(f"🔍 Configuration détectée : Table='{client_table}', Colonne='{geo_col}'")
        return client_table, geo_col

    def load_data(self):
        """Charger et préparer les données depuis le DWH"""
        engine = self.get_engine()
        if not engine: return None

        client_table, geo_col = self.detect_columns(engine)

        logger.info("📂 Chargement des données OLAP...")
        
        query = f"""
        SELECT 
            f.TotalAmount, 
            f.Quantity, 
            f.OrderID,
            d.Year, 
            d.Month,
            c.{geo_col} as ClientInfo,
            e.FullName as EmployeeName
        FROM FactSales f
        LEFT JOIN DimDate d ON f.DateKey = d.DateKey
        LEFT JOIN {client_table} c ON f.CustomerKey = c.CustomerKey
        LEFT JOIN DimEmployee e ON f.EmployeeKey = e.EmployeeKey
        """
        
        try:
            df = pd.read_sql(query, engine)
            logger.info(f"✅ Données chargées : {len(df)} lignes")
            return df
        except Exception as e:
            logger.error(f"❌ Erreur SQL : {e}")
            return None

    def create_3d_scatter(self, df):
        """Créer un graphique 3D interactif"""
        logger.info("📊 Création du graphique 3D...")
        
        if df is None or df.empty:
            return

        # Agrégation pour le cube (limité au Top 200 pour la performance)
        cube = df.groupby(['Year', 'ClientInfo', 'EmployeeName']).agg({
            'TotalAmount': 'sum',
            'Quantity': 'sum'
        }).reset_index()
        
        # On garde les plus grosses ventes pour éviter de saturer le graph
        cube = cube.sort_values('TotalAmount', ascending=False).head(300)

        # Encodage numérique pour les axes (Plotly 3D préfère les nombres)
        cube['Client_ID'] = pd.factorize(cube['ClientInfo'])[0]
        cube['Emp_ID'] = pd.factorize(cube['EmployeeName'])[0]

        fig = go.Figure(data=[
            go.Scatter3d(
                x=cube['Year'],
                y=cube['Client_ID'], # On utilise l'ID pour le placement
                z=cube['Emp_ID'],    # On utilise l'ID pour le placement
                mode='markers',
                marker=dict(
                    size=cube['TotalAmount'] / cube['TotalAmount'].max() * 40 + 5,
                    color=cube['TotalAmount'],
                    colorscale='Viridis',
                    opacity=0.8,
                    showscale=True,
                    colorbar=dict(title="Ventes ($)")
                ),
                text=[
                    f"Année: {r.Year}<br>Client: {r.ClientInfo}<br>Employé: {r.EmployeeName}<br>Ventes: ${r.TotalAmount:,.0f}"
                    for r in cube.itertuples()
                ],
                hoverinfo='text'
            )
        ])

        # Astuce : On remplace les ticks numériques par les vrais noms
        fig.update_layout(
            title='Cube OLAP 3D (Année x Client x Employé)',
            scene=dict(
                xaxis_title='Année',
                yaxis=dict(title='Clients', tickvals=cube['Client_ID'].unique(), ticktext=cube['ClientInfo'].unique()),
                zaxis=dict(title='Employés', tickvals=cube['Emp_ID'].unique(), ticktext=cube['EmployeeName'].unique()),
            ),
            margin=dict(l=0, r=0, b=0, t=30),
            height=800
        )

        output_path = os.path.join(self.figures_dir, '3d_olap_scatter.html')
        fig.write_html(output_path)
        logger.info(f"💾 Graphique 3D sauvegardé : {output_path}")

    def create_kpi_dashboard(self, df):
        """Afficher les KPI"""
        if df is None: return

        total_sales = df['TotalAmount'].sum()
        total_qty = df['Quantity'].sum()
        avg_basket = total_sales / len(df)

        print("\n" + "="*40)
        print("📊 TABLEAU DE BORD KPI (DWH)")
        print("="*40)
        print(f"💰 CA Total       : ${total_sales:,.2f}")
        print(f"📦 Quantité Totale: {total_qty:,}")
        print(f"🛒 Panier Moyen   : ${avg_basket:,.2f}")
        print("="*40 + "\n")

    def create_delivery_chart(self):
        """Créer le graphique Livré vs Non Livré (Données Réelles)"""
        logger.info("🚚 Analyse des livraisons (Données RAW)...")
        
        # On cherche le fichier Orders.csv
        orders_path = os.path.join(self.raw_dir, 'Orders.csv')
        
        if not os.path.exists(orders_path):
            logger.warning("⚠️ Fichier Orders.csv introuvable. Graphique annulé.")
            return

        try:
            df_orders = pd.read_csv(orders_path)
            
            # Détection de la colonne date d'envoi
            date_col = next((c for c in ['ShippedDate', 'Shipped Date'] if c in df_orders.columns), None)
            
            if date_col:
                # Logique : Vide = Non Livré
                df_orders['Status'] = df_orders[date_col].apply(lambda x: 'Non Livrée' if pd.isnull(x) else 'Livrée')
                
                counts = df_orders['Status'].value_counts().reset_index()
                counts.columns = ['Status', 'Count']
                
                fig = px.pie(counts, values='Count', names='Status', 
                             title='Statut des Commandes (Réel)',
                             color='Status',
                             color_discrete_map={'Livrée': '#00CC96', 'Non Livrée': '#EF553B'})
                
                output_path = os.path.join(self.figures_dir, 'delivery_status.html')
                fig.write_html(output_path)
                logger.info(f"💾 Graphique Livraison sauvegardé : {output_path}")
            else:
                logger.warning("⚠️ Colonne ShippedDate introuvable dans Orders.csv")
                
        except Exception as e:
            logger.error(f"❌ Erreur lecture Orders.csv : {e}")

    def run(self):
        # 1. Charger
        df = self.load_data()
        
        # 2. Visualiser
        if df is not None:
            self.create_kpi_dashboard(df)
            self.create_3d_scatter(df)
            
        # 3. Graphique indépendant (Source Raw)
        self.create_delivery_chart()
        
        logger.info("🎉 Traitement terminé.")

if __name__ == "__main__":
    viz = OLAPVisualizer()
    viz.run()