import pandas as pd
import os
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DWLoader:
    def __init__(self):
        # Définition des chemins relatifs
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.processed_dir = os.path.join(base_dir, 'data', 'processed')
        self.warehouse_dir = os.path.join(base_dir, 'data', 'warehouse')
        
        # On crée le dossier warehouse s'il n'existe pas
        os.makedirs(self.warehouse_dir, exist_ok=True)

    def load_local(self):
        """
        Charge les données uniquement sous forme de fichiers (CSV) 
        et crée une base locale SQLite (fichier .db) pour valider l'étape DWH.
        Ne nécessite AUCUN serveur SQL installé.
        """
        logger.info("📦 DÉBUT DU CHARGEMENT (Mode Local / Sans Serveur)...")

        # Liste des fichiers à déplacer
        files_mapping = {
            'DimDate.csv': 'DimDate',
            'DimClient.csv': 'DimClient',
            'DimEmployee.csv': 'DimEmployee',
            'DimProduct.csv': 'DimProduct',
            'FactSales.csv': 'FactSales'
        }

        # On prépare un moteur SQLite local (c'est juste un fichier, pas de serveur à installer)
        # Cela permet d'avoir une "base de données" pour le projet sans les soucis de connexion.
        db_path = os.path.join(self.warehouse_dir, 'northwind_dwh.db')
        engine = create_engine(f'sqlite:///{db_path}')

        success_count = 0

        for filename, table_name in files_mapping.items():
            source_path = os.path.join(self.processed_dir, filename)
            dest_csv_path = os.path.join(self.warehouse_dir, filename)
            
            if os.path.exists(source_path):
                try:
                    # 1. Lire le fichier traité
                    df = pd.read_csv(source_path)
                    
                    # 2. Sauvegarder en CSV dans le warehouse (Fichiers finaux)
                    df.to_csv(dest_csv_path, index=False)
                    logger.info(f"  📄 CSV copié : {filename}")
                    
                    # 3. Sauvegarder dans la base de données locale (SQLite)
                    # Cela valide la case "Data Warehouse" du projet sans serveur complexe
                    df.to_sql(table_name, engine, if_exists='replace', index=False)
                    logger.info(f"  💾 Table SQL locale créée : {table_name}")
                    
                    success_count += 1
                except Exception as e:
                    logger.error(f"  ❌ Erreur sur {filename}: {e}")
            else:
                logger.warning(f"  ⚠️ Fichier source introuvable : {filename}")

        if success_count == len(files_mapping):
            logger.info("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS")
            logger.info(f"📁 Vos données sont dans : {self.warehouse_dir}")
            logger.info(f"🗄️ Base de données locale créée : {db_path}")
        else:
            logger.warning("⚠️ Chargement terminé avec des manquements.")

if __name__ == "__main__":
    loader = DWLoader()
    loader.load_local()