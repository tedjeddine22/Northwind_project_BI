import pandas as pd
import os
from database_connect import DatabaseConnector
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self):
        self.db = DatabaseConnector()
        # Chemin absolu pour éviter les erreurs de dossier
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.raw_dir = os.path.join(base_dir, 'data', 'raw')
        os.makedirs(self.raw_dir, exist_ok=True)
    
    def extract_from_access(self):
        """Extraction depuis Access (ou Excel si Access échoue)"""
        logger.info("📥 Tentative d'extraction...")
        
        # Liste des tables à extraire
        tables = ['Orders', 'Customers', 'Employees', 'Order Details', 'Products', 'Categories']
        
        conn = self.db.connect_access()
        if conn:
            try:
                for table in tables:
                    # Gestion des espaces dans les noms de table Access
                    query_table = f"[{table}]"
                    df = pd.read_sql(f'SELECT * FROM {query_table}', conn)
                    # Sauvegarde
                    safe_name = table.replace(" ", "_")
                    df.to_csv(os.path.join(self.raw_dir, f'{safe_name}.csv'), index=False, encoding='utf-8')
                    logger.info(f"✅ {table} extrait : {len(df)} lignes")
                conn.close()
                return True
            except Exception as e:
                logger.error(f"❌ Erreur lors de l'extraction Access: {e}")
                return False
        else:
            logger.warning("⚠️ Impossible de se connecter à Access. Assurez-vous que le fichier est dans /data/")
            return False

if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.extract_from_access()