"""
Script principal pour l'exécution complète de l'ETL
"""
import sys
import os
import logging

# Configuration des chemins
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import des modules
# On utilise try/except pour gérer les cas où les scripts s'appeleraient différemment
try:
    from scripts.extract_data import DataExtractor
    from scripts.transform_data import DataTransformer
    from scripts.load_dwh import DWLoader
except ImportError:
    # Fallback si on lance depuis le dossier racine
    from extract_data import DataExtractor
    from transform_data import DataTransformer
    from load_dwh import DWLoader

# Configuration du Logging
log_dir = os.path.join(parent_dir, 'data')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'etl_log.log'), mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_etl():
    """Exécution complète du pipeline ETL"""
    logger.info("=" * 60)
    logger.info("🚀 DÉMARRAGE DU PIPELINE ETL (NORTHWIND)")
    logger.info("=" * 60)
    
    try:
        # ---------------------------------------------------------
        # Étape 1: Extraction
        # ---------------------------------------------------------
        logger.info("\n📥 ÉTAPE 1: EXTRACTION (Source -> Raw)")
        extractor = DataExtractor()
        # On utilise extract_from_access car c'est la méthode qu'on a validée ensemble
        success_extract = extractor.extract_from_access()
        
        if not success_extract:
            logger.error("❌ Échec de l'extraction. Arrêt du pipeline.")
            return False
        
        # ---------------------------------------------------------
        # Étape 2: Transformation
        # ---------------------------------------------------------
        logger.info("\n⚙️ ÉTAPE 2: TRANSFORMATION (Raw -> Processed)")
        transformer = DataTransformer()
        transformer.transform_all()
        # transform_all ne retourne rien dans notre version, s'il ne plante pas, c'est bon.
        
        # ---------------------------------------------------------
        # Étape 3: Chargement
        # ---------------------------------------------------------
        logger.info("\n💾 ÉTAPE 3: CHARGEMENT (Processed -> Warehouse)")
        loader = DWLoader()
        # On appelle load_local() car nous avons configuré le mode sans serveur
        loader.load_local()
        
        logger.info("=" * 60)
        logger.info("✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 60)
        
        # Générer un rapport de synthèse
        generate_summary()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur critique dans le pipeline ETL: {e}", exc_info=True)
        return False

def generate_summary():
    """Génère un rapport de synthèse mis à jour"""
    summary = """
    ============================
    RAPPORT DE SYNTHÈSE ETL
    ============================
    
    📊 DONNÉES TRAITÉES:
    
    1. Extraction:
       - Source: Microsoft Access / Excel
       - Destination: CSV (Raw)
       - Statut: OK
    
    2. Transformation:
       - Nettoyage: Suppression espaces, formatage dates
       - Modélisation: Star Schema (Étoile)
       - Dimensions: DimDate, DimClient, DimEmployee, DimProduct
       - Faits: FactSales
    
    3. Chargement (Warehouse):
       - Type: Hybride (Fichiers CSV + Base SQLite locale)
       - Localisation: /data/warehouse/
       - Fichier DB: northwind_dwh.db
    
    🎯 PRÊT POUR ANALYSE:
    
    - Les données sont prêtes pour les Notebooks.
    - Ouvrez 'notebooks/dashboard_analysis.ipynb'.
    """
    
    print(summary)
    
    # Sauvegarder le rapport
    report_path = os.path.join(parent_dir, 'data', 'warehouse', 'etl_summary.txt')
    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(summary)
    except Exception as e:
        logger.warning(f"Impossible d'écrire le rapport texte : {e}")

if __name__ == "__main__":
    success = run_etl()
    if success:
        print("\n🎉 Pipeline ETL exécuté avec succès!")
    else:
        print("\n❌ Pipeline ETL échoué. Consultez data/etl_log.log")
        