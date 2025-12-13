import os
import pyodbc
from sqlalchemy import create_engine
import urllib.parse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnector:
    def __init__(self):
        # CONFIGURATION - A ADAPTER SELON VOTRE ORDINATEUR
        self.sql_server = 'MSSQLSERVER_TADJ'  # ou le nom de votre instance (ex: LAPTOP-XYZ\SQLEXPRESS)
        self.sql_database = 'Northwind' # Nom de la base de données SQL Server
        self.sql_driver = 'ODBC Driver 17 for SQL Server' # Vérifiez votre driver ODBC
        
        # Chemin vers le fichier Access dans le dossier data
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.access_path = os.path.join(base_dir, 'data', 'Northwind 2012.accdb') # Nom du fichier à vérifier
        
    def connect_sql_server(self):
        """Connexion brute via PyODBC"""
        try:
            conn_str = f'DRIVER={{{self.sql_driver}}};SERVER={self.sql_server};DATABASE={self.sql_database};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            return conn
        except Exception as e:
            logger.error(f"❌ Erreur SQL Server: {e}")
            return None
    
    def connect_access(self):
        """Connexion Access via PyODBC"""
        try:
            # Note: Nécessite le driver Microsoft Access Database Engine 2010 ou 2016 redistributable
            conn_str = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.access_path};'
            conn = pyodbc.connect(conn_str)
            return conn
        except Exception as e:
            logger.error(f"❌ Erreur Access (Vérifiez le chemin et le driver 32/64 bits): {e}")
            return None
    
    def get_sqlalchemy_engine(self):
        """Retourne un moteur SQLAlchemy pour SQL Server"""
        try:
            params = urllib.parse.quote_plus(f'DRIVER={{{self.sql_driver}}};SERVER={self.sql_server};DATABASE={self.sql_database};Trusted_Connection=yes;')
            engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
            return engine
        except Exception as e:
            logger.error(f"❌ Erreur Engine SQLAlchemy: {e}")
            return None
print("DatabaseConnector module loaded.")