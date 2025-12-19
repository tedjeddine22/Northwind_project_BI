import logging
import pyodbc
import urllib.parse
from sqlalchemy import create_engine
from config import SERVER_NAME, DATABASE_NAME, DRIVER, ACCESS_DB_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_sql_engine():
    """SQLAlchemy engine for SQL Server (Trusted Connection)"""
    try:
        odbc_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER_NAME};"
            f"DATABASE={DATABASE_NAME};"
            f"Trusted_Connection=yes;"
        )
        params = urllib.parse.quote_plus(odbc_str)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        return engine
    except Exception as e:
        logger.error(f"❌ Error creating SQLAlchemy Engine: {e}")
        return None

def get_sql_connection():
    """Raw pyodbc connection to SQL Server"""
    try:
        conn_str = (
            f"DRIVER={{{DRIVER}}};"
            f"SERVER={SERVER_NAME};"
            f"DATABASE={DATABASE_NAME};"
            f"Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"❌ Error connecting to SQL Server: {e}")
        return None

def get_access_connection():
    """Raw pyodbc connection for MS Access .accdb"""
    try:
        conn_str = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            f"DBQ={ACCESS_DB_PATH};"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"❌ Error connecting to Access: {e}")
        logger.error("💡 Hint: install 'Microsoft Access Database Engine' (match 32/64-bit with Python).")
        return None
