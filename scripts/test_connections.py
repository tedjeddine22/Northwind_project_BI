"""Small utility to test DB connections locally"""
from database_connect import DatabaseConnector

if __name__ == '__main__':
    dbc = DatabaseConnector()
    res = dbc.test_connections()
    print(res)
