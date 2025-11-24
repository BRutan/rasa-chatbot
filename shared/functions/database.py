from objects.database.enums import SQLDialect
from objects.sqlalchemy_connector import SQLAlchemyConnector
import os
from typing import Any, Dict

def backend_conn_kwargs() -> Dict[str, Any]:
    return {"host": os.environ["PG_HOST"], 
            "username": os.environ["PG_USERNAME"],
            "password": os.environ["PG_PASSWORD"],
            "port": os.environ["PG_PORT"],
            "dbname": os.environ["PG_DBNAME"]}

def feature_store_conn_kwargs() -> Dict[str, Any]:
    return {"host": os.environ["FS_HOST"], 
            "username": os.environ["FS_USERNAME"],
            "password": os.environ["FS_PASSWORD"],
            "port": os.environ["FS_PORT"],
            "dbname": os.environ["FS_DBNAME"]}

def connect_to_backend() -> SQLAlchemyConnector:
    """
    * Generate a connection to the backend.
    """
    conn_kwargs = backend_conn_kwargs()
    return SQLAlchemyConnector(SQLDialect.POSTGRES, conn_kwargs)

def connect_to_feature_store() -> SQLAlchemyConnector:
    """
    * Generate a new connection to the feature store.
    """
    conn_kwargs = feature_store_conn_kwargs()
    return SQLAlchemyConnector(SQLDialect.POSTGRES, conn_kwargs)