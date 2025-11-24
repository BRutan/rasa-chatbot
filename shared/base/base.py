from abc import ABC, abstractmethod
from argparse import Namespace
import logging
from objects.database.enums import OutputMode, SQLDialect, WriteMode
from objects.functions.logger import get_logger
import re
from typing import Any, Dict, List, Union

class ConnectorBase(ABC):
    """
    * Base class for derived 
    connectors.
    """
    #__conn_kwarg_map = {"host": "url", "database": "dbname"}
    __conn_kwarg_map = {"database": "dbname", "username": "username"}
    def __init__(self, conn_kwargs, log=None, do_mapping:bool=False):
        """
        * Store members in base class.
        """
        self.__validate(conn_kwargs, log, do_mapping)
        self.__initialize(conn_kwargs, log, do_mapping)
        
    @classmethod
    def map_kwargs(cls, conn_kwargs):
        """
        * Map connection kwargs if using
        nonstandard keys.
        """
        mapper = ConnectorBase.__conn_kwarg_map
        conn_kwargs = {mapper[key] if key in mapper else key: conn_kwargs[key] for key in conn_kwargs}
        return conn_kwargs
    
    @abstractmethod
    def connect(self, dialect:Union[str, SQLDialect], conn_kwargs:Union[dict, Namespace]):
        """
        * Create connection to different database (only if connection type is different.) 
        """
        pass

    @abstractmethod
    def execute(self, query:str):
        """
        * Execute query in database, if supported for language.
        """
        pass
    
    @abstractmethod
    def read(self, table_or_query:str, query:bool=False, out_mode:OutputMode=OutputMode.ROWJSON):
        """
        * Read from server.
        """
        pass

    @abstractmethod
    def write(self, data:Any, table_name:str, mode:Union[str, WriteMode], upsert_cols:List[str]=None):
        """
        * Write data to table.
        """
        pass

    @abstractmethod
    def upsert(self, upsert_data:Any, table_name:str, upsert_cols:List[str], col_mapping:Dict[str, str]=None):
        """
        * Upsert data into the target
        table at current connection.
        """
        pass

    # Private Helpers:
    def __validate(self, conn_kwargs, log, do_mapping:bool=False):
        """
        * Validate constructor arguments.
        """
        errs = []
        if not isinstance(conn_kwargs, dict):
            errs.append("conn_kwargs must be a dictionary.")
        if log is not None and not isinstance(log, logging.Logger):
            errs.append("log must be a logging.Logger object if provided.")
        if not isinstance(do_mapping, bool):
            errs.append("do_mapping must be boolean.")
        if errs:
            raise ValueError("\n".join(errs))
        
    def __initialize(self, conn_kwargs, log, do_mapping:bool=False):
        """
        * Initialize the object.
        """
        self.conn_kwargs = conn_kwargs
        if "host" in self.conn_kwargs and self.conn_kwargs["host"]:
            self.conn_kwargs["host"] = self.conn_kwargs["host"].strip("/")
            self.conn_kwargs["host"] = re.sub("^https?://", "", self.conn_kwargs["host"])
        if do_mapping:
            self.conn_kwargs = self.map_kwargs(self.conn_kwargs)
        self.log = log if log is not None else get_logger()