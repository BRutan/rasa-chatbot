from copy import deepcopy
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from enum import Enum
import functools
from io import StringIO
import itertools
import json
import logging
from objects.base.base import ConnectorBase
from objects.database.enums import ObjectType, OutputMode, SQLDialect, WriteMode
import psycopg2
import random
import re
import sqlalchemy as sq
import sqlalchemy.dialects.postgresql as psql
from sqlalchemy.engine import URL
import sqlalchemy.schema as sqm
import sqlalchemy.sql as sql
import sqlalchemy.sql.expression as sqe
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
import tempfile
from tqdm import tqdm
from typing import Any, Dict, List, Tuple, Union
import uuid

def convert_sets_to_lists(val):
    if isinstance(val, dict):
        for k in val:
            val[k] = convert_sets_to_lists(val[k])
    elif isinstance(val, set):
        return list(val)
    return val

# Map conn_str -> engine
ENGINE_CACHE = {}
# Map conn_str -> connection
CONN_CACHE = {}

def _conv_to_bool(val):
    if isinstance(val, bool):
        return val
    return True if val.lower() == "true" else False

def _process_result_value(value):
    if value is None:
        return value
    else:
        return str(value)
    
_COERCE_METHODS = {sql.sqltypes.JSON: lambda val : json.dumps(val) if isinstance(val, str) else val, 
                   sql.sqltypes.BOOLEAN: _conv_to_bool,
                   psql.base.BYTEA: lambda val : val.encode("utf-8") if isinstance(val, str) else val,
                   psql.UUID: _process_result_value,
                   uuid.UUID: _process_result_value}

class SQLAlchemyConnector(ConnectorBase):
    """
    * General connector using sqlalchemy.
    """
    __type_cat_cache = {} # Map SQLDialect -> type_name -> type_category
    __conn_strs = {SQLDialect.POSTGRES: "postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}",
                   SQLDialect.REDSHIFT: "redshift+psycopg2://{username}@{host}:{port}/{dbname}",
                   SQLDialect.TSQL: ";SERVER={host};DATABASE={dbname};UID={username};PWD={password};TrustServerCertificate=yes;",
                   SQLDialect.NEO4J: "neo4j+jdbc://{host}:{port}/neo4j?UID={username}&PWD={password}&LogLevel=6&StrictlyUseBoltScheme=false"}
    __built_in_schemas = {SQLDialect.POSTGRES: ["public", "information_schema", "pg_catalog", "pg_toast"],
                          SQLDialect.TSQL: ["sys","information_schema","guest","db_owner","db_accessadmin","db_securityadmin","db_ddladmin","db_backupoperator","db_datareader","db_datawriter","db_denydatareader","db_denydatawriter","sqlagentuserrole","sqlagentreaderrole","sqlagentoperatorrole","databasemailuserrole","targetserversrole","db_ssisadmin","db_ssisltduser","db_ssisoperator","managed_backup","smart_admin"]}
    def __init__(self, 
                 dialect:SQLDialect, 
                 conn_kwargs:dict, 
                 include_schemas:list=None, 
                 log:logging.Logger=None,
                 **extra_conn_kwargs):
        """
        * Connect to database instance using
        sqlalchemy.
        """
        super().__init__(conn_kwargs, log)
        self.__validate(dialect, include_schemas)
        self.__initialize(dialect, include_schemas, **extra_conn_kwargs)
        
    # Properties:
    @property
    def sql_metadata(self):
        return self.metadata
    @property
    def built_in_schemas_by_dialect(self):
        return deepcopy(SQLAlchemyConnector.__built_in_schemas[self.dialect])
    @classmethod
    def conn_string(self, dialect:Union[str, SQLDialect]):
        if dialect not in SQLAlchemyConnector.__conn_strs:
            raise ValueError(f"Dialect {dialect} is not configured.")
        return SQLAlchemyConnector.__conn_strs[dialect]
    @property
    def primary_foreign_key_relationships(self):
        return self.relationships
    
    def assign_columns(self, data:List[Any], target_table:str):
        """
        * Assign columns to the data based on the
        target_table columns.
        """
        self.change_reflection_if_necessary(target_table)
        formatted = self.format_table_for_metadata(target_table)
        if formatted not in self.metadata_tables:
            raise ValueError(f"Target table {target_table} not currently defined at connection.")
        schema, tbl = formatted.split(".")
        columns = self.inspector.get_columns(tbl, schema)
        # Filter out built in autoincrement columns and default columns:
        columns = [elem for elem in columns if elem["name"] not in ["timestamp", "row_id", "id"]]
        df = {elem["name"]: [] for elem in columns}
        idx_to_col = {idx: elem["name"] for idx, elem in enumerate(columns)}
        for row in data:
            for idx, val in enumerate(row):
                col = idx_to_col[idx]
                df[col].append(val)
        return df
    
    def change_reflection_if_necessary(self, table_name:str, incl_views:bool=False):
        """
        * Change metadata reflection only if a new schema 
        needs to be analyzed.
        """
        schema = SQLAlchemyConnector.get_object_schema(self, obj_name=table_name)
        if 1 == 1: #schema != self.last_schema or incl_views != self.incl_views:
            self.metadata.reflect(bind=self.engine, schema=schema, views=incl_views)
            self.last_schema = schema
            self.incl_views = incl_views
            # Normalize the tables:
            self.metadata_tables = {t.lower(): tbl for t, tbl in self.metadata.tables.items()}
        
    def check_convert_enum(self, enum_obj:Union[str, Enum], target_enum:Enum, errs:list=None):
        """
        * Check validity of passed enum object.
        If is a string then attempt to convert.
        """
        if enum_obj is None:
            return enum_obj
        errs_ = []
        if not isinstance(enum_obj, (str, Enum)):
            errs_.append("enum_obj must be a string or an Enum.")
        elif isinstance(enum_obj, str):
            if enum_obj.upper() not in target_enum.__members__:
                errs_.append(f"enum_obj string is not a valid {target_enum.__name__}.")
            else:
                enum_obj = target_enum.__members__[enum_obj.upper()]
        if errs_:
            if errs is None:
                raise ValueError("\n".join(errs_))
            errs.extend(errs_)
        return enum_obj
    
    def create_temp_table(self, source_query:str, tmp_table_name:str=None, session:Session=None) -> str:
        """
        * Create temporary table name.
        Make sure that it is in a temporary schema.
        """
        # Do not use schema in temp table name:
        if not tmp_table_name:
            tmp_table_name = self.get_random_temp_table_name(session=session, must_not_exist=True)
        if "." in tmp_table_name:
            _, tmp_table_name = tmp_table_name.split(".")
        query = f"""
        create temporary table {tmp_table_name} as
        {source_query}
        """
        self.execute(query, session=session)
        # If schema not provided then get the full 
        # table name:
        if "." not in tmp_table_name:
            tmp_table_name = self.get_full_temp_table_name(tmp_table_name, session)
        return tmp_table_name

    def create_staging_table(self, table_name:str, data:Union[List[Any], Dict[str, Any]]=None, session:Session=None):
        """
        * Create temporary staging table based on
        same schema as target table.
        """
        staging_table_name = table_name + "_staging"
        self.execute(f"drop table if exists {staging_table_name}", session=session)
        query = f"""
        CREATE TABLE {staging_table_name}
        AS 
        SELECT * FROM {table_name}
        WITH NO DATA
        """
        query = f"""
        BEGIN;
        LOCK TABLE {table_name}; 
        CREATE TABLE {staging_table_name} (LIKE {table_name} INCLUDING ALL);
        COMMIT;
        """
        self.execute(query, None, session=session)
        if data is not None:
            self.write(data, staging_table_name, mode=WriteMode.OVERWRITE)
        return staging_table_name
    
    def ddl_if_not_exists(self, ddl:str):
        """
        * Swap CREATE with CREATE IF NOT EXISTS
        for ddl.
        """
        patt = re.compile(r"CREATE\s+([^\s]+)\s+([^\s]+)\s+", flags=re.IGNORECASE|re.MULTILINE|re.DOTALL)
        return patt.sub("CREATE \g<1> IF NOT EXISTS \g<2> ", ddl)

    def get_full_temp_table_name(self, tmp_table_name:str, session:Session=None) -> str:
        """
        * Get the full temporary table name.
        """
        query = f"""
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname LIKE 'pg_temp%'
        and lower(tablename) = '{tmp_table_name.lower()}';
        """
        records = self.read(query, query=True, out_mode=OutputMode.ROWJSON, session=session)
        return [r["schemaname"] + "." + r["tablename"] for r in records][0] if records else None

    def get_join_key(self, l_table_name:str, r_table_name:str, l_alias:str, r_alias:str) -> str:
        """
        * Return join key statement.
        """
        if not self.tables_have_relationship(l_table_name, r_table_name):
            raise RuntimeError(f"{l_table_name} and {r_table_name} do not have a relationship.")
        l_alias += "."
        r_alias += "."
        rel_columns = self.get_foreign_keys(l_table_name, r_table_name)
        pkey_cols = list(rel_columns.keys())[0]
        left = ",".join([l_alias + c for c in pkey_cols])
        right = ",".join([r_alias + rel_columns[pkey_cols][0][idx] for idx in range(len(pkey_cols))])
        return f"({left}) = ({right})"
    
    def object_exists(self, obj_name:str, obj_type:ObjectType, session:Session=None) -> bool:
        """
        * Indicate that the object exists
        in the current connection.
        """
        if obj_type == ObjectType.TABLE:
            return self.table_exists(obj_name, session)
        elif obj_type == ObjectType.VIEW:
            return self.view_exists(obj_name, session)
        elif obj_type == ObjectType.SCHEMA:
            return self.schema_exists(obj_name, session)
        elif obj_type == ObjectType.DATABASE:
            return obj_name in self.get_all_databases()

    def schema_exists(self, schema_name:str, session:Session=None) -> bool:
        """
        * Indicate that the schema exists.
        """
        return schema_name in self.inspector.get_schema_names()
    
    def get_join(self, table_columns:Dict[str, List[str]], tables:List[str]):
        """
        * Output a sql query that
        joins all table names together using
        their primary-foreign key relationships.
        """
        tbl_objs = {}
        for table_name in tables:
            tbl_objs[table_name] = self.get_table_obj(table_name)
        # Get SELECT columns:
        obj_cols = []
        for table_name in table_columns:
            tbl = tbl_objs[table_name]
            obj_cols.extend([getattr(tbl.c, c) for c in table_columns[table_name]])
        stmt = sq.select(obj_cols)
        for child_name in tables:
            c_obj = tbl_objs[child_name]
            for fk in c_obj.foreign_keys:
                parent_name = ".".join(fk.target_fullname.split(".")[0:2])
                if parent_name not in tbl_objs:
                    continue
                # Join based on primary - foreign key relationship:
                p_obj = tbl_objs[parent_name]
                fk_col, fk_parent_col = (fk.column.name, fk.parent.name)
                stmt = stmt.select_from(p_obj).join(c_obj, getattr(c_obj.c, fk_col) == getattr(p_obj.c, fk_parent_col))
        return stmt.sql()
    
    def remove_type_quantifiers(self, type_name:str) -> str:
        """
        * Remove type quantifiers from type string
        (ex: varchar(1) or decimal(10, 30) or text[30]).
        """
        # Remove quantifiers from array but maintain array aspect:
        if re.match(r"^[^\[]+\[[^\]]*\]$", type_name):
            return re.sub(r"\[[^\]]*\]", "[]", type_name)
        # Remove non array quantifiers fully:
        return re.sub(r"[\(][^\)]+[\)]", "", type_name)
    
    def drop_duplicates(self, table:str, columns:list=None):
        """
        * Drop duplicates from table. Use optional
        columns to determine duplicates.
        """
        if columns is None:
            columns = self.get_table_columns(table, False)
            columns = [c["name"] for c in columns if c["type"].compile() != "JSON"]
        conds = [f"t.{c} = tt.{c}" for c in columns if c not in ["id", "timestamp"]]
        conds.append("t.id < tt.id")
        where = " AND ".join(conds)
        query = f"""
        DELETE FROM {table} t
        USING {table} tt
        WHERE {where}
        """
        self.execute(query)
        # Reset the identity column:
        self.reset_identity(table)

    def get_record_count_fast_approx(self, table:str):
        """
        * Get the record count in efficient manner.
        """
        query = f"""
        SELECT (reltuples / relpages * (pg_relation_size(oid) / 8192))::bigint AS ct
        FROM pg_class
        WHERE oid = '{table}'::regclass;
        """
        return self.read(query, query=True, out_mode=OutputMode.ROWJSON)
    
    def reset_identity(self, table:str):
        """
        * Reset identity if present in table.
        """
        if "id" in self.get_table_columns(table, True):
            query = f"ALTER TABLE {table} ALTER COLUMN id RESTART WITH 1;"
            self.execute(query)
            
    def get_column_schema(self, 
                          table:str, 
                          names_only:bool=False,
                          names_types_only:bool=False):
        """
        * Get column schema with 
        metadata for table.
        """
        schema, table_name = table.lower().split(".")
        records = self.inspector.get_columns(table_name, schema)
        if not names_only and not names_types_only:
            return {r["name"].lower(): {k:v for k,v in r.items() if k != "name"} for r in records} 
        elif names_types_only:
            out = {}
            for r in records:
                out[r["name"].lower()] = str(r["type"]).lower()
            return out
        elif names_only:
            return [r["name"].lower() for r in records]
    
    def connect(self, 
                dialect:Union[str, SQLDialect], 
                conn_kwargs:Dict[str, Any], 
                include_schemas:list=None,
                **extra_conn_kwargs):
        """
        * Connect to the database instance using
        sqlalchemy.
        """
        self.__validate(dialect)
        self.dialect = dialect if isinstance(dialect, SQLDialect) else SQLDialect.__members__[dialect.upper()]
        self.conn_kwargs = deepcopy(conn_kwargs)
        self.conn_kwargs = self.map_kwargs(self.conn_kwargs)
        self.metadata_tables = {}
        conn_template = SQLAlchemyConnector.__conn_strs[self.dialect]
        conn_str = conn_template.format(**self.conn_kwargs)
        self.include_schemas = [] if include_schemas is None else include_schemas
        if conn_str not in ENGINE_CACHE:
            self.log.debug("Making new engine for dialect %s.", dialect)
            connect_args = None
            if self.dialect == SQLDialect.POSTGRES:
                connect_args = {"options": "-csearch_path=public,pg_catalog"}
                if extra_conn_kwargs.get("sslmode") is not None:
                    conn_str += "?sslmode={sslmode}".format(**extra_conn_kwargs)
                self.engine = sq.create_engine(conn_str, connect_args=connect_args, pool_size=20, max_overflow=0)
            elif self.dialect == SQLDialect.TSQL:
                conn_str = "DRIVER={ODBC Driver 18 for SQL Server}" + conn_str
                conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
                self.engine = sq.create_engine(conn_url, pool_size=20, max_overflow=0)
            ENGINE_CACHE[conn_str] = self.engine
        else:
            self.log.debug("Reusing engine previously created.")
            self.engine = ENGINE_CACHE[conn_str]
        if conn_str not in CONN_CACHE:
            self.log.debug("Making new connection for host %s and database %s", self.conn_kwargs["host"], self.conn_kwargs["dbname"])
            self.conn = self.engine.connect()
            CONN_CACHE[conn_str] = self.conn
        else:
            self.log.debug("Reusing connection previously created.")
            self.conn = CONN_CACHE[conn_str]
        self.metadata = sq.MetaData()
        self.inspector = sq.inspect(self.engine)
        self.built_in_schemas = SQLAlchemyConnector.__built_in_schemas.get(self.dialect, [])
        self.built_in_schemas = [s for s in self.built_in_schemas if s not in self.include_schemas]
        # Reset all object mappings and caches:
        self.__sqlalchemy_schema_to_tables = {}
        self.objs_by_tp = {}
        for tp in ObjectType.__members__:
            tp_enum = ObjectType.__members__[tp]
            self.objs_by_tp[tp_enum] = {}
        # Store { object_name -> column_name } for all primary keys listed in instance:
        self.__primary_keys = {}
        # Map all tables to dependent tables (has foreign key relationship):
        self.relationships = {}
        self.last_schema = None
        self.incl_views = None
        
    def get_write_order(self):
        """
        * Return the write order that
        must be performed based on
        primary-foreign key relationships.
        """
        builtin_schemas = self.built_in_schemas
        builtin_schemas = [s for s in builtin_schemas if s not in self.include_schemas]
        builtin_schemas = "','".join(builtin_schemas)
        query = f"""
                with recursive fk_tree as (
        -- All tables not referencing anything else
        select t.oid as reloid, 
                t.relname as table_name, 
                s.nspname as schema_name,
                null::text COLLATE "default" as referenced_table_name,
                null::text COLLATE "default" as referenced_schema_name,
                1 as level
        from pg_class t
            join pg_namespace s on s.oid = t.relnamespace
        where relkind = 'r'
            and not exists (select *
                            from pg_constraint
                            where contype = 'f'
                            and conrelid = t.oid)
            and s.nspname not in ('{builtin_schemas}')
        union all 
        select ref.oid, 
                ref.relname, 
                rs.nspname,
                p.table_name,
                p.schema_name,
                p.level + 1
        from pg_class ref
            join pg_namespace rs on rs.oid = ref.relnamespace
            join pg_constraint c on c.contype = 'f' and c.conrelid = ref.oid
            join fk_tree p on p.reloid = c.confrelid
        where ref.oid != p.reloid  -- do not enter to tables referencing theirselves.
        ), all_tables as (
        -- this picks the highest level for each table
        select schema_name, table_name,
                level, 
                row_number() over (partition by schema_name, table_name order by level desc) as last_table_row
        from fk_tree
        )
        select schema_name, table_name, level
        from all_tables at
        where last_table_row = 1
        order by level;
        """
        ordered_tables = self.execute(query, out_mode=OutputMode.ROWJSON)
        ordered_tables = [r["schema_name"] + "." + r["table_name"] for r in ordered_tables]
        return ordered_tables
    
    def get_indirectly_linked_tables(self, parent_table:str, column:str) -> Dict[str, List[str]]:
        """
        * Get all tables that are indirectly linked
        to column in parent_table, i.e. through a     
        """
        parent_relationships = self.get_foreign_key_parents(parent_table, fkey=column)
        if not parent_relationships:
            return None
        out = {}
        for ancestor_name, relationships in parent_relationships.items():
            pass
        
        return out

    def get_foreign_key_parents(self, child_table:str, parent_table:str=None, fkey:Union[Tuple[Any], str]=None) -> Dict[str, List[str]]:
        """
        * Get table where 
        """
        self.setup_fkey_relationships(skip_builtins=True, overwrite=False)
        fkey = (fkey,) if fkey is not None and not isinstance(fkey, tuple) else fkey
        if parent_table is not None and parent_table not in self.relationships:
            return None
        elif (parent_table is not None and 
              parent_table in self.relationships 
              and child_table not in self.relationships[parent_table]):
            return None
        elif parent_table is None:
            out = {}
            for parent_table, relationships in self.relationships.items():
                if child_table not in relationships:
                    continue
                for pkey, fkeys in relationships[child_table].items():
                    if fkey and fkey not in fkeys:
                        continue
                    if parent_table not in out:
                        out[parent_table] = {}
                    if fkey:
                        out[parent_table][fkey] = pkey
                    elif not fkey:
                        for fk in fkeys:
                            if fk not in out[parent_table]:
                                out[parent_table][fk] = []
                            out[parent_table][fk].append(pkey)
        elif child_table in self.relationships[parent_table]:
            out = {}
            relationships = self.relationships[parent_table][child_table]
            for pkey, fkeys in relationships.items():
                if fkey and fkey not in fkeys:
                    break
                if fkey:
                    out[parent_table][fkey] = pkey
                elif not fkey:
                    for fk in fkeys:
                        if fk not in out[parent_table]:
                            out[parent_table][fk] = []
                        out[parent_table][fk].append(pkey)
        return out

    def get_foreign_keys(self, parent_table:str, child_table:str=None, pkey:Union[str, Tuple[Any]]=None) -> Dict[str, List[str]]:
        """
        * Return ([pkey...], foreign_key) if primary - foreign key relationships
        exists between two tables.
        """
        # Check input types:
        errs = []
        if not isinstance(parent_table, str):
            errs.append(f"parent_table must be a string. Is {type(parent_table).__name__}.")
        if child_table is not None and not isinstance(child_table, str):
            errs.append(f"child_table must be a string if passed. Is {type(child_table).__name__}.")
        if pkey is not None and not isinstance(pkey, (tuple, str)):
            errs.append(f"pkey must be a string or tuple of strings if passed. Is {type(pkey).__name__}.")
        elif pkey is not None and isinstance(pkey, str):
            pkey = (pkey,)
        if errs:
            raise ValueError("\n".join(errs))
        self.setup_fkey_relationships(skip_builtins=True, overwrite=False)
        if parent_table not in self.relationships: # Skip if parent table does not have any foreign key relationships
            return None
        elif child_table is None:
            rels = self.relationships[parent_table]
            rels = {ct: {pk: fks for pk, fks in rels[ct].items() if pk == pkey or pkey is None}
                    for ct in rels}
            return rels
        # Retrieve all foreign keys ignoring the passed primary key of the parent table.
        elif child_table in self.relationships[parent_table] and not pkey:
            return self.relationships[parent_table][child_table]
        elif child_table in self.relationships[parent_table] and pkey:
            return {pk: fks for pk, fks in self.relationships[parent_table][child_table].items()
                    if pk == pkey}
        return None

    def map_schemas_to_tables(self, recalc=False):
        """
        * Map sqlalchemy table column schemas to tables
        that can accept those schemas.
        """
        if not self.__sqlalchemy_schema_to_tables or recalc:
            tables = self.get_objects_of_type(ObjectType.TABLE)
            self.get_objects_of_type(ObjectType.VIEW)
            for tbl_name in tables:
                table = tables[tbl_name]
                tps = sorted([col.type for col in table.columns], key=lambda x : str(x))
                tps = tuple(tps)
                if tps not in self.__sqlalchemy_schema_to_tables:
                    self.__sqlalchemy_schema_to_tables[tps] = []
                self.__sqlalchemy_schema_to_tables[tps].append(table)
        return self.__sqlalchemy_schema_to_tables

    def get_object_type(self, obj:str):
        """
        * Determine the object type.
        """
        elems = obj.split(".")
        if len(elems) == 2:
            full_name = ".".join(elems)
            for tp in [ObjectType.TABLE, ObjectType.VIEW]:
                if full_name in self.objs_by_tp[tp]:
                    return tp
            return None
        return None
        
    def get_table_obj(self, table_name:str, repeat=True):
        """
        * Return sqlalchemy table object.
        """
        try:
            tbl = sq.Table(table_name, self.metadata, autoload_with=self.engine)
            return tbl
        except Exception as ex:
            if not repeat:
                raise ex
            self.change_reflection_if_necessary(table_name)
            return self.get_table_obj(table_name, repeat=False)
    
    def get_object_definition(self, obj:Union[str, sq.Table], obj_type:ObjectType):
        """
        * Get the DDL for the object.
        """
        if obj_type == ObjectType.TABLE:
            return sqm.CreateTable(obj).compile(self.engine).string
        elif obj_type == ObjectType.VIEW:
            return self.__get_view_def(obj.name if not isinstance(obj, str) else obj)
        elif obj_type == ObjectType.FUNCTION:
            return self.__get_function_def(obj)
        elif obj_type == ObjectType.PROCEDURE:
            return self.__get_procedure_def(obj)
        else:
            raise ValueError(f"Cannot get definition of {obj_type.name.upper()}")
        
    def get_table_columns(self, table_name:str, cols_only:bool=False, incl_identity:bool=False):
        """
        * Get the columns used in the table.
        """
        elems = table_name.split(".")
        if len(elems) != 2:
            elems.insert(0, None)
        schema, table = elems
        col_info = self.inspector.get_columns(table, schema)
        # Skip identity columns that are generated as always
        # if requested by passing incl_identity = True.
        if not incl_identity:
            col_info = [c for c in col_info if not ("identity" in c and c["identity"]["always"])]
        # Only output column names if requested.
        if cols_only:
            columns = [elem["name"] for elem in col_info]
        else:
            columns = col_info
        return columns
    
    def drop_table(self, table_name:str):
        """
        * Drop the table.
        """
        self.execute(f"drop table {table_name}")
        
    def get_cursor_for_chunks(self, query:str, chunk_size:int, out_mode:OutputMode.ROWJSON):
        with self.engine.raw_connection().cursor() as cur:
            cur.itersize = chunk_size
            cur.execute(query)
            for row in cur:
                # Properly quoted for SQL insertion
                if row == (None,):
                    break
                values = ", ".join(psycopg2.extensions.adapt(v).getquoted().decode('utf-8') for v in row)
                yield f"({values})"
        
    def execute(self, query:str, out_mode:OutputMode=None, session:Session=None, rollback_stmt:bool=False):
        """
        * Execute query in database, if supported for language.
        """
        is_sub = not session is None
        session = Session(self.engine) if not is_sub else session
        # Handle bind parameters being incorrectly interpreted within literals
        # by escaping them:
        query = self.__escape_bind_params(query)
        query_obj = sq.text(query)
        try:
            results = session.execute(query_obj)
            if results is None or not results.returns_rows:
                if not is_sub and not rollback_stmt:
                    session.commit()
                    session.close()
                elif not is_sub and rollback_stmt:
                    session.rollback()
                return None
        except Exception as ex:
            session.close()
            raise ex
        # If returns records then retrieve and output:
        data = results.fetchall()
        if not is_sub and not rollback_stmt:
            session.close()
        elif not is_sub and rollback_stmt:
            session.rollback()
        # Output the records:
        if out_mode == OutputMode.ROWJSON:
            out_data = []
            columns = list(results.keys())
            for record in data:
                out_data.append({c: record[idx] for idx, c in enumerate(columns)})
            return out_data
        elif out_mode == OutputMode.COLUMNAR:
            columns = results.keys()
            out_data = {c: [] for c in columns}
            for record in data:
                for idx, c in enumerate(out_data):
                    out_data[c].append(record[idx])
            return out_data
        else:    
            return data
        
    def record_exists(self, record:dict, table:str, unique_cols:list=None) -> bool:
        """
        * Return True if the record is present.
        Performs search on the unique or primary key.
        """
        errs = []
        if not isinstance(record, dict):
            errs.append("record must be a dictionary.")
        if not isinstance(table, str):
            errs.append("table must be a string.")
        if errs:
            raise ValueError("\n".join(errs))
        schema = self.get_column_schema(table=table, names_types_only=True)
        # If a unique constraint is used in the table then
        # check just the presence of the non constraint columns:
        if unique_cols is None:
            unique_cols = self.get_unique_constraint_columns(table, ignore_identity=True)
        # Check all columns if no constraint columns present:
        if not unique_cols:
            values = {c: self.wrap_literal(v, schema[c]) for c, v in record.items()}
        # Use unique constraint columns as lookup:
        else:
            values = {c: self.wrap_literal(v, schema[c]) 
                      for c, v in record.items() 
                      if c in unique_cols}
        where_clause = " AND ".join([f"{c} = {v}" 
                                        for c, v in values.items()])
        query = f"""
        select 1
        from {table}
        where {where_clause}
        limit 1;
        """
        records = self.read(query, query=True, out_mode=OutputMode.ROWJSON)
        return True if records else False

    def read(self, table_or_query:str, query=False, out_mode:OutputMode=None, session:Session=None):
        """
        * Read from database.
        """
        table_or_query = re.sub("\n", "", table_or_query).strip()
        if not query and not table_or_query.lower().startswith("select"):
            table_or_query = f"SELECT * FROM {table_or_query}"
        self.log.debug("Executing query: %s", table_or_query)
        # Remove comments prior to running since 
        # causes a sqlalchemy error:
        #table_or_query = sqlparse.format(table_or_query, strip_comments=True)
        return self.execute(table_or_query, out_mode=out_mode, session=session)

    def write(self, 
              data:Union[Dict[str, List[Any]], List[Tuple]], 
              table_name:str, 
              mode:Union[str, WriteMode], 
              session=None, 
              do_explicit_cast:bool=False,
              table_schema:Dict[str, str]=None,
              upsert_cols:List[str]=None,
              do_commit:bool=False):
        """
        * Write data to table.
        """
        errs = []
        if not isinstance(data, (list, dict)):
            errs.append("data must be a list or dictionary.")
        if not isinstance(table_name, str):
            errs.append("table_name must be a string.")
        elif not self.table_exists(table_name):
            errs.append("table_name does not exist at current connection.")
        if isinstance(mode, str):
            if mode.upper() in WriteMode.__members__:
                mode = WriteMode.__members__[mode.upper()]
            else:
                raise ValueError("mode string not a valid WriteMode.")
        elif not isinstance(mode, WriteMode):
            errs.append("mode must be a WriteMode object.")
        if errs:
            raise ValueError("\n".join(errs))
        is_sub = True if session is not None else False
        if len(data) == 0:
            self.log.info("No data provided. Exiting.")
            return
        elif mode == WriteMode.UPSERT:
            self.upsert(data, table_name, upsert_cols, session, do_commit)
            return
        session = Session(self.engine) if session is None else session
        self.change_reflection_if_necessary(table_name)
        formatted = self.format_table_for_metadata(table_name)
        target_tbl = self.metadata_tables[formatted]
        # Exclude id columns that are always
        # generated since cannot insert non default
        # values into them.
        if not table_schema:
            columns = {col.name:col.type 
                    for col in target_tbl.columns
                    if not isinstance(col.server_default, sq.Identity) 
                    or (isinstance(col.server_default, sq.Identity) and not col.server_default.always)}
        else:
            columns = table_schema
        if mode == WriteMode.OVERWRITE:
            # Delete all records before inserting:
            self.log.debug("Performing overwrite. Deleting all records from %s prior to insert.", table_name)
            session.execute(sqe.text(f"DELETE FROM {table_name} WHERE 1 = 1"))
            # Reset identity columns if present:
            if "id" in columns:
                query = f"SELECT setval(pg_get_serial_sequence('{target_tbl}', 'id'), 1) from {target_tbl}"
                session.execute(sqe.text(query))
        stmt = psql.insert(target_tbl)
        # Jsonify the data if necessary (convert each row to json object with {'column_name' -> 'value'}):
        if isinstance(data, list) and not isinstance(data[0], dict):
            data = self.assign_columns(data, table_name)
            cols = [col for col, _ in columns.items() if col in data]
            num_records = len(data[cols[0]])
            data = [{col:data[col][row] for col in data} for row in range(num_records)]
        elif isinstance(data, list) and isinstance(data[0], dict):        
            # Only keep columns that will fit in the table:
            data = [{col:record[col] for col in record if col in columns} for record in data]
        # Coerce the data if necessary:
        coerce = {col: _COERCE_METHODS[type(columns[col])] for col in columns if type(columns[col]) in _COERCE_METHODS}
        if coerce:
            # Attempt to coerce types if necessary:
            for record in data:
                for col in record:
                    if col in coerce and record[col] is not None:
                        record[col] = coerce[col](record[col])
        # Write in chunks if too large:
        if len(data) > 10000:
            # Use copy_from to do bulk insert:
            #self.__bulk_insert(data, table_name)
            self.__write_in_chunks(data, 10000, table_name, mode, session)
            return
        elif do_explicit_cast:
            #data = [{c: re.sub("'+", "'", v) for c,v in r.items()} for r in data]
            stmt = stmt.values(**{c: sq.text(f"cast(:{c} as {columns[c]})")
                                  for c in columns})
        if mode == WriteMode.IF_NOT_EXISTS:
            stmt = stmt.on_conflict_do_nothing(index_elements=self.get_unique_constraint_columns(table_name, all_constraints=False))
        session.execute(stmt, data)
        self.__finalize_transaction(session, is_sub, do_commit)
        
    def format_table_for_metadata(self, table_name:str) -> str:
        """
        * Format the table for searching
        in the metadata reflection.
        """
        if table_name.count(".") == 2:
            return ".".join(table_name.split(".")[1:]).lower()
        elif table_name.count(".") == 1:
            return table_name.lower()
        
    def write_literals(self, 
                       data:List[Dict[str, Any]], 
                       table_name:str, 
                       columns:List[str]=None,
                       session:Session=None, 
                       mode=WriteMode.APPEND):
        """
        * Write literals that are expected to be preformatted to the target database.
        """
        # Precheck or process:
        if not data:
            self.log.info("Skipping since no data provided.")
            return
        elif isinstance(data, list) and isinstance(data[0], list) and not columns:
            raise ValueError("columns must be passed if not providing ROWJSON.")
        # Handle raw records:
        elif isinstance(data, list) and isinstance(data[0], list):
            insert_columns = ",".join(columns)
            insert_records = ["(" + ",".join((str(v).replace("'", "''") for v in r)) + ")" for r in data]
        # Process ROWJSON:
        elif isinstance(data, list) and isinstance(data[0], dict):
            insert_columns = ",".join(data[0].keys())
            insert_records = ["(" + ",".join((v.replace("'", "''") for v in r.values())) + ")" for r in data]
        # Generate insert statement:
        insert_header = f"EXEC('INSERT INTO {table_name} ({insert_columns}) VALUES "
        insert_stmt = insert_header + ",\n".join(insert_records) + "')"
        self.execute(insert_stmt, session=session)

    def write_literals_(self, 
                       data:List[Dict[str, Any]], 
                       table_name:str, 
                       session:Session=None, 
                       mode=WriteMode.APPEND):
        """
        * Write literals that are expected to be preformatted to the target database.
        """
        with self.engine.raw_connection().cursor() as cursor:
            insert_columns = ",".join(data[0].keys())
            insert_data = [tuple((v for v in r.values())) for r in data]

            # Prepare your insert query: make sure the column expects a string, not evaluated SQL
            cursor.executemany(f"INSERT INTO {table_name} ({insert_columns}) VALUES ({','.join(['?'] * len(insert_data[0]))})", insert_data)

            self.engine.raw_connection().commit()
        
        
    def write_with_dml(self, 
                       data:List[Dict[str, Any]],
                       table_name:str, 
                       session=None,
                       mode:WriteMode=WriteMode.APPEND):
        """
        * Write data using INSERT type statement.
        """
        dml = [f"insert into {table_name} ({','.join(data[0])})"]
        dml.append("values")
        dml.append(",\n".join(["(" + ",".join(r.values()) + ")" for r in data]))
        dml = "\n".join(dml)
        is_sub = not session is None
        session = Session(self.engine) if not is_sub else session
        dml = self.__escape_bind_params(dml)
        with open("test.sql", "w") as f:
            f.write(dml)
        session.execute(sq.text(dml))
        session.commit()
        if not session:
            session.close()

    def upsert(self, data:Union[Dict[str, List[Any]], List[Tuple]], table_name:str, upsert_cols:dict, session:Session=None, do_commit:bool=False):
        """
        * Upsert data into the target
        table at current connection.
        """
        # Build the where clause:
        if not data:
            self.log.info("No data provided. Skipping upsert.")
            return
        # Use unique key as upsert columns if not provided:
        if upsert_cols is None:
            unique_cols = set(self.get_merge_columns(table_name))
            upsert_cols = tuple([k for k in unique_cols])
        elif isinstance(upsert_cols, dict):
            upsert_cols = tuple([k for k in upsert_cols])
        elif isinstance(upsert_cols, list):
            # Use all existing values in the dataset
            # for the upsert columns
            upsert_cols = tuple([k for k in upsert_cols])
        else:
            raise ValueError("upsert_cols must be a list, dictionary or None.")
        # Build upsert as list of pairs for checking:
        upsert_vals = set()
        for r in data:
            upsert_val = []
            for k in upsert_cols:
                if k not in r:
                    continue
                upsert_val.append(r[k] if not isinstance(r[k], list) else tuple(r[k]))
            upsert_vals.add(tuple(upsert_val))
        lookup = upsert_cols
        upsert_cols = {upsert_cols: list(upsert_vals)}
        is_sub = not session is None
        session = Session(self.engine, expire_on_commit=False) if session is None else session
        table = self.get_table_obj(table_name)
        # Throw exception if not all of the upsert columns
        # are present in the target table:
        table_cols = self.get_column_schema(table_name, names_only=True)
        invalid = [c for c in lookup if c not in table_cols]
        if invalid:
            raise ValueError(f"The following upsert columns not present in {table_name}: {','.join(invalid)}.")
        # If child tables exist with foreign key relationships,
        # then need to include the primary key column in the merge statement:
        self.setup_fkey_relationships()
        # Perform a MERGE instead of a delete and insert
        # if the table has a primary-foreign key relationship
        # with other tables and there is data in the  
        if ((isinstance(upsert_cols, list) or isinstance(upsert_cols, dict)) 
            and self.has_dependent_tables(table_name)
            and self.__downstream_data_exists_for_cols(table_name, upsert_cols)):
            self.log.info("Performing merge into table %s.", table_name)
            merge_cols = self.get_merge_columns(table_name, data, ignore_identity=True)
            if not self.is_unique_constraint(table_name, merge_cols):
                msg = f"""Cannot upsert into {table_name} using columns {','.join(merge_cols)}: 
                is not used in a unique constraint and has dependent tables with values,
                therefore requiring a merge."""
                raise ValueError(msg)
            self.merge_data(data, table_name, merge_cols=merge_cols, filter_values=upsert_cols, session=session, do_commit=do_commit)
        else:
            # Do delete then insert:
            #self.log.debug("Making data unique based on constraints in %s if necessary.", table_name)
            #data = self.make_data_unique(data, table_name)
            self.log.debug("Deleting records then inserting into table %s.", table_name)
            stmt = sq.delete(table)
            values = upsert_cols[lookup]
            if len(lookup) == 1:
                stmt = stmt.where(getattr(table.c, lookup[0]).in_([v for v in values]))
            else:
                stmt = stmt.where(sq.tuple_(*[getattr(table.c, c) for c in lookup]).in_(values))
            self.log.debug("Executing statement %s.", str(stmt))
            session.execute(stmt)
            self.log.debug("Inserting data into %s", table_name)
            self.write(data, table_name, session=session, mode=WriteMode.APPEND, do_commit=do_commit)
        # Close out the transaction:
        self.__finalize_transaction(session, is_sub, do_commit)
        
    def __finalize_transaction(self, session:Session, is_sub:bool=False, do_commit:bool=False):
        # Commit each transaction if requested:
        if do_commit and not is_sub:
            session.commit()
            session.close()
        # Commit without closing session:
        elif do_commit:
            session.commit()
        # Commit and close session:
        elif not is_sub:
            session.commit()
            session.close()

    def merge_tables(self, source:str, target:str, merge_cols:dict=None, filter_values:dict=None, session:Session=None, do_commit:bool=False):
        """
        * Merge data from one table into another.
        """
        is_sub = not session is None
        session = Session(self.engine) if not is_sub else session
        columns = self.get_table_columns(target, cols_only=False, incl_identity=False)
        col_names = [c["name"] for c in columns]
        if merge_cols is None:
            merge_cols = self.get_merge_columns(target)
            if not merge_cols:
                raise ValueError(f"No unique columns or merge columns specified for table {target}")
        using = source
        # Filter out source set if filter_values was provided using a CTE:
        if filter_values and any(isinstance(v, list) for _, v in filter_values.items()):
            lookup = tuple(list(filter_values.keys())[0])
            filter_vals = []
            for f_col_val in filter_values[lookup]:
                filter_val = [self.wrap_literal(v) for v in f_col_val]
                filter_vals.append("(" + ",".join(filter_val) + ")")
            filter = f"({','.join(lookup)}) in (" + ",".join(filter_vals) + ")"
            using = f"""
            SELECT {','.join(c for c in col_names)}
            FROM {source}
            WHERE {filter}
            """
            tmp_tbl_name = self.get_random_temp_table_name(target + "_source_", must_not_exist=False)
            using = self.create_temp_table(using, tmp_tbl_name, session)
        if not merge_cols:
            raise ValueError("Could not determine columns to perform merge with.")
        # Use IS NOT DISTINCT FROM
        # if a merge column in source or target is null:
        # Make temp table for source table.
        match_stmt = []
        source_columns_null_records = self.get_columns_with_null_records(using, session)
        target_columns_null_records = self.get_columns_with_null_records(target, session)
        for c in merge_cols:
            # Use IS NOT DISTINCT FROM to handle null presence
            # in either column:
            if c not in source_columns_null_records and c not in target_columns_null_records:
                match_stmt.append(f"t.{c} = s.{c}")
            else:
                match_stmt.append(f"t.{c} is not distinct from s.{c}")
        match_stmt = " AND ".join(match_stmt)
        update_stmt = ",".join([f'{c} = s.{c}' for c in col_names if c not in merge_cols])
        insert_stmt = ",".join(col_names)
        values_stmt = ",".join([f's.{c}' for c in col_names])
        query = f"""
        merge into {target} as t
        using {using} as s
        on ({match_stmt})
        when not matched then 
        insert ({insert_stmt}) 
        values ({values_stmt})
        when matched then 
        update set {update_stmt}
        """
        self.log.debug("Executing statement: ")
        self.log.debug(query)
        session.execute(sq.text(query))
        self.__finalize_transaction(session, is_sub, do_commit)
    
    def merge_data(self, data:List[Dict[str, Any]], table_name:str, merge_cols:dict, filter_values:dict=None, session:Session=None, do_commit:bool=False):
        """
        * Merge dataset into target table.
        """
        staging_table_name = self.create_staging_table(table_name, data, session)
        self.merge_tables(staging_table_name, table_name, merge_cols, filter_values, session, do_commit)
        self.drop_table(staging_table_name)
        
    def _merge_sqlalchemy(self, table, data, upsert_cols, conn):
        """
        * Version that uses sqlalchemy instead of postgres:
        """
        stmt = psql.insert(table)
        for record in data:
            stmt = stmt.values(**{c: record[c] for c in record})
        where = functools.reduce(lambda a, b: a & b, [getattr(table.c, c).in_(v) for c, v in upsert_cols.items() if v is not None])
        stmt = stmt.on_conflict_do_update(
            index_elements=[c for c in upsert_cols],
            index_where=where,
            set_={k: getattr(stmt.excluded, k) for k in record if k not in upsert_cols}
        )
        self.log.debug("Executing statement %s.", str(stmt))
        conn.execute(stmt)
            
    def merge_non_unique(self, data, table_name, merge_cols, session:Session=None, do_commit:bool=False):
        """
        * 
        """
        is_sub = not session is None
        session = Session(self.engine) if not is_sub else session 
        staging_table_name = self.create_staging_table(table_name, data)
        query = f"""
        UPDATE {table_name} as t
        SET {','.join([f'{c} = tt.{c}' for c in data[0] if c not in merge_cols])}
        FROM {staging_table_name} as tt
        WHERE ({' AND '.join([f't.{c} = tt.{c}' for c in merge_cols])}); 
        """
        self.log.debug("Executing statement: ")
        self.log.debug(query)
        session.execute(query)
        self.log.debug("Dropping the staging table.")
        session.execute(f"DROP TABLE {staging_table_name}")
        self.__finalize_transaction(session, is_sub, do_commit)

    def view_exists(self, view_name:str, session:Session=None) -> bool:
        """
        * Check that the view exists in the current
        connection.
        """
        if self.dialect == SQLDialect.TSQL:
            query = f"""
            select top 0 * 
            from {view_name}
            """
        elif self.dialect == SQLDialect.POSTGRES:
            query = f"""
            select true
            from {view_name}
            limit 1
            """
        else:
            raise RuntimeError(f"Not supported for dialect {self.dialect.name}.")
        try:
            self.read(query, query=True, session=session)
            return True
        except SQLAlchemyError as ex:
            if "UndefinedTable" in str(ex) or re.search(r"invalid\s+object\s+name\s+", str(ex), flags=re.IGNORECASE):
                return False
            else:
                raise ex

    def table_exists(self, table_name:str, session:Session=None) -> bool:
        """
        * Test that the table exists.
        """
        if self.dialect == SQLDialect.TSQL:
            query = f"""
            select top 0 * 
            from {table_name}
            """
        elif self.dialect == SQLDialect.POSTGRES:
            query = f"""
            select true
            from {table_name}
            limit 1
            """
        else:
            raise RuntimeError(f"Not supported for dialect {self.dialect.name}.")
        try:
            self.read(query, query=True, session=session)
            return True
        except SQLAlchemyError as ex:
            if "UndefinedTable" in str(ex) or re.search(r"invalid\s+object\s+name\s+", str(ex), flags=re.IGNORECASE):
                return False
            else:
                raise ex
        
    def get_object_schema(self, obj_name:str) -> str:
        """
        * Retrieve the object's schema.
        """
        if obj_name.count(".") == 2:
            _, schema, _ = obj_name.split(".")
        elif obj_name.count(".") == 1:
            schema, _ = obj_name.split(".")
        else:
            schema = None
        return schema
        
    def get_temp_schemas(self, session:Session=None) -> List[str]:
        """
        * Retrieve all temporary schemas.
        """
        query = """
        SELECT schemaname 
        FROM pg_tables
        WHERE schemaname LIKE 'pg_temp_%';
        """
        records = self.read(query, query=True, out_mode=OutputMode.ROWJSON, session=session)
        return [r["schemaname"] for r in records] if records else None
    
    def get_random_temp_table_name(self, base_name:str=None, session:Session=None, must_not_exist:bool=False) -> str:
        """
        * Get a random table name. Ensure name does not exist.
        """
        schemas = self.get_temp_schemas(session)
        schemas = [] if not schemas else schemas
        base_name = random.choice(schemas) + "." if schemas and not base_name else base_name
        # Do not use schema that isn't temp if 
        # was provided in base_name:
        if "." in base_name and self.get_object_schema(base_name) not in schemas:
            if not schemas:
                base_name = base_name.split(".")[-1]
            else:
                base_name = random.choice(schemas) + "." + base_name.split(".")[-1]
        tmp_tbl_name = base_name + str(random.randint(1, 1000))
        retry_ct = 0
        while not must_not_exist and self.table_exists(tmp_tbl_name, session) and retry_ct < 20:
            tmp_tbl_name = base_name + str(random.randint(1, 1000))
            retry_ct += 1
        if retry_ct == 20:
            raise RuntimeError(f"Could not genereate temp table name with base {base_name} after 20 retries.")
        return tmp_tbl_name

    def get_columns_with_null_records(self, table_name:str, session:Session=None) -> List[str]:
        """
        * Get columns with null records.
        """
        #union_dqls = []
        null_columns = []
        for col in self.get_column_schema(table_name, names_only=True):
            query = f"select '{col.lower()}' as column_name from {table_name} where {col} is null limit 1"
            records = self.read(query, query=True, session=session)
            if records:
                null_columns.append(col)
            #query = "\n UNION \n".join(union_dqls)
            #records = self.read(query, query=True, out_mode=OutputMode.ROWJSON, session=session)
        return null_columns

    def get_primary_keys(self, skip_builtins:bool=False, overwrite:bool=False):
        """
        * Get primary keys defined in instance.
        """
        if self.__primary_keys and not overwrite:
            return self.__primary_keys
        query = """SELECT
                tc.constraint_name as name,
                lower(tc.constraint_type) as type,
                concat(tc.table_schema, '.', tc.table_name) as parent_obj_name,
                'table' as parent_obj_type,
                concat(ccu.table_schema, '.', ccu.table_name) AS external_ref_obj_name,
                'table' as external_ref_obj_type,
                kcu.column_name as external_ref_col,
                ccu.column_name AS child_column_name 
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        """
        if skip_builtins:
            builtins = "','".join(self.built_in_schemas)
            query += f"""
            constraint_schema not in ('{builtins}')
            """
        data = self.read(query, query=True, out_mode=OutputMode.ROWJSON)
        for r in data:
            obj_name = r["parent_obj_name"]
            pkey_col = r["external_ref_col"]
            self.__primary_keys[obj_name] = pkey_col
        return self.__primary_keys
    
    def setup_fkey_relationships(self, 
                                 skip_builtins:bool=False, 
                                 overwrite:bool=False,
                                 pkeys_only:bool=False):
        """
        * Detect primary foreign key relationships.
        """
        if self.relationships and not overwrite:
            return
        # Overwrite the data or pull data initially:
        self.relationships = {}
        builtins = "','".join(self.built_in_schemas).lower()
        if self.dialect == SQLDialect.POSTGRES:
            query = f"""
            SELECT
                ns1.nspname || '.' || cl1.relname AS parent_table,
                array_agg(att1.attname ORDER BY pk_cols.ord) AS parent_keys,
                ns2.nspname || '.' || cl2.relname AS child_table,
                array_agg(att2.attname ORDER BY fk_cols.ord) AS child_keys
            FROM pg_constraint con2
            JOIN pg_class cl2 ON cl2.oid = con2.conrelid
            JOIN pg_namespace ns2 ON ns2.oid = cl2.relnamespace
            JOIN unnest(con2.conkey) WITH ORDINALITY AS fk_cols(attnum, ord) ON TRUE
            JOIN pg_attribute att2 ON att2.attrelid = con2.conrelid AND att2.attnum = fk_cols.attnum
            JOIN pg_constraint con1 ON con2.confrelid = con1.conrelid AND
                                    con2.confkey[fk_cols.ord] = con1.conkey[fk_cols.ord]
            JOIN pg_class cl1 ON cl1.oid = con1.conrelid
            JOIN pg_namespace ns1 ON ns1.oid = cl1.relnamespace
            JOIN unnest(con1.conkey) WITH ORDINALITY AS pk_cols(attnum, ord) ON pk_cols.ord = fk_cols.ord
            JOIN pg_attribute att1 ON att1.attrelid = con1.conrelid AND att1.attnum = pk_cols.attnum
            WHERE con2.contype = 'f'
            AND con1.contype IN {"('p', 'u')" if not pkeys_only else "('p')"} 
            {" AND ns1.nspname not in ('" + builtins + "')" if skip_builtins else " "}
            {" AND ns2.nspname not in ('" + builtins + "')" if skip_builtins else " "}
            GROUP BY
                ns1.nspname, cl1.relname, con1.conname,
                ns2.nspname, cl2.relname, con2.conname
            ORDER BY
                ns1.nspname, cl1.relname, con1.conname, 
                ns2.nspname, cl2.relname, con2.conname;
            """
        else:
            raise RuntimeError(f"Not supported yet for dialect {self.dialect.name}.")
        data = self.read(query, query=True, out_mode=OutputMode.ROWJSON)
        for r in data:
            parent_table = r["parent_table"]
            child_table = r["child_table"]
            parent_cols = tuple(r["parent_keys"])
            fkey_cols = tuple(r["child_keys"])
            if parent_table not in self.relationships:
                self.relationships[parent_table] = {}
            if child_table not in self.relationships[parent_table]:
                self.relationships[parent_table][child_table] = {}
            if parent_cols not in self.relationships[parent_table][child_table]:
                self.relationships[parent_table][child_table][parent_cols] = set()
            rels = self.relationships[parent_table][child_table][parent_cols]
            if isinstance(rels, set):
                rels.add(fkey_cols)
            else:
                rels.append(fkey_cols)
        self.relationships = convert_sets_to_lists(self.relationships)

    def wrap_literal(self, val:Any, type_name:str=None, backend:Any=None, dialect:SQLDialect=None) -> str:
        """
        * Return literal as its string representation
        within the dialect.
        """
        # If the specific type name was passed,
        # then wrap the literal based on the type category:
        if isinstance(val, date):
            val = val.strftime("%Y-%m-%d")
        elif isinstance(val, datetime):
            val = val.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(val, time):
            val = val.strftime("%H:%M:%S")
        elif isinstance(val, timedelta):
            val = str(val)
        elif isinstance(val, Decimal):
            val = int(val)
        elif not isinstance(val, str) and val is not None:
            orig_type = type(val)
            val = str(val).replace("'", r"\"")
            if orig_type == dict:
                val = json.loads(val)
            else:
                val = orig_type(val)
        if type_name and backend:
            if not dialect:
                raise ValueError("dialect must be passed if backend is passed.")
            # Wrap implementing full dialect rules, including how 
            # to handle nested quotes once wrapped.
            elif (self.should_wrap_quotes(type_name, dialect, backend) 
                  and isinstance(val, str) 
                  and not backend.literal_is_wrapped(dialect, val)):
                return backend.wrap_string_literal(dialect, type_name, val)
            else:
                return f"{val}"
        # Wrap the literal based on the content:
        elif isinstance(val, str) and not val.startswith("'") and not val.endswith("'"):
            return f"'{val}'"
        elif isinstance(val, memoryview):
            val = [self.wrap_literal(v.decode("ascii") if isinstance(v, bytes) else v) for v in val.tolist()]
            return f"ARRAY[{','.join(val)}]"
        else:
            return f"{val}"
        
    def should_wrap_quotes(self, type_name:str, dialect:SQLDialect, backend:Any) -> bool:
        """
        * Indicate if should wrap the literal in quotes
        based upon the type name and/or category.
        """
        if dialect == SQLDialect.POSTGRES:
            if backend.sql_type_is_array(dialect, type_name):
                return True
            elif type_name == "money":
                return True
            # Determine if should be wrapped or not based on the type category:
            category = self.get_add_type_category(type_name, backend, dialect)
            return category not in ["boolean", "geometry", "numeric", "unknown"]
        elif dialect == SQLDialect.DATABRICKS:
            return type_name.lower() in ["binary", "date", "string", "timestamp", "timestamp_ntz", "interval day to second", "interval year to month"]
        elif dialect == SQLDialect.TSQL:
            cats = ["string", "schema", "chronological", "pseudo"]
            standard_cat = backend.get_sql_standard_type_category(dialect, type_name)
            return any(standard_cat.startswith(cat) for cat in cats)
        
    def get_id_columns(self, table_name:str) -> List[str]:
        """
        * Retrieve table id columns.    
        """
        table = self.get_table_obj(table_name)
        if table is None:
            raise ValueError(f"No table found for {table_name}.")
        return [col.name for col in table.columns if hasattr(col, "identity") and col.identity is not None]

    def table_has_data(self, table_name:str) -> bool:
        """
        * Check that data     
        """
        query = f"select true from {table_name} limit 1"
        return True if self.read(query, query=True) else False

    def has_dependent_tables(self, parent_table:str):
        """
        * Determine if table has downstream dependent tables,
        i.e. has foreign key relationship with the parent table.
        Return all columns that are used as foreign key in tables.
        """
        return parent_table in self.relationships

    def tables_have_relationship(self, l_table:str, r_table:str) -> bool:
        """
        * Check if tables have relationship.
        """
        errs = []
        if not self.table_exists(l_table):
            errs.append(f"{l_table} does not exist.")
        if not self.table_exists(l_table):
            errs.append(f"{r_table} does not exist.")
        if errs:
            raise ValueError("\n".join(errs))
        return self.get_foreign_keys(l_table, r_table) is not None
        
    def tables_are_linked_by(self, 
                             parent_table:str, 
                             child_table:str, 
                             parent_column:str,
                             checked:set=None) -> bool:
        """
        * Indicate if possible to reach child table
        through primary foreign key relationships directly
        or indirectly by parent column.    
        """
        checked = set() if not checked else checked
        child_tables = self.get_foreign_keys(parent_table)
        if child_table not in child_tables:
            return False
        # If directly reachable then indicate true:
        elif parent_column in child_tables[child_table]:
            return True
        checked.add(child_table)
        # Traverse the graph to determine if reachable:
        for sub_child, fkeys in child_tables.items():
            if sub_child in checked:
                continue
            for fkey in fkeys:
                if self.tables_are_linked_by(parent_table, sub_child, parent_column, checked):
                    return True
            pass

    def is_unique_constraint(self, table_name:str, cols:dict) -> bool:
        """
        * Determine if table has a unique constraint.
        """
        table = self.get_table_obj(table_name)
        if table is None:
            raise ValueError(f"No table found for {table_name}.")
        unique_cols = []
        for const in table.constraints:  
            if isinstance(const, (sq.UniqueConstraint, sq.PrimaryKeyConstraint)):
                unique_cols.extend([c.name for c in const.columns])
        return set(cols).issubset(set(unique_cols))
        
    def get_primary_key_columns(self, table_name:str) -> Dict[str, str]:
        """
        * Output all { (pkey,...) -> (type,...) } information.
        """
        cols_out = {}
        table = self.get_table_obj(table_name)
        if table is None:
            raise ValueError(f"No table found for {table_name}.")
        for cnst in table.constraints:
            if isinstance(cnst, sq.PrimaryKeyConstraint):
                cols = tuple([c.name for c in cnst.columns])
                cols_out[cols] = tuple([c.type.lower() for c in cnst.columns])    
        return cols_out
    
    def get_merge_columns(self, 
                          table_name:str, 
                          data:List[Dict[str, Any]]=None, 
                          ignore_identity:bool=True,
                          incl_types:bool=False):
        """
        * Return columns that should be used
        in a MERGE statement.
        If upsert columns were provided and any of
        them are in a unique key, then need to 
        use all of the columns in the unique key also.
        """
        table = self.get_table_obj(table_name)
        if table is None:
            raise ValueError(f"No table found for {table_name}.")
        unique_const = [const for const in table.constraints if isinstance(const, sq.UniqueConstraint)]
        # Exclude constraints where an id column is used if requested:
        if ignore_identity:
            unique_const =  [const for const in unique_const if not any(hasattr(col, "identity") 
                                                                        and col.identity is not None 
                                                                        and col.identity.always for col in const.columns)]
        unique_const = [[c.name for c in const] for const in unique_const]
        # Do not unique constraints that have columns not present in data set
        # that we intend to merge:
        if data:
            unique_const = [const for const in unique_const if all(c in data[0] for c in const)]
        pkey_const = [c for c in table.constraints if isinstance(c, sq.PrimaryKeyConstraint)]
        pkey_const = list(pkey_const[0].columns) if pkey_const else None
        # Prefer primary key column by default unless it won't work as unique identifier
        # for the upsert that is being performed:
        if pkey_const and not any(hasattr(c, "identity") and c.identity and c.identity.always for c in pkey_const):
            return [c.name for c in pkey_const]
        # Choose the widest constraint:
        unique_const_cts = {}
        for const in unique_const:
            key_len = len(const)
            if key_len not in unique_const_cts:
                unique_const_cts[key_len] = []
            unique_const_cts[key_len].append(const)
        max_cols = max(unique_const_cts)
        id_columns = self.get_id_columns(table_name)
        unique_cols = [const for const in unique_const_cts[max_cols]
                        if not any(c in id_columns for c in const)]
        # If a tie then randomly choose:
        chosen = random.choice(unique_cols) if len(unique_cols) > 0 else unique_cols
        # Include column types in output if requested:
        if incl_types:
            schema = self.get_column_schema(table_name, names_types_only=True)
            chosen = {c: schema[c] for c in chosen}
        return chosen
    
    def get_unique_lookup_expr(self, 
                               table_name:str,
                               row:Dict[str, Any],
                               table_alias:str=None,
                               handle_nulls:bool=True):
        """
        * Get the unique lookup expression
        that can be used in a WHERE clause.
        """
        table_alias = "" if table_alias is None else table_alias
        lookup_value = self.get_unique_lookup_value(table_name, row, handle_nulls)
        lookup_cols = self.get_merge_columns(table_name, [row], ignore_identity=True, incl_types=False) 
        lookup_cols_str = ",".join([table_alias + "." + c for c in lookup_cols])
        lookup_value_str = ",".join([self.wrap_literal(v) for v in lookup_value])
        return f"({lookup_cols_str}) = ({lookup_value_str})"
    
    def get_unique_lookup_value(self, 
                                table_name:str, 
                                row:Dict[str, Any], 
                                handle_nulls:bool=True,
                                const:List[str]=None) -> Tuple[Any]:
        """
        * Get the unique lookup value.
        """
        if const is None:
            const = self.get_merge_columns(table_name, [row], ignore_identity=True, incl_types=False)
        # Put in same order as constraint:
        values_in_order = [row[c] for c in const if c in row]
        value = []
        for v in values_in_order:
            if isinstance(v, list):
                value.append(tuple(v))
            elif isinstance(v, dict):
                value.append(json.dumps(v))
            elif handle_nulls and v is None:
                value.append("")
            else:
                value.append(v)
        value = tuple(value)
        return value
        
    def get_unique_constraint_columns(self, 
                                      table_name:str, 
                                      ignore_identity:bool=True,
                                      all_constraints:bool=False,
                                      incl_types:bool=False):
        """
        * Return unique constraint columns.
        """
        table = self.get_table_obj(table_name)
        if table is None:
            raise ValueError(f"No table found for {table_name}.")
        col_schema = self.get_column_schema(table_name, names_types_only=True)
        unique_const = []
        for const in table.constraints:
            if isinstance(const, (sq.UniqueConstraint, sq.PrimaryKeyConstraint)):
                if ignore_identity and any(c.identity for c in const.columns):
                    continue
                elems = [c.name for c in const.columns]
                if not elems:
                    continue
                unique_const.append({c: col_schema[c] for c in elems} if incl_types else elems)
        # Select the narrowest constraint if multiple present:
        if not all_constraints and len(unique_const) == 1:
            unique_const = unique_const[0]
        elif not all_constraints:
            narrowest_width = min(len(cols) for cols in unique_const)
            unique_const = [cols for cols in unique_const if len(cols) == narrowest_width]
            # If tie then randomly select:
            unique_const = random.choice(unique_const) if len(unique_const) > 1 else unique_const[0]
        return unique_const
    
    # Private Helpers:
    def __write_in_chunks(self, 
                          data:List[Dict[str, Any]], 
                          chunk_size:int, 
                          table_name:str, 
                          mode:WriteMode, 
                          session:Session):
        """
        * Write to table in chunks.
        """
        self.log.info("Writing data in chunks of size %s.", chunk_size)
        # Divide data into chunks then write:
        for n in tqdm(range(len(data) // chunk_size + 1)):
            chunk_elems = data[n*chunk_size:(n+1)*chunk_size]
            if not chunk_elems:
                continue
            self.write(chunk_elems, table_name, mode=WriteMode.APPEND, session=session, do_commit=True)
        #session.commit()
        session.close()

    def __bulk_insert(self, data:List[Dict[str, Any]], table_name:str, session:Session):
        """
        * Use copy_from protocol to perform bulk insert
        of large dataset.
        """
        cursor = self.engine.raw_connection().cursor()
        file = StringIO()
        for r in data:
            file.write("\t".join(v for _, v in r.items()))
        cursor.copy_from(file, table_name)
        
    def __downstream_data_exists_for_cols(self, 
                                          table_name:str, 
                                          upsert_cols:Dict[str, Any]) -> bool:
        """
        * Determine if data exists 
        in any child tables linked by primary-foreign
        key relationship
        for any upsert column values.

        We are checking to determine if deleting source records
        in table_name based on upsert_cols
        will cause a foreign key constraint violation,
        which requires the upsert to be a merge instead of 
        a cleaner delete.
        """
        child_tables = self.get_foreign_keys(table_name)
        if not child_tables:
            return False
        # Check downstream tables (child tables)
        # that have foreign key relationship with table_name
        # have records that will be deleted if we
        # delete table_name records with values in upsert_cols
        # to prevent foreign key violation:
        lookup = list(upsert_cols.keys())[0]
        filter_vals = []
        for f_col_val in upsert_cols[lookup]:
            filter_val = [self.wrap_literal(v) for v in f_col_val]
            filter_vals.append("(" + ",".join(filter_val) + ")")
        filter = f"({','.join(['p.' + c for c in lookup])}) in (" + ",".join(filter_vals) + ")"
        for child_table, relationships in child_tables.items():
            if not self.table_has_data(child_table):
                self.log.debug("Skipping child table %s since no data present.", child_table)
                continue
            elif not any(set(rels).intersection(lookup) for rels in relationships):
                continue
            candidates = {k: v for k,v in relationships.items() if set(k).intersection(lookup)}
            for pkey, fkeys in candidates.items():
                for fkey in fkeys:
                    key_map = {"p." + p_col: "c." + f_col for p_col, f_col in zip(pkey, fkey)}
                    # Build the join key using upsert column literals passed:
                    join_key = " AND ".join([f"{p_col} = {f_col}" for p_col, f_col in key_map.items()])
                    query = f"""
                    select 1
                    from {table_name} as p
                    inner join {child_table} as c
                    on {join_key}
                    where {filter}
                    limit 1;
                    """
                    self.log.debug("Executing query: ")
                    self.log.debug(query)
                    records = self.read(query, query=True)
                    if records:
                        return True
        # No overlap by value between any primary and foreign keys:
        return False

    def __get_procedure_def(self, proc_name:str):
        """
        * Get stored procedure definition.
        """
        if self.dialect == SQLDialect.POSTGRES:
            schema, proc_name = proc_name.split(".")
            query = f"""
            SELECT prosrc FROM pg_proc 
            WHERE proname = '{proc_name}' 
            AND pronamespace::regnamespace::text = '{schema}';"""
            result = self.read(query, query=True, out_mode=OutputMode.ROWJSON)
            return result[0]["prosrc"] if len(result) > 0 else None
        
    def __get_function_def(self, func_name):
        """
        * Get the function definition.
        """ #TODO: move into dialect parser
        if self.dialect == SQLDialect.POSTGRES:
            schema, func_name = func_name.split(".")
            query = f"""
            SELECT pg_get_functiondef(oid) 
            FROM pg_proc 
            WHERE proname = '{func_name}' 
            AND pronamespace::regnamespace::text = '{schema}';"""
            result = self.read(query, query=True, out_mode=OutputMode.ROWJSON)
            return result[0]["pg_get_functiondef"] if len(result) > 0 else None

    def __get_view_def(self, obj_name):
        """
        * Get view ddl.
        """
        if self.dialect == SQLDialect.POSTGRES:
            query = f"SELECT pg_get_viewdef('{obj_name.lower()}')"
            result = self.execute(query)
            return result[0][0] if len(result) > 0 else None
        elif self.dialect == SQLDialect.TSQL:
            query = f"SELECT OBJECT_DEFINITION(OBJECT_ID('{obj_name}')) AS ViewDefinition;"
            result = self.execute(query)
            return result[0][0]

    def get_current_database(self) -> str:
        if self.dialect == SQLDialect.TSQL:
            db_name = self.read("SELECT DB_NAME() AS db", query=True, out_mode=OutputMode.ROWJSON)[0]["db"]
        elif self.dialect == SQLDialect.POSTGRES:
            db_name = self.read("SELECT current_database() as db", query=True, out_mode=OutputMode.ROWJSON)[0]["db"]
        return db_name
    
    def get_all_databases(self) -> List[str]:
        """
        * Retrieve all databases.
        """
        if self.dialect == SQLDialect.TSQL:
            dbs = self.read("SELECT name FROM sys.databases", query=True, out_mode=OutputMode.ROWJSON)
        elif self.dialect == SQLDialect.POSTGRES:
            pass
        return [db["name"] for db in dbs]
    
    def get_all_schemas(self, skip_builtins:bool=True) -> List[str]:
        schemas = [sch.lower() for sch in self.inspector.get_schema_names()]
        if skip_builtins:
            schemas = [sch for sch in schemas if sch not in self.built_in_schemas]
        return schemas
    
    def get_all_tables(self) -> List[str]:
        """
        * Return all tables in current connection.
        """
        return self.inspector.get_table_names()

    def get_all_tables_with_sequences(self, incl_column:bool=False) -> Dict[str, str]:
        """
        * Get all tables serial sequences.
        Return {table_name -> [sequence_name...]}.
        """
        sequence_regex = re.compile(r"nextval\('(.+?)'::regclass\)")
        out = {}
        for table_name in self.inspector.get_table_names():
            columns = self.inspector.get_columns(table_name)
            for column in columns:
                default = column.get("default")
                if not isinstance(default, str):
                    continue
                match = sequence_regex.search(default)
                if not match:
                    continue
                sequence_name = match.group(1)
                if table_name not in out:
                    out[table_name] = []
                record = {sequence_name: column["name"]} if incl_column else sequence_name
                out[table_name].append(record)
        return out

    def get_add_type_category(self, type_name:str, backend:Any, dialect:SQLDialect):
        """
        * Retrieve type category or add to the cache.
        """
        if dialect not in SQLAlchemyConnector.__type_cat_cache:
            SQLAlchemyConnector.__type_cat_cache[dialect] = {}
        if type_name not in SQLAlchemyConnector.__type_cat_cache[dialect]:
            category = backend.get_sql_type_category(dialect, type_name)
            SQLAlchemyConnector.__type_cat_cache[dialect][type_name] = category
        return SQLAlchemyConnector.__type_cat_cache[dialect][type_name]

    def __initialize(self, dialect:SQLDialect, include_schemas:list=None, **extra_conn_kwargs):
        """
        * Initialize the object.
        """
        self.connect(dialect, self.conn_kwargs, include_schemas=include_schemas, **extra_conn_kwargs)
        
    def __validate(self, dialect, include_schemas=None):
        """
        * Validate constructor parameters.
        """
        errs = []
        if isinstance(dialect, str) and dialect.upper() not in SQLDialect.__members__:
            errs.append("dialect is not a listed SQLDialect enum value.")
        elif isinstance(dialect, SQLDialect) and dialect not in SQLAlchemyConnector.__conn_strs: 
            errs.append("dialect does not have connection string template ready.")
        elif not isinstance(dialect, (str, SQLDialect)):
            errs.append("dialect must be a SQLDialect or string corresponding to one.")
        if include_schemas is not None:
            if not isinstance(include_schemas, list):
                errs.append("include_schemas must be a list if provided.")
            elif not all(isinstance(s, str) for s in include_schemas):
                errs.append("include_schemas must only contain strings.")
        if errs:
            raise ValueError("\n".join(errs))
        
    def make_nulls_blank_if_necessary(self, records:List[Dict[str, Any]], table_name:str):
        """
        * Make null columns blank if needed.
        """
        tbl_obj = self.get_table_obj(table_name)
        non_null_cols = set()
        for c in tbl_obj.columns:
            if not c.nullable:
                non_null_cols.add(c.name)
        for r in records:
            r.update({c: "" if v is None and c in non_null_cols else v for c, v in r.items()})
        return records                   
        
    def make_data_unique(self, records:List[Dict[str, Any]], table_name:str):
        """
        * Make the data unique based on
        unique constraints prior to insertion.
        """
        if not records:
            return None
        schema, table = table_name.split(".")
        repeated = []
        out_data = []
        self.change_reflection_if_necessary(table_name)
        # Gather primary key columns. Ignore autoincrement columns/
        #Ignore the pkey and use unique if the pkey column is singular and is identity with default
        pkeys = [c.name for c in self.get_table_obj(table_name).primary_key.columns 
                 if not c.autoincrement or (hasattr(c, "identity") and not c.identity.always)]
        # Only use the primary key if it is present in all of the data.
        unique_const = self.inspector.get_unique_constraints(table, schema)
        unique_const = sorted(unique_const, key=lambda x: len(x["column_names"]), reverse=True)
        # If no unique constraints that can work with and not all primary key columns
        # are available, fail if any pkey columns are missing in any records:
        if pkeys and not unique_const and not all([all(k in r for k in pkeys) for r in records]):
            missing = [[k for k in pkeys if k not in r] for r in records]
            missing = set(itertools.chain.from_iterable(missing))
            raise RuntimeError(f"The following keys are missing from at least one record: {','.join(missing)}")
        # If unique constraints are available then use if not all records have all primary key columns: 
        else:
            use_pkey = True if pkeys and all((all(k in r for k in pkeys) for r in records)) else False
        # Only include constraints where all columns are present in data (assumed to be uniform):
        if unique_const:
            id_cols = self.get_id_columns(table_name)
            # Do not include constraints with id columns:
            unique_const = [cst["column_names"] for cst in unique_const 
                            if not set(cst["column_names"]) - set(records[0])
                            and not any(c in id_cols for c in cst["column_names"])] 
            # Choose the widest:
            max_len = max(len(const) for const in unique_const) if unique_const else None
            unique_const = [const for const in unique_const if len(const) == max_len]
        seen = set()
        if not pkeys and not unique_const:
            return records
        elif unique_const and all(len(cols) > len(pkeys) for cols in unique_const):
            # If more than one unique constraint, then choose
            # make sure all unique constraints apply to the dataset.
            for idx, r in enumerate(records):
                all_keys = []
                for const in unique_const:
                    if any(c not in r for c in const):
                        missing = [c for c in const if c not in r]
                        raise RuntimeError(f"Some records are missing {','.join(missing)} columns.")
                    key = self.get_unique_lookup_value(table_name, r, handle_nulls=True, const=const)
                    all_keys.append(key)
                if all(key not in seen for key in all_keys):
                    out_data.append(r)
                else:
                    repeated.append(r)
                seen.update(all_keys)
            return out_data
        elif use_pkey and pkeys:
            for r in records:
                key = tuple([tuple(v) if isinstance(v, list) else v for k,v in r.items() if k in pkeys])
                if key not in seen:
                    out_data.append(r)
                    seen.add(key)
            return out_data
        
    def __escape_bind_params(self, val:str) -> str:
        """
        * Escape bind parameters.
        """
        if re.search(r"(?<!\\):\d+", val):
            val = re.sub(r"(?<!\\)(:\d+)", r"\\\g<1>", val)
        return val