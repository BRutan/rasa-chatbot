from enum import Enum

class SQLDialect(Enum):
    POSTGRES=1
    SNOWFLAKE=2
    REDSHIFT=3
    TSQL=4
    NEO4J=5
    ORACLE=6
    DATABRICKS=7

class OutputMode(Enum):
    ROWJSON=1 # [ { key -> value } ] with consistent schema
    COLUMNAR=2 # { col -> [values] }
    NODE=3 # [ Node ]
    PANDAS=4 # pandas.DataFrame
    PYSPARK_DF=5
    QUERYINFO=6

class WriteMode(Enum):
    APPEND=1
    UPSERT=2
    OVERWRITE=3
    IF_NOT_EXISTS=4

class ObjectType(Enum):
    QUERY=0
    COLUMN=1
    DATABASE=2
    CATALOG=3
    ETL=4
    FUNCTION=5
    PROCEDURE=6
    SCHEMA=7
    TABLE=8
    TYPE=9
    VIEW=10
    TRIGGER=11
    ROLE=12
    CONSTRAINT=13
    LITERAL=14
    INDEX=15
    PARTITION=16
    PARAMETER=17
    VARIABLE=18
    OPERATOR=19
