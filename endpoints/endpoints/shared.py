from argon2 import PasswordHasher, exceptions
from asyncpg.pool import PoolConnectionProxy
from decimal import Decimal
from fastapi import HTTPException
from fastapi.security import OAuth2PasswordRequestForm
from functools import wraps
import httpx
import json
import inspect
import logging
import logging.config
import os
from pydantic import BaseModel
import yaml
from typing import Any, Dict, Union

HASHER = PasswordHasher()

def get_logger() -> logging.Logger:
    """
    * Return logging object using configuration.
    """
    config_path = os.path.join(os.environ["HOME"], "objects/config/logging.yaml")
    with open(config_path, "r") as f:
        log_cfg = yaml.safe_load(f)
    logging.config.dictConfig(log_cfg)
    return logging.getLogger(os.environ["APP_NAME"])

logger = get_logger()

def normalize_data(data:Union[Dict[str, Any], BaseModel]) -> Dict[str, Any]:
    """
    * Normalize data prior to insert.
    """
    if isinstance(data, BaseModel):
        data = data.model_dump()
    data = {c: v.lower() if isinstance(v, str) else v for c,v in data.items()}
    data = {c: str(v) if isinstance(v, (float, Decimal)) else v for c,v in data.items()}
    return data

async def send_file_to_endpoint(file_path:str, 
                                url:str, 
                                method_name:str,
                                header:Dict[str, Any]=None,
                                data:Dict[str, Any]=None):
    """
    * Send file to endpoint.
    """
    if file_path.endswith(".docx"):
        format = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        mode = "rb"
    elif file_path.endswith(".pdf"):
        format = "application/pdf"
        mode = "rb"
    elif file_path.endswith(".txt"):
        format = "text/plain"
        mode = "rb"
    elif file_path.endswith(".csv"):
        format = "text/csv"
        mode = "rb"
    elif file_path.endswith(".jpg"):
        format = "image/jpeg"
        mode = "rb"
    elif file_path.endswith(".png"):
        format = "image/png"
        mode = "rb"
    else:
        _, ext = os.path.splitext(file_path)
        raise ValueError(f"Not supported for extension {ext}.")
    async with httpx.AsyncClient() as client:
        with open(file_path, mode) as f:
            files = {"file": (file_path, f, format)}
            method = getattr(client, method_name)
            # Note that data needs to be dictionary when using form and not json.dumps():
            result = await method(url, data=data, files=files, headers=header)
            if result.status_code != 200:
                logger.error("error: %s", result.text)
            result.raise_for_status()
            return result.json()

async def split_insert_data(data:BaseModel, conn:PoolConnectionProxy):
    """
    * Split and insert data into fact and dimension tables,
    in required order.
    """
    pass

async def make_retrieve_token(conn:PoolConnectionProxy, form:OAuth2PasswordRequestForm):
    """
    * If user has not been registered, register in backend using hashed password, then return
    generated auth_token.
    If registered then check password. If invalid throw exception. Otherwise return
    the previously generated auth_token.
    """
    # Check if user is registered.
    query = "select password as hashed_password, token from credentials.auth_tokens where username = $1"
    result = await conn.fetchrow(query, form.username)
    if result is None:
        logger.info("Username is not registered. Registering and returning token.")
        query = """
        INSERT INTO users.credentials (username, password, token)
        VALUES ($1, $2, $3)
        """
        # Determine if the user is registered:
        hashed_password = HASHER.hash(form.password)
        token = secrets.token_urlsafe(32)
        await conn.execute(query, form.username, hashed_password, token)
    elif result == "1":
        raise HTTPException(status_code=401, detail="Password incorrect.")
    # Compare the stored hashed password, fail 
    else:
        try:
            HASHER.verify(result["hashed_password"], form.password)
        except exceptions.VerifyMismatchError:
            raise HTTPException("Password does not match.")
        token = result["token"]
    return token

def log_execution(func):
    """
    * Log start end end of logs.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        logger.info(f"Starting {func.__name__}()")
        if inspect.ismethod(func):
            result = func(self, *args, **kwargs)
        else:
            args = tuple([self] + list(args))
            result = func(*args, **kwargs)
        logger.info(f"Finished {func.__name__}()")
        return result
    return wrapper

def async_log_execution(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        logger.info(f"Starting {func.__name__}()")
        if inspect.ismethod(func):
            result = await func(self, *args, **kwargs)
        else:
            args = tuple([self] + list(args))
            result = await func(*args, **kwargs)
        logger.info(f"Finished {func.__name__}()")
        return result
    return wrapper