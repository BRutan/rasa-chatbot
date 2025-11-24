from datetime import datetime, timedelta
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
import logging
import json
from jose import JWTError, jwt
import os

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        database=os.environ["PG_DBNAME"],
        host=os.environ["PG_HOST"] if os.environ.get("IS_LOCAL", "0") == "1" else "localhost",
        port=os.environ["PG_PORT"],
        min_size=1,
        max_size=10
    )

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def credentials_exception():
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )