import asyncpg
from fastapi import FastAPI

db_pool = None

async def init_db(app: FastAPI, dsn: str):
    global db_pool
    db_pool = await asyncpg.create_pool(dsn=dsn)
    app.state.db_pool = db_pool  # optional for access via request.app.state

async def close_db():
    await db_pool.close()