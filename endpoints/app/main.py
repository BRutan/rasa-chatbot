import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dependencies import logger
from functions.backend import reset_document_storage
from routers import auth, backend, cases, chatbot, transactions, users
import os

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    logger.debug("Starting up.")
    dsn = "postgresql://{username}:{password}@{host}:{port}/{dbname}"
    dsn = dsn.format(username=os.environ["PG_USERNAME"],
                     password=os.environ["PG_PASSWORD"],
                     host=os.environ["PG_HOST"],
                     port=os.environ["PG_PORT"],
                     dbname=os.environ["PG_DBNAME"])
    logger.debug("Using dsn: %s.", dsn)
    app.state.db_pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=10,
    )
    # Reset the stored documents if RESET_BACKEND env variable set:
    if os.environ.get("RESET_BACKEND", "0") == "1":
        logger.info("Resetting document storage since RESET_BACKEND == 1.")
        reset_document_storage()

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()

app.include_router(auth.router)
app.include_router(backend.router)
app.include_router(cases.router)
app.include_router(chatbot.router)
app.include_router(transactions.router)
app.include_router(users.router)

@app.get("/healthcheck")
async def health_check():
    return { "message": "connected to endpoints." }