from fastapi import FastAPI
from loguru import logger
import asyncpg
from contextlib import asynccontextmanager
import os
import sys

def setup_logging():
    """
    Configures loguru logging based on environment variables.

    Environment Variables:
    - LOG_LEVEL: Severity level (DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL). Default: INFO.
    - LOG_FORMAT: Custom format string for logs.
    - LOG_SERIALIZE: Set to "true" to output logs in JSON format.
    - LOG_ENQUEUE: Set to "true" to make logging thread-safe and non-blocking. Default: true.
    """

    # Remove default handler
    logger.remove()

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = os.getenv(
        "LOG_FORMAT",
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    log_serialize = os.getenv("LOG_SERIALIZE", "false").lower() == "true"
    log_enqueue = os.getenv("LOG_ENQUEUE", "true").lower() == "true"

   # Add stdout handler
    logger.add(
        sys.stdout,
        level=log_level,
        format=log_format,
        serialize=log_serialize,
        colorize=True,
        enqueue=log_enqueue
    )
    logger.info(f"Logging initialized with level: {log_level}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "15432")
    pg_user = os.getenv("PG_USER", "saver")
    pg_password = os.getenv("PG_USER", "saver")
    pg_database = os.getenv("PG_USER", "saverdb")
    app.state.pool = await asyncpg.create_pool(
        user=pg_user,
        password=pg_password,
        database=pg_database,
        host=pg_host,
        port=pg_port,
    )
    yield
    await app.state.pool.close()


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

# TODO: 검색어와 매직코드가 api를 통해 들어오면 rabbitmq 메시지 소비해서 매직코드 검증. kagi 호출 후 그 결과를 reddis에 저장하여 캐시로 사용함.
