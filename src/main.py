import logging
import sys

import dotenv
import redis

from src.config import ConfigurationError, WorkerSettings
from src.kagi import KagiSearchClient
from src.store import RedisResultStore
from src.worker import RabbitMQConsumer, SearchWorker


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def run() -> int:
    dotenv.load_dotenv(dotenv_path="../.env")
    try:
        settings = WorkerSettings.from_env()
    except ConfigurationError as exc:
        logging.basicConfig(level=logging.ERROR, stream=sys.stderr)
        logging.error(
            "worker_configuration_invalid error=%s detail=%s",
            type(exc).__name__,
            exc,
        )
        return 2

    configure_logging(settings.log_level)
    redis_client = redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        password=settings.redis.password,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        health_check_interval=30,
    )
    store = RedisResultStore(
        redis_client,
        query_ttl=settings.redis.query_ttl,
        lease_ttl=settings.redis.lease_ttl,
        max_result_bytes=settings.max_result_bytes,
    )
    kagi = KagiSearchClient(
        settings.kagi,
        max_result_bytes=settings.max_result_bytes,
    )
    try:
        store.ping()
    except (redis.RedisError, OSError, TimeoutError) as exc:
        logging.error("worker_startup_failed dependency=redis error=%s", type(exc).__name__)
        kagi.close()
        store.close()
        return 1

    worker = SearchWorker(
        store,
        kagi,
        max_message_bytes=settings.max_message_bytes,
        busy_requeue_delay=settings.busy_requeue_delay,
    )
    consumer = RabbitMQConsumer(settings.rabbitmq, worker)
    consumer.install_signal_handlers()
    try:
        consumer.run()
    finally:
        kagi.close()
        store.close()
        logging.info("worker_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
