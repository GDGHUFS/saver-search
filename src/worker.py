from __future__ import annotations

import logging
import signal
import threading
import time
from contextlib import suppress
from typing import Any

import pika
from redis.exceptions import RedisError

from src.config import RabbitMQSettings
from src.kagi import KagiSearchClient, KagiSearchError
from src.message import InvalidSearchMessage, parse_search_command
from src.store import RedisResultStore


LOGGER = logging.getLogger(__name__)
REDIS_ERRORS = (RedisError, OSError, TimeoutError)


class SearchWorker:
    def __init__(
        self,
        store: RedisResultStore,
        kagi: KagiSearchClient,
        *,
        max_message_bytes: int,
        busy_requeue_delay: float,
    ) -> None:
        self._store = store
        self._kagi = kagi
        self._max_message_bytes = max_message_bytes
        self._busy_requeue_delay = busy_requeue_delay

    def process(self, channel: Any, method: Any, properties: Any, body: bytes) -> None:
        delivery_tag = method.delivery_tag
        try:
            command = parse_search_command(
                body,
                properties,
                max_body_bytes=self._max_message_bytes,
            )
        except InvalidSearchMessage as exc:
            LOGGER.warning("search_message_rejected reason=%s", exc.reason_code)
            channel.basic_reject(delivery_tag=delivery_tag, requeue=False)
            return

        job_ref = command.job_id[:12]
        try:
            if self._store.has_valid_completed_result(command.query_hash):
                LOGGER.info("search_cache_hit job=%s", job_ref)
                channel.basic_ack(delivery_tag=delivery_tag)
                return
            lease_token = self._store.acquire_lease(command.query_hash)
        except REDIS_ERRORS as exc:
            LOGGER.error("search_storage_unavailable operation=prepare error=%s", type(exc).__name__)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return

        if lease_token is None:
            LOGGER.info("search_job_busy job=%s", job_ref)
            time.sleep(self._busy_requeue_delay)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return

        try:
            result = self._kagi.search(command.query)
        except KagiSearchError as exc:
            LOGGER.error(
                "search_upstream_failed job=%s reason=%s retryable=%s",
                job_ref,
                exc.reason_code,
                exc.retryable,
            )
            self._finish_failed(
                channel,
                delivery_tag,
                command.query_hash,
                lease_token,
                exc.reason_code,
            )
            return
        except Exception as exc:
            LOGGER.critical(
                "search_worker_internal_error job=%s error=%s",
                job_ref,
                type(exc).__name__,
            )
            raise

        try:
            stored = self._store.complete(command.query_hash, lease_token, result)
        except (RedisError, OSError, TimeoutError, ValueError) as exc:
            LOGGER.error("search_storage_unavailable operation=complete error=%s", type(exc).__name__)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return
        if not stored:
            LOGGER.warning("search_lease_lost job=%s operation=complete", job_ref)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return

        channel.basic_ack(delivery_tag=delivery_tag)
        LOGGER.info("search_completed job=%s", job_ref)

    def _finish_failed(
        self,
        channel: Any,
        delivery_tag: int,
        query_hash: str,
        lease_token: str,
        reason_code: str,
    ) -> None:
        try:
            stored = self._store.fail(query_hash, lease_token, reason_code)
        except REDIS_ERRORS as exc:
            LOGGER.error("search_storage_unavailable operation=fail error=%s", type(exc).__name__)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return
        if not stored:
            LOGGER.warning("search_lease_lost job=%s operation=fail", query_hash[:12])
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
            return
        channel.basic_ack(delivery_tag=delivery_tag)


class RabbitMQConsumer:
    def __init__(self, settings: RabbitMQSettings, worker: SearchWorker) -> None:
        self._settings = settings
        self._worker = worker
        self._stop_event = threading.Event()
        self._connection: pika.BlockingConnection | None = None

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._consume_once()
            except (pika.exceptions.AMQPError, OSError, TimeoutError) as exc:
                if self._stop_event.is_set():
                    break
                LOGGER.error("rabbitmq_connection_lost error=%s", type(exc).__name__)
                self._stop_event.wait(self._settings.reconnect_delay)
            finally:
                self._close_connection()

    def _consume_once(self) -> None:
        credentials = pika.PlainCredentials(
            self._settings.username,
            self._settings.password,
            erase_on_connect=True,
        )
        parameters = pika.ConnectionParameters(
            host=self._settings.host,
            port=self._settings.port,
            virtual_host=self._settings.virtual_host,
            credentials=credentials,
            heartbeat=self._settings.heartbeat,
            blocked_connection_timeout=5,
            socket_timeout=5,
            connection_attempts=3,
            retry_delay=1,
        )
        connection = pika.BlockingConnection(parameters)
        self._connection = connection
        channel = connection.channel()
        channel.queue_declare(queue=self._settings.queue, durable=True)
        channel.basic_qos(prefetch_count=self._settings.prefetch_count)
        channel.basic_consume(
            queue=self._settings.queue,
            on_message_callback=self._worker.process,
            auto_ack=False,
        )
        LOGGER.info("rabbitmq_consumer_started queue=%s", self._settings.queue)
        channel.start_consuming()

    def stop(self, *_signal_args: Any) -> None:
        if self._stop_event.is_set():
            return
        LOGGER.info("worker_shutdown_requested")
        self._stop_event.set()
        connection = self._connection
        if connection is not None and connection.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                connection.add_callback_threadsafe(connection.close)

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def _close_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None and connection.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                connection.close()
