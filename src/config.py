from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(ValueError):
    """실행에 필요한 환경 변수가 없거나 잘못된 경우."""


def _positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value


def _positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a number") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be positive")
    return value


@dataclass(frozen=True)
class RedisSettings:
    host: str
    port: int
    db: int
    password: str | None
    query_ttl: int
    lease_ttl: int


@dataclass(frozen=True)
class RabbitMQSettings:
    host: str
    port: int
    username: str
    password: str
    virtual_host: str
    queue: str
    heartbeat: int
    prefetch_count: int
    reconnect_delay: float


@dataclass(frozen=True)
class KagiSettings:
    api_key: str
    endpoint: str
    timeout: float
    max_attempts: int
    retry_base_delay: float


@dataclass(frozen=True)
class WorkerSettings:
    redis: RedisSettings
    rabbitmq: RabbitMQSettings
    kagi: KagiSettings
    max_message_bytes: int
    max_result_bytes: int
    busy_requeue_delay: float
    log_level: str

    @classmethod
    def from_env(cls) -> WorkerSettings:
        api_key = os.getenv("APIKEY")
        if not api_key:
            raise ConfigurationError("APIKEY is required")

        try:
            redis_db = int(os.getenv("REDIS_DB", "0"))
        except ValueError as exc:
            raise ConfigurationError("REDIS_DB must be an integer") from exc
        if redis_db < 0:
            raise ConfigurationError("REDIS_DB must not be negative")

        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError("LOG_LEVEL is invalid")

        return cls(
            redis=RedisSettings(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=_positive_int("REDIS_PORT", 6379),
                db=redis_db,
                password=os.getenv("REDIS_PASSWORD"),
                query_ttl=_positive_int("SEARCH_QUERY_TTL", 180),
                lease_ttl=_positive_int("SEARCH_LEASE_TTL", 90),
            ),
            rabbitmq=RabbitMQSettings(
                host=os.getenv("RABBITMQ_HOST", "localhost"),
                port=_positive_int("RABBITMQ_PORT", 5672),
                username=os.getenv("RABBITMQ_USER", "guest"),
                password=os.getenv("RABBITMQ_PASSWORD", "guest"),
                virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
                queue=os.getenv("SEARCH_QUEUE", "saver.search.requests"),
                heartbeat=_positive_int("RABBITMQ_HEARTBEAT", 120),
                prefetch_count=_positive_int("SEARCH_PREFETCH_COUNT", 1),
                reconnect_delay=_positive_float("RABBITMQ_RECONNECT_DELAY", 2.0),
            ),
            kagi=KagiSettings(
                api_key=api_key,
                endpoint=os.getenv("KAGI_SEARCH_URL", "https://kagi.com/api/v1/search"),
                timeout=_positive_float("KAGI_TIMEOUT", 10.0),
                max_attempts=_positive_int("KAGI_MAX_ATTEMPTS", 3),
                retry_base_delay=_positive_float("KAGI_RETRY_BASE_DELAY", 0.5),
            ),
            max_message_bytes=_positive_int("SEARCH_MAX_MESSAGE_BYTES", 65_536),
            max_result_bytes=_positive_int("SEARCH_MAX_RESULT_BYTES", 2_000_000),
            busy_requeue_delay=_positive_float("SEARCH_BUSY_REQUEUE_DELAY", 1.0),
            log_level=log_level,
        )
