from __future__ import annotations

import json
import secrets

from redis import Redis


QUERY_PREFIX = "saver:search:query:"
LEASE_PREFIX = "saver:search:lease:"

COMPLETE_SCRIPT = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'COMPLETED', 'result', ARGV[2])
redis.call('HDEL', KEYS[1], 'error_code')
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('DEL', KEYS[2])
return 1
"""

FAIL_SCRIPT = """
if redis.call('GET', KEYS[2]) ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'FAILED', 'error_code', ARGV[2])
redis.call('HDEL', KEYS[1], 'result')
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('DEL', KEYS[2])
return 1
"""

RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisResultStore:
    def __init__(
        self,
        redis: Redis,
        *,
        query_ttl: int,
        lease_ttl: int,
        max_result_bytes: int,
    ) -> None:
        self._redis = redis
        self._query_ttl = query_ttl
        self._lease_ttl = lease_ttl
        self._max_result_bytes = max_result_bytes

    @staticmethod
    def query_key(query_hash: str) -> str:
        return f"{QUERY_PREFIX}{query_hash}"

    @staticmethod
    def lease_key(query_hash: str) -> str:
        return f"{LEASE_PREFIX}{query_hash}"

    def ping(self) -> None:
        self._redis.ping()

    def has_valid_completed_result(self, query_hash: str) -> bool:
        values = self._redis.hmget(self.query_key(query_hash), "status", "result")
        status, raw_result = values
        if status != "COMPLETED" or not isinstance(raw_result, str):
            return False
        if len(raw_result.encode("utf-8")) > self._max_result_bytes:
            return False
        try:
            parsed = json.loads(
                raw_result,
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(parsed, dict) or "meta" not in parsed or "data" not in parsed:
            return False
        return True

    def acquire_lease(self, query_hash: str) -> str | None:
        token = secrets.token_urlsafe(24)
        acquired = self._redis.set(
            self.lease_key(query_hash),
            token,
            nx=True,
            ex=self._lease_ttl,
        )
        return token if acquired else None

    def complete(self, query_hash: str, token: str, result: str) -> bool:
        if len(result.encode("utf-8")) > self._max_result_bytes:
            raise ValueError("result exceeds Redis contract size")
        return bool(
            self._redis.eval(
                COMPLETE_SCRIPT,
                2,
                self.query_key(query_hash),
                self.lease_key(query_hash),
                token,
                result,
                self._query_ttl,
            )
        )

    def fail(self, query_hash: str, token: str, reason_code: str) -> bool:
        return bool(
            self._redis.eval(
                FAIL_SCRIPT,
                2,
                self.query_key(query_hash),
                self.lease_key(query_hash),
                token,
                reason_code,
                self._query_ttl,
            )
        )

    def release_lease(self, query_hash: str, token: str) -> None:
        self._redis.eval(RELEASE_SCRIPT, 1, self.lease_key(query_hash), token)

    def close(self) -> None:
        self._redis.close()
