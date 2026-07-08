import hashlib
import json
import unittest
from types import SimpleNamespace

from redis.exceptions import ConnectionError as RedisConnectionError

from src.kagi import KagiSearchError
from src.worker import SearchWorker


QUERY = "검색"
QUERY_HASH = hashlib.sha256(QUERY.encode()).hexdigest()


def delivery():
    payload = {
        "schemaVersion": 1,
        "jobId": QUERY_HASH,
        "magicCode": "A" * 43,
        "query": QUERY,
        "queryHash": QUERY_HASH,
    }
    properties = SimpleNamespace(
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=2,
        message_id=QUERY_HASH,
        type="search.requested.v1",
    )
    method = SimpleNamespace(delivery_tag=7)
    return method, properties, json.dumps(payload, ensure_ascii=False).encode()


class FakeChannel:
    def __init__(self):
        self.acks = []
        self.nacks = []
        self.rejects = []

    def basic_ack(self, **kwargs):
        self.acks.append(kwargs)

    def basic_nack(self, **kwargs):
        self.nacks.append(kwargs)

    def basic_reject(self, **kwargs):
        self.rejects.append(kwargs)


class FakeStore:
    def __init__(self):
        self.completed = False
        self.lease = "lease-token"
        self.complete_calls = []
        self.fail_calls = []
        self.prepare_error = None
        self.write_error = None

    def has_valid_completed_result(self, _query_hash):
        if self.prepare_error:
            raise self.prepare_error
        return self.completed

    def acquire_lease(self, _query_hash):
        return self.lease

    def complete(self, query_hash, token, result):
        if self.write_error:
            raise self.write_error
        self.complete_calls.append((query_hash, token, result))
        return True

    def fail(self, query_hash, token, reason_code):
        if self.write_error:
            raise self.write_error
        self.fail_calls.append((query_hash, token, reason_code))
        return True


class FakeKagi:
    def __init__(self, *, result='{"data":[]}', error=None):
        self.result = result
        self.error = error
        self.queries = []

    def search(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return self.result


class SearchWorkerTest(unittest.TestCase):
    def create_worker(self, store, kagi):
        return SearchWorker(
            store,
            kagi,
            max_message_bytes=65_536,
            busy_requeue_delay=0.001,
        )

    def test_stores_result_before_acknowledging(self):
        channel = FakeChannel()
        store = FakeStore()
        kagi = FakeKagi()
        worker = self.create_worker(store, kagi)

        worker.process(channel, *delivery())

        self.assertEqual(kagi.queries, [QUERY])
        self.assertEqual(store.complete_calls[0][0], QUERY_HASH)
        self.assertEqual(channel.acks, [{"delivery_tag": 7}])
        self.assertEqual(channel.nacks, [])

    def test_acks_cached_result_without_kagi_call(self):
        channel = FakeChannel()
        store = FakeStore()
        store.completed = True
        kagi = FakeKagi()
        worker = self.create_worker(store, kagi)

        worker.process(channel, *delivery())

        self.assertEqual(kagi.queries, [])
        self.assertEqual(channel.acks, [{"delivery_tag": 7}])

    def test_records_upstream_failure_then_acks(self):
        channel = FakeChannel()
        store = FakeStore()
        kagi = FakeKagi(
            error=KagiSearchError("upstream_unavailable", retryable=True)
        )
        worker = self.create_worker(store, kagi)

        worker.process(channel, *delivery())

        self.assertEqual(store.fail_calls[0][2], "upstream_unavailable")
        self.assertEqual(channel.acks, [{"delivery_tag": 7}])

    def test_does_not_ack_when_redis_write_fails(self):
        channel = FakeChannel()
        store = FakeStore()
        kagi = FakeKagi()
        worker = self.create_worker(store, kagi)
        store.write_error = RedisConnectionError("secret redis endpoint")

        worker.process(channel, *delivery())

        self.assertEqual(channel.acks, [])
        self.assertEqual(channel.nacks, [{"delivery_tag": 7, "requeue": True}])

    def test_rejects_invalid_message_without_requeue(self):
        channel = FakeChannel()
        store = FakeStore()
        worker = self.create_worker(store, FakeKagi())
        method, properties, _body = delivery()

        worker.process(channel, method, properties, b"not-json")

        self.assertEqual(channel.rejects, [{"delivery_tag": 7, "requeue": False}])


if __name__ == "__main__":
    unittest.main()
