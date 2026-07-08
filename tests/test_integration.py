import hashlib
import json
import os
import unittest
import uuid

import httpx
import pika
import redis

from src.config import KagiSettings
from src.kagi import KagiSearchClient
from src.store import RedisResultStore
from src.worker import SearchWorker


@unittest.skipUnless(
    os.getenv("SAVER_INTEGRATION_TEST") == "1",
    "set SAVER_INTEGRATION_TEST=1 to use local Redis and RabbitMQ",
)
class LocalInfrastructureIntegrationTest(unittest.TestCase):
    def test_consumes_delivery_and_stores_completed_result(self):
        query = f"integration-{uuid.uuid4().hex}"
        query_hash = hashlib.sha256(query.encode()).hexdigest()
        queue = f"saver.search.test.{uuid.uuid4().hex}"
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD"),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        store = RedisResultStore(
            redis_client,
            query_ttl=60,
            lease_ttl=30,
            max_result_bytes=10_000,
        )
        http_client = httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={"data": {"search": [{"title": "integration result"}]}},
                )
            )
        )
        kagi = KagiSearchClient(
            KagiSettings(
                api_key="test-key",
                endpoint="https://kagi.test/search",
                timeout=1,
                max_attempts=1,
                retry_base_delay=0.01,
            ),
            max_result_bytes=10_000,
            client=http_client,
        )
        worker = SearchWorker(
            store,
            kagi,
            max_message_bytes=65_536,
            busy_requeue_delay=0.01,
        )
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASSWORD", "guest"),
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST", "localhost"),
                port=int(os.getenv("RABBITMQ_PORT", "5672")),
                virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
                credentials=credentials,
                socket_timeout=2,
                blocked_connection_timeout=2,
            )
        )
        channel = connection.channel()
        try:
            channel.queue_declare(queue=queue, durable=True)
            body = json.dumps(
                {
                    "schemaVersion": 1,
                    "jobId": query_hash,
                    "magicCode": "A" * 43,
                    "query": query,
                    "queryHash": query_hash,
                }
            ).encode()
            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    delivery_mode=pika.DeliveryMode.Persistent,
                    message_id=query_hash,
                    type="search.requested.v1",
                ),
            )
            method, properties, delivered_body = channel.basic_get(
                queue=queue,
                auto_ack=False,
            )
            self.assertIsNotNone(method)

            worker.process(channel, method, properties, delivered_body)

            result = redis_client.hgetall(store.query_key(query_hash))
            self.assertEqual(result["status"], "COMPLETED")
            self.assertEqual(
                json.loads(result["result"])["data"]["search"][0]["title"],
                "integration result",
            )
            queue_state = channel.queue_declare(queue=queue, passive=True)
            self.assertEqual(queue_state.method.message_count, 0)
        finally:
            redis_client.delete(store.query_key(query_hash), store.lease_key(query_hash))
            channel.queue_delete(queue=queue)
            connection.close()
            http_client.close()
            store.close()


if __name__ == "__main__":
    unittest.main()
