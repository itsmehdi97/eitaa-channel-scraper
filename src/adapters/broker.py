import json

import pika
from pika.channel import Channel


class RabbitConnection:
    def __init__(
        self,
        *,
        conn_url: str,
    ) -> None:
        self._conn = self._connect(conn_url)

    def bind_queue(
        self, *,
        queue: str,
        exchange: str,
        routing_key: str,
        exchange_type='direct'
    ) -> None:
        _chann = self.get_channel()
        _chann.queue_declare(queue=queue, durable=True)
        _chann.exchange_declare(exchange=exchange, exchange_type=exchange_type)
        _chann.queue_bind(exchange=exchange, routing_key=routing_key, queue=queue)

    def _connect(self, url: str) -> pika.BaseConnection:
        return pika.BlockingConnection(
            pika.URLParameters(url)
        )

    def get_channel(self) -> Channel:
        return self._conn.channel()

    @classmethod
    def publish(
        cls, data: str | bytes, *, channel: Channel, exchange: str, routing_key: str
    ):
        channel.basic_publish(
            exchange=exchange,
            properties=pika.BasicProperties(delivery_mode=2),
            routing_key=routing_key,
            body=data
        )
