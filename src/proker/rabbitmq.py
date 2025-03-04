# messaging_broker/rabbitmq.py
import pika
from typing import Any, Dict, Optional
from .retry import auto_reconnect
from .core import BaseProducer, RetryPolicy
import json


class RabbitMQProducer(BaseProducer):
    def __init__(self, config: Dict, retry_policy: RetryPolicy):
        self.config = config
        self.retry_policy = retry_policy
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(
            self.config.get("username", "guest"), self.config.get("password", "guest")
        )
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5672),
                credentials=credentials,
                virtual_host=self.config.get("virtual_host", "/"),
                heartbeat=self.config.get("heartbeat", 600),
            )
        )
        self.channel = self.connection.channel()
        self._declare_infrastructure()

    def _declare_infrastructure(self):
        exchange = self.config.get("exchange")
        if exchange:
            self.channel.exchange_declare(
                exchange=exchange["name"],
                exchange_type=exchange.get("type", "topic"),
                durable=exchange.get("durable", True),
            )

        queue = self.config.get("queue")
        if queue:
            self.channel.queue_declare(
                queue=queue["name"], durable=queue.get("durable", True)
            )
            if exchange:
                self.channel.queue_bind(
                    exchange=exchange["name"],
                    queue=queue["name"],
                    routing_key=queue.get("routing_key", "#"),
                )

    @auto_reconnect
    def publish(self, message: Dict[str, Any], routing_key: Optional[str] = None):
        exchange = self.config.get("exchange", {}).get("name", "")
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key or self.config.get("routing_key", ""),
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
