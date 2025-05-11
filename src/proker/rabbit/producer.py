# messaging_broker/rabbitmq.py
import pika
from typing import Any, Dict, Optional
from proker.retry import auto_reconnect
from proker.core import BaseProducer
from proker.retry import RetryPolicy

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RabbitMQProducer(BaseProducer):
    def __init__(self, config: Dict, retry_policy: RetryPolicy):
        super().__init__()
        self.config = config
        self.retry_policy = retry_policy
        self.connection = None
        self.channel = None

    def connect(self):
        credentials = pika.PlainCredentials(
            self.config.get("username", "guest"), self.config.get("password", "guest")
        )
        if self.config.get("uri", "#") != "#":
            url_parameter = pika.URLParameters(self.config.get("uri"))
            self.connection = pika.BlockingConnection(url_parameter)
        else:
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
            self.declare_exchange(
                exchange_name=exchange["name"],
                exchange_type=exchange.get("type", "topic"),
                durable=exchange.get("durable", True),
            )

        queue = self.config.get("queue")
        if queue:
            self.declare_queue(
                queue_name=queue["name"],
                durable=queue.get("durable", True),
            )

            if exchange:
                self.bind_queue(
                    exchange_name=exchange["name"],
                    queue_name=queue["name"],
                    routing_key=queue.get("routing_key", "#"),
                )

    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "topic",
        durable: bool = True,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        headers: Dict = None,
        message_ttl: int = None,
        max_length_bytes: int = None,
    ):
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
        )

    def declare_queue(
        self,
        queue_name: str,
        durable: bool = True,
        no_ack: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Dict = None,
    ):
        self.channel.queue_declare(
            queue=queue_name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )

    def bind_queue(self, exchange_name: str, queue_name: str, routing_key: str = "#"):
        self.channel.queue_bind(
            exchange=exchange_name,
            queue=queue_name,
            routing_key=routing_key,
        )

    @auto_reconnect
    def publish(self, message: str | bytes, routing_key: Optional[str] = None):
        try:
            exchange = self.config.get("exchange", {}).get("name", "")
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key or self.config.get("routing_key", ""),
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError:
            logger.error("amqp connection error")
            self.connect()
            self.publish(message, routing_key)

    def is_connected(self) -> bool:
        try:
            return bool(
                self.connection
                and self.connection.is_open
                and self.channel
                and self.channel.is_open
            )
        except Exception as e:
            logger.debug(f"Connection check failed: {str(e)}")
            return False
