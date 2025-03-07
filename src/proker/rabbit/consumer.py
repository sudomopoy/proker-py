import pika
import os
from urllib.parse import urlparse
from proker.core import BaseProducer
import json
from proker.retry import auto_reconnect, RetryPolicy
from proker.serializer import BaseSerializer, JSONSerializer
from proker.core import BaseConsumer
from typing import Any, Callable, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RabbitMQConsumer(BaseConsumer):
    def __init__(self, config: Dict, retry_policy: RetryPolicy, serializer: BaseSerializer = JSONSerializer()):
        super().__init__()
        self.serializer = serializer
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
        
    @auto_reconnect
    def consume(self,callback: Callable[[Any], None], queue_name: str = '', auto_ack: bool = True):
        queue = queue_name or self.config.get('queue_name', '')
        self.channel.basic_consume(
            queue=queue,
            on_message_callback=lambda ch, method, properties, body: self._handle_message(ch, method, properties, body, callback),
            auto_ack= auto_ack or self.config.get('auto_ack', True)
        )
        self.channel.start_consuming()
 
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
                durable=queue.get("durable",True),
            )
            
            if exchange:
                self.bind_queue(
                    exchange_name=exchange["name"],
                    queue_name=queue["name"],
                    routing_key=queue.get("routing_key", "#"),
                )

    def declare_exchange(self, exchange_name: str, exchange_type: str = "topic", durable: bool = True, auto_delete: bool = False, internal: bool = False, passive: bool = False, headers: Dict = None, message_ttl: int = None,):
        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
        )
    def declare_queue(self, queue_name: str, durable: bool = True, no_ack: bool = False, exclusive: bool = False, auto_delete: bool = False, arguments: Dict = None):
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
            routing_key= routing_key,
        )

                
    def _handle_message(self, channel, method, properties, body, callback):
        try:
            message = self.serializer.unmarshal(body)
            callback(message)
            if not self.config.get('auto_ack', True):
                channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            if not self.config.get('auto_ack', True):
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
    def is_connected(self) -> bool:
        try:
            return bool(self.connection and self.connection.is_open 
                     and self.channel and self.channel.is_open)
        except Exception as e:
            logger.debug(f"Connection check failed: {str(e)}")
            return False