# messaging_broker/core.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional
from .retry import RetryPolicy
from .rabbitmq import RabbitMQProducer, RabbitMQConsumer
from .kafka import KafkaProducer, KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseConnection(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass


class BaseProducer(BaseConnection):
    @abstractmethod
    def publish(self, message: Dict[str, Any], routing_key: Optional[str] = None):
        pass


class BaseConsumer(BaseConnection):
    @abstractmethod
    def consume(self, callback: Callable[[Dict[str, Any]], None]):
        pass


class MessageBrokerFactory:
    @staticmethod
    def get_producer(
        broker_type: str, config: Dict, retry_policy: RetryPolicy = RetryPolicy()
    ):
        if broker_type == "rabbitmq":
            return RabbitMQProducer(config, retry_policy)
        elif broker_type == "kafka":
            return KafkaProducer(config, retry_policy)
        raise ValueError("Unsupported broker type")

    @staticmethod
    def get_consumer(
        broker_type: str, config: Dict, retry_policy: RetryPolicy = RetryPolicy()
    ):
        if broker_type == "rabbitmq":
            return RabbitMQConsumer(config, retry_policy)
        elif broker_type == "kafka":
            return KafkaConsumer(config, retry_policy)
        raise ValueError("Unsupported broker type")
