from proker.retry import RetryPolicy
from proker.rabbit.consumer import RabbitMQConsumer
from proker.rabbit.producer import RabbitMQProducer
from proker.core import BaseConsumer, BaseProducer
from proker.serializer import BaseSerializer, JSONSerializer
from typing import Dict


class MessageBrokerFactory:
    @staticmethod
    def get_producer(
        broker_type: str, config: Dict, retry_policy: RetryPolicy = RetryPolicy()
    ) -> BaseProducer:
        if broker_type == "rabbitmq":
            return RabbitMQProducer(config, retry_policy)
        elif broker_type == "kafka":
            raise ValueError("Kafka is not supported yet")
        raise ValueError("Unsupported broker type")

    @staticmethod
    def get_consumer(
        broker_type: str,
        config: Dict,
        retry_policy: RetryPolicy = RetryPolicy(),
        serializer: BaseSerializer = JSONSerializer(),
    ) -> BaseConsumer:
        if broker_type == "rabbitmq":
            return RabbitMQConsumer(config, retry_policy, serializer)
        elif broker_type == "kafka":
            raise ValueError("Kafka is not supported yet")
        raise ValueError("Unsupported broker type")
