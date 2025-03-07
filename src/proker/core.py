# messaging_broker/core.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseConnection(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass


class BaseProducer(BaseConnection):
    @abstractmethod
    def publish(self, message: Dict[str, Any], routing_key: Optional[str] = None):
        pass
    
    @abstractmethod
    def declare_exchange(self, exchange_name: str, exchange_type: str = "topic", durable: bool = True, auto_delete: bool = False, internal: bool = False, passive: bool = False, headers: Dict = None, message_ttl: int = None):
        pass
    @abstractmethod
    def declare_queue(self, queue_name: str, durable: bool = True, no_ack: bool = False, exclusive: bool = False, auto_delete: bool = False, arguments: Dict = None):
        pass
    @abstractmethod
    def bind_queue(self, exchange_name: str, queue_name: str, routing_key: str = "#"):
        pass

class BaseConsumer(BaseConnection):
    @abstractmethod
    def consume(self, callback: Callable[[Dict[str, Any]], None] ,queue_name: str = '', auto_ack: bool = True):
        pass
    
    @abstractmethod
    def declare_exchange(self, exchange_name: str, exchange_type: str = "topic", durable: bool = True, auto_delete: bool = False, internal: bool = False, passive: bool = False, headers: Dict = None, message_ttl: int = None):
        pass
    @abstractmethod
    def declare_queue(self, queue_name: str, durable: bool = True, no_ack: bool = False, exclusive: bool = False, auto_delete: bool = False, arguments: Dict = None):
        pass
    @abstractmethod
    def bind_queue(self, exchange_name: str, queue_name: str, routing_key: str = "#"):
        pass