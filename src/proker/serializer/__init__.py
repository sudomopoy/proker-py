# messaging_broker/core.py (additions)
from abc import ABC, abstractmethod
from typing import Type, Any
import json
from google.protobuf.message import Message


class BaseSerializer(ABC):
    @abstractmethod
    def marshal(self, message: Any) -> bytes:
        pass

    @abstractmethod
    def unmarshal(self, data: bytes) -> Any:
        pass


class JSONSerializer(BaseSerializer):
    def marshal(self, message: Any) -> bytes:
        return json.dumps(message).encode("utf-8")

    def unmarshal(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class ProtobufSerializer(BaseSerializer):
    def __init__(self, proto_class: Type[Message]):
        self.proto_class = proto_class

    def marshal(self, message: Message) -> bytes:
        if not isinstance(message, self.proto_class):
            raise ValueError("Invalid message type")
        return message.SerializeToString()

    def unmarshal(self, data: bytes) -> Message:
        message = self.proto_class()
        message.ParseFromString(data)
        return message
