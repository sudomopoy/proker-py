from abc import ABC, abstractmethod

class ProkerProducer(ABC):
    @abstractmethod
    def publish(self,topic:str, message):
        pass