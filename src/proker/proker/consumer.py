from abc import ABC, abstractmethod

class ProkerConsumer(ABC):
    @abstractmethod
    def consume(self,topic:str):
        pass
