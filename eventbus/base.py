from abc import ABC, abstractmethod


class EventBus(ABC):
    @abstractmethod
    def handler(self, event_cls):
        pass

    @abstractmethod
    async def publish(self, event) -> None:
        pass

    @abstractmethod
    async def start_consuming(self) -> None:
        pass

    @abstractmethod
    async def stop_consuming(self) -> None:
        pass
