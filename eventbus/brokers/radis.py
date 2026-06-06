import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Type
import redis.asyncio as redis
from redis.asyncio.client import PubSub

from aioeventbus.eventbus.base import EventBus
from aioeventbus.eventbus.events import DomainEvent


class RedisEventBus(EventBus):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        channel: str = "domain_events",
        max_workers: int = 10,
    ) -> None:
        self.broker_name = "Redis"
        self.host = host
        self.port = port
        self.channel = channel
        self._subscriptions: Dict[str, List[Callable]] = defaultdict(list)
        self._event_classes: Dict[str, Type[DomainEvent]] = {}

        # Пул соединений для публикации (и для подписки тоже может использоваться)
        self._pub_redis: Optional[redis.Redis] = None
        self._sub_redis: Optional[redis.Redis] = None
        self._pubsub: Optional[PubSub] = None

        self._consumer_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def handler(self, event_cls: Type[DomainEvent]):
        """Декоратор для регистрации обработчиков событий."""
        event_type = event_cls.__event_type__
        self._event_classes[event_type] = event_cls

        def decorator(func: Callable):
            self._subscriptions[event_type].append(func)
            return func

        return decorator

    async def _ensure_publish_connection(self) -> None:
        """Гарантирует, что соединение для публикации открыто."""
        if self._pub_redis is not None:
            return
        self._pub_redis = await redis.from_url(f"redis://{self.host}:{self.port}")

    async def publish(self, event: DomainEvent) -> None:
        """Асинхронная публикация события в Redis."""
        await self._ensure_publish_connection()
        message = json.dumps(event.to_dict())
        await self._pub_redis.publish(self.channel, message)

    async def start_consuming(self) -> None:
        """Запускает фоновую задачу-потребителя."""
        if self._consumer_task and not self._consumer_task.done():
            return
        self._stop_event.clear()
        self._consumer_task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        """Основной цикл потребления сообщений (работает в отдельной Task)."""
        # Отдельное соединение для подписки
        self._sub_redis = await redis.from_url(f"redis://{self.host}:{self.port}")
        self._pubsub = self._sub_redis.pubsub()
        await self._pubsub.subscribe(self.channel)

        try:
            async for raw_message in self._pubsub.listen():
                if self._stop_event.is_set():
                    break
                if raw_message["type"] != "message":
                    continue
                # raw_message["data"] — это байты
                raw = json.loads(raw_message["data"].decode())
                event_type = raw.get("event_type")
                data = raw.get("data", {})
                event_cls = self._event_classes.get(event_type)
                if event_cls is None:
                    print(f"[WARN] Unknown event type: {event_type}")
                    continue
                event = event_cls(**data)

                loop = asyncio.get_running_loop()
                for handler in self._subscriptions.get(event_type, []):
                    await loop.run_in_executor(self._executor, handler, event)
        finally:
            await self._pubsub.unsubscribe(self.channel)
            await self._pubsub.close()
            await self._sub_redis.close()

    async def stop_consuming(self) -> None:
        """Останавливает потребителя и освобождает ресурсы."""
        self._stop_event.set()
        if self._consumer_task:
            await self._consumer_task
        if self._pub_redis:
            await self._pub_redis.close()
        self._executor.shutdown(wait=False)
