import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Type
import aio_pika
from aio_pika import Message, ExchangeType, connect_robust
from aioeventbus.eventbus.base import EventBus
from aioeventbus.eventbus.events import DomainEvent


class RabbitMQEventBus(EventBus):
    def __init__(
        self,
        host: str = "localhost",
        exchange: str = "domain_events",
        max_workers: int = 10
    ) -> None:
        self.broker_name = "RabbitMQ"
        self.host = host
        self.exchange_name = exchange
        self._subscriptions: Dict[str, List[Callable]] = defaultdict(list)
        self._event_classes: Dict[str, Type[DomainEvent]] = {}

        # Publish connection
        self._pub_connection: Optional[aio_pika.RobustConnection] = None
        self._pub_channel: Optional[aio_pika.RobustChannel] = None
        self._pub_exchange: Optional[aio_pika.Exchange] = None

        # Consumer
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

    async def _ensure_publish_connection(self):
        """Гарантирует, что соединение для публикации открыто."""
        if self._pub_connection and not self._pub_connection.is_closed:
            return
        if self._pub_connection:
            await self._pub_connection.close()
        self._pub_connection = await connect_robust(host=self.host)
        self._pub_channel = await self._pub_connection.channel()
        self._pub_exchange = await self._pub_channel.declare_exchange(
            self.exchange_name, ExchangeType.FANOUT, durable=False
        )

    async def publish(self, event: DomainEvent) -> None:
        """Асинхронная публикация события в RabbitMQ."""
        await self._ensure_publish_connection()
        message = json.dumps(event.to_dict())
        await self._pub_exchange.publish(
            Message(body=message.encode()),
            routing_key=""
        )

    async def start_consuming(self) -> None:
        """Запускает фоновую задачу-потребителя."""
        if self._consumer_task and not self._consumer_task.done():
            return
        self._stop_event.clear()
        self._consumer_task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        """Основной цикл потребления сообщений (работает в отдельной Task)."""
        # Отдельное соединение для потребления
        connection = await connect_robust(host=self.host)
        try:
            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                self.exchange_name, ExchangeType.FANOUT, durable=False
            )
            queue = await channel.declare_queue(exclusive=True)
            await queue.bind(exchange)

            async def on_message(message: aio_pika.IncomingMessage):
                async with message.process():
                    raw = json.loads(message.body.decode())
                    event_type = raw.get("event_type")
                    data = raw.get("data", {})
                    event_cls = self._event_classes.get(event_type)
                    if event_cls is None:
                        print(f"[WARN] Unknown event type: {event_type}")
                        return
                    event = event_cls(**data)

                    # Вызываем синхронные обработчики в пуле потоков
                    loop = asyncio.get_running_loop()
                    for handler in self._subscriptions.get(event_type, []):
                        await loop.run_in_executor(self._executor, handler, event)

            await queue.consume(on_message)
            # Ожидаем сигнала остановки
            await self._stop_event.wait()
        finally:
            await connection.close()

    async def stop_consuming(self) -> None:
        """Останавливает потребителя и освобождает ресурсы."""
        self._stop_event.set()
        if self._consumer_task:
            await self._consumer_task
        if self._pub_connection and not self._pub_connection.is_closed:
            await self._pub_connection.close()
        self._executor.shutdown(wait=False)
