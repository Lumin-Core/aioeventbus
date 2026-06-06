# 📡 aioeventbus

**Асинхронная шина событий для RabbitMQ, Redis и NATS**

[![PyPI version](https://badge.fury.io/py/aioeventbus.svg)](https://badge.fury.io/py/aioeventbus)
[![Python versions](https://img.shields.io/pypi/pyversions/aioeventbus.svg)](https://pypi.org/project/aioeventbus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Простая, гибкая и готовая к продакшену библиотека для обмена сообщениями между микросервисами.

**aioeventbus** предоставляет единый асинхронный API для публикации и подписки на доменные события через популярные брокеры:
- 🐰 **RabbitMQ** (fanout-обменники)
- 🔴 **Redis** (Pub/Sub)
- 🚀 **NATS** (базовая модель подписки)

Смените брокер, отредактировав одну строку в конфиге – код приложения останется неизменным.

---

## ✨ Возможности

- 🔌 **Независимость от брокера** – одинаковый интерфейс для RabbitMQ, Redis, NATS
- ⚡ **Полностью асинхронно** – построено на `asyncio`, идеально для FastAPI / Sanic / aiohttp
- 🧵 **Поддержка синхронных обработчиков** – ваши хендлеры могут быть обычными функциями; они выполняются в пуле потоков без блокировки event loop
- 📝 **Простая регистрация через декоратор** – `@bus.handler(EventClass)`
- 🔁 **Автоматическое переподключение** – надёжные соединения для всех брокеров
- 🛠️ **CLI для генерации конфигов** – начните работу за секунды
- 📦 **Минимальные зависимости** – только библиотеки для выбранного брокера

---

## 📦 Установка

```bash
pip install aioeventbus
```

Дополнительно установите клиентскую библиотеку для нужного брокера:

| Брокер    | Команда                          |
|-----------|----------------------------------|
| RabbitMQ  | `pip install aio-pika`           |
| Redis     | `pip install redis`              |
| NATS      | `pip install nats-py`            |

---

## 🚀 Быстрый старт

### 1. Создайте конфигурационный файл

Файл `config.yaml`:

```yaml
broker: rabbitmq        # rabbitmq, redis, nats
host: localhost
port: 5672              # порты по умолчанию: RabbitMQ=5672, Redis=6379, NATS=4222
exchange: domain_events # для RabbitMQ
max_workers: 10
```

Или сгенерируйте его через CLI:

```bash
aioeventbus init --broker nats --output config.yaml
```

### 2. Определите события домена

```python
from dataclasses import dataclass
from aioeventbus import DomainEvent

@dataclass
class OrderCreated(DomainEvent):
    __event_type__ = "OrderCreated"
    order_id: str
    customer_id: str
    amount: float
```

### 3. Зарегистрируйте обработчики

```python
from aioeventbus import create_event_bus

bus = create_event_bus("config.yaml")

@bus.handler(OrderCreated)
def notify_admin(event: OrderCreated):
    print(f"[ADMIN] Новый заказ {event.order_id} от {event.customer_id}")

@bus.handler(OrderCreated)
def start_fulfillment(event: OrderCreated):
    print(f"[FULFILLMENT] Обработка заказа {event.order_id}")
```

### 4. Запустите потребителя (например, в lifespan FastAPI)

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bus.start_consuming()
    yield
    await bus.stop_consuming()

app = FastAPI(lifespan=lifespan)
```

### 5. Публикуйте события из любого места

```python
@app.post("/orders")
async def create_order(order_id: str, customer_id: str, amount: float):
    event = OrderCreated(order_id, customer_id, amount)
    await bus.publish(event)
    return {"status": "ok"}
```

---

## 📂 Конфигурация

Библиотека использует YAML-файл конфигурации. Пример для каждого брокера:

<details>
<summary><b>RabbitMQ</b></summary>

```yaml
broker: rabbitmq
host: localhost
port: 5672
exchange: domain_events
max_workers: 10
durable: false        # временная очередь (удаляется при остановке потребителя)
```
</details>

<details>
<summary><b>Redis</b></summary>

```yaml
broker: redis
host: localhost
port: 6379
channel: domain_events
max_workers: 10
```
</details>

<details>
<summary><b>NATS</b></summary>

```yaml
broker: nats
host: localhost
port: 4222
subject: domain_events
max_workers: 10
```
</details>

Все поля, кроме `broker`, опциональны – значения по умолчанию совпадают с примерами выше.

---

## 🧰 CLI утилита

`aioeventbus` поставляется с интерфейсом командной строки:

```bash
aioeventbus init --broker redis --output my_config.yaml
```

Опции:
- `--broker` – выберите `rabbitmq`, `redis` или `nats`
- `--output` – путь к выходному файлу (по умолчанию `eventbus_config.yaml`)
- `--host`, `--port`, `--exchange`, `--channel`, `--subject`, `--max-workers` – переопределить значения по умолчанию

---

## 🧪 Пример с FastAPI

Полный пример находится в директории [`examples/fastapi_app`](examples/fastapi_app).

Запустите его:

```bash
cd examples/fastapi_app
pip install -r requirements.txt   # fastapi, uvicorn, aioeventbus
uvicorn main:app --reload
```

Затем выполните запросы:

```bash
curl -X POST "http://localhost:8000/orders?order_id=123&customer_id=alice&amount=99.9"
curl -X POST "http://localhost:8000/orders/123/pay?payment_id=pay_456"
```

В терминале появятся сообщения от зарегистрированных обработчиков.

---

## 🏗️ Архитектура

Библиотека построена вокруг абстрактного класса `EventBus`:

```python
class EventBus(ABC):
    def handler(self, event_cls): ...
    async def publish(self, event) -> None: ...
    async def start_consuming(self) -> None: ...
    async def stop_consuming(self) -> None: ...
```

Три конкретные реализации: `RabbitMQEventBus`, `RedisEventBus` и `NatsEventBus`.  
Фабрика `create_event_bus` читает ваш конфиг и возвращает нужный экземпляр.

**Как движутся сообщения:**

```mermaid
graph LR
    A[Издатель] -->|publish(event)| B[Брокер]
    B -->|fanout / pub/sub| C[Подписчик]
    C -->|десериализация| D[Объект события]
    D -->|run_in_executor| E[Синхронные обработчики]
```

- Потребители автоматически создают временные очереди (или подписки), так что каждый подписчик получает копию каждого события.
- Синхронные обработчики запускаются в пуле потоков, чтобы не блокировать event loop asyncio.
- Соединения управляются надёжно – они автоматически переподключаются при обрыве связи с брокером.

---

## 🤝 Участие в разработке

Приветствуются любые вклады!  
Пожалуйста, ознакомьтесь с [трекером задач](https://github.com/yourusername/aioeventbus/issues) и отправляйте pull request с понятным описанием изменений.

1. Сделайте форк репозитория  
2. Создайте ветку для новой функциональности  
3. Установите зависимости для разработки: `pip install -e .[dev]`  
4. Запустите тесты: `pytest`  
5. Отправьте pull request  

---

## 📄 Лицензия

Этот проект распространяется под лицензией **MIT** – подробности в файле [LICENSE](LICENSE).

---

## 🙌 Благодарности

Создано с ❤️ с использованием:
- [aio-pika](https://github.com/mosquito/aio-pika) для RabbitMQ
- [redis-py](https://github.com/redis/redis-py) для Redis
- [nats.py](https://github.com/nats-io/nats.py) для NATS

---

**Приятной событийно-ориентированной разработки!** 🚀
