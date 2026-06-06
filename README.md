# 📡 aioeventbus

**Asynchronous Event Bus for RabbitMQ, Redis & NATS**

[![PyPI version](https://badge.fury.io/py/aioeventbus.svg)](https://badge.fury.io/py/aioeventbus)
[![Python versions](https://img.shields.io/pypi/pyversions/aioeventbus.svg)](https://pypi.org/project/aioeventbus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Simple, flexible, and production‑ready event bus for microservices communication.

**aioeventbus** provides a unified async API to publish and subscribe to domain events using popular message brokers:
- 🐰 **RabbitMQ** (fanout exchanges)
- 🔴 **Redis** (Pub/Sub)
- 🚀 **NATS** (core subscription model)

Change your broker by editing a single line in the config file – no application code changes required.

---

## ✨ Features

- 🔌 **Broker‑agnostic** – same interface for RabbitMQ, Redis, NATS  
- ⚡ **Fully asynchronous** – built on `asyncio`, perfect for FastAPI / Sanic / aiohttp  
- 🧵 **Sync handlers support** – your handlers can be ordinary functions; they run in a thread pool without blocking the event loop  
- 📝 **Simple decorator‑based registration** – `@bus.handler(EventClass)`  
- 🔁 **Automatic reconnection** – robust connections for all brokers  
- 🛠️ **CLI to generate config files** – get started in seconds  
- 📦 **Minimal dependencies** – only required libraries for your chosen broker  

---

## 📦 Installation

```bash
pip install aioeventbus
```

Additionally, install the client library for the broker you intend to use:

| Broker    | Command                          |
|-----------|----------------------------------|
| RabbitMQ  | `pip install aio-pika`           |
| Redis     | `pip install redis`              |
| NATS      | `pip install nats-py`            |

---

## 🚀 Quick Start

### 1. Create a configuration file

Create `config.yaml`:

```yaml
broker: rabbitmq        # rabbitmq, redis, nats
host: localhost
port: 5672              # default ports: RabbitMQ=5672, Redis=6379, NATS=4222
exchange: domain_events # for RabbitMQ
max_workers: 10
```

Or generate it with the CLI:

```bash
aioeventbus init --broker nats --output config.yaml
```

### 2. Define your domain events

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

### 3. Register handlers

```python
from aioeventbus import create_event_bus

bus = create_event_bus("config.yaml")

@bus.handler(OrderCreated)
def notify_admin(event: OrderCreated):
    print(f"[ADMIN] New order {event.order_id} from {event.customer_id}")

@bus.handler(OrderCreated)
def start_fulfillment(event: OrderCreated):
    print(f"[FULFILLMENT] Processing order {event.order_id}")
```

### 4. Start consuming (e.g., in a FastAPI lifespan)

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

### 5. Publish events from anywhere

```python
@app.post("/orders")
async def create_order(order_id: str, customer_id: str, amount: float):
    event = OrderCreated(order_id, customer_id, amount)
    await bus.publish(event)
    return {"status": "ok"}
```

---

## 📂 Configuration

The library uses a YAML config file. Example for each broker:

<details>
<summary><b>RabbitMQ</b></summary>

```yaml
broker: rabbitmq
host: localhost
port: 5672
exchange: domain_events
max_workers: 10
durable: false        # exclusive queue (deleted when consumer stops)
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

All fields except `broker` are optional – the defaults match the examples above.

---

## 🧰 CLI Tool

`aioeventbus` comes with a command‑line interface:

```bash
aioeventbus init --broker redis --output my_config.yaml
```

Options:
- `--broker` – choose `rabbitmq`, `redis` or `nats`
- `--output` – output file path (default: `eventbus_config.yaml`)
- `--host`, `--port`, `--exchange`, `--channel`, `--subject`, `--max-workers` – override defaults

---

## 🧪 Example with FastAPI

A complete example is available in the [`examples/fastapi_app`](examples/fastapi_app) directory.

Run it:

```bash
cd examples/fastapi_app
pip install -r requirements.txt   # fastapi, uvicorn, aioeventbus
uvicorn main:app --reload
```

Then:
```bash
curl -X POST "http://localhost:8000/orders?order_id=123&customer_id=alice&amount=99.9"
curl -X POST "http://localhost:8000/orders/123/pay?payment_id=pay_456"
```

You'll see logs from the registered handlers.

---

## 🏗️ Architecture

The library is built around the abstract `EventBus` class:

```python
class EventBus(ABC):
    def handler(self, event_cls): ...
    async def publish(self, event) -> None: ...
    async def start_consuming(self) -> None: ...
    async def stop_consuming(self) -> None: ...
```

Three concrete implementations are provided: `RabbitMQEventBus`, `RedisEventBus`, and `NatsEventBus`.  
The `create_event_bus` factory reads your config and returns the correct instance.

**How messages flow:**

```mermaid
graph LR
    A[Publisher] -->|publish(event)| B[Broker]
    B -->|fanout / pub/sub| C[Subscriber]
    C -->|deserialize| D[Event Object]
    D -->|run_in_executor| E[Sync Handlers]
```

- Consumers automatically create temporary queues (or subscriptions) so each subscriber gets a copy of every event.  
- Synchronous handlers run in a thread pool to avoid blocking the asyncio event loop.  
- Connections are managed robustly – they auto‑reconnect if the broker goes down.

---

## 🤝 Contributing

Contributions are very welcome!  
Please check the [issue tracker](https://github.com/yourusername/aioeventbus/issues) and open a PR with a clear description of your changes.

1. Fork the repository  
2. Create a feature branch  
3. Install development dependencies: `pip install -e .[dev]`  
4. Run tests: `pytest`  
5. Submit a pull request  

---

## 📄 License

This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.

---

## 🙌 Acknowledgements

Built with ❤️ using:
- [aio-pika](https://github.com/mosquito/aio-pika) for RabbitMQ
- [redis-py](https://github.com/redis/redis-py) for Redis
- [nats.py](https://github.com/nats-io/nats.py) for NATS

---

**Happy event‑driven coding!** 🚀
