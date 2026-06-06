from contextlib import asynccontextmanager
from fastapi import FastAPI
from aioeventbus.examples.fastapi_app.events import OrderCreated, OrderPaid
from aioeventbus.examples.fastapi_app.handlers import bus


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.start_consuming()
        # Отображаем имя текущего брокера
        broker_name = getattr(bus, "broker_name", "Unknown")
        print(f"✅ Connected to {broker_name}")
    except Exception as e:
        print(f"❌ Failed to connect to broker: {e}")
        raise
    yield
    await bus.stop_consuming()


app = FastAPI(lifespan=lifespan)


@app.post("/orders")
async def create_order(order_id: str, customer_id: str, amount: float):
    event = OrderCreated(order_id, customer_id, amount)
    await bus.publish(event)
    return {"status": "ok"}


@app.post("/orders/{order_id}/pay")
async def pay_order(order_id: str, payment_id: str):
    event = OrderPaid(order_id, payment_id)
    await bus.publish(event)
    return {"status": "OrderPaid event published"}