from aioeventbus.eventbus.factory import create_event_bus
from aioeventbus.examples.fastapi_app.events import OrderCreated, OrderPaid

bus = create_event_bus()


@bus.handler(OrderCreated)
def notify_admin(event: OrderCreated):
    print(f"[ADMIN] New order: {event.order_id} from {event.customer_id}")


@bus.handler(OrderCreated)
def start_fulfillment(event: OrderCreated):
    print(f"[FULFILMENT] Start fulfillment of order: {event.order_id}")


@bus.handler(OrderPaid)
def send_recipient(event: OrderPaid):
    print(f"[BILLING] Sending payment check: {event.payment_id}")
