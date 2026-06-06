from dataclasses import dataclass
from aioeventbus.eventbus.events import DomainEvent


@dataclass
class OrderCreated(DomainEvent):
    __event_type__ = "OrderCreated"

    order_id: str
    customer_id: str
    amount: float


@dataclass
class OrderPaid(DomainEvent):
    __event_type__ = "OrderPaid"

    order_id: str
    payment_id: str
