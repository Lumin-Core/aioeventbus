import yaml
from pathlib import Path
from aioeventbus.eventbus.brokers.nats import NatsEventBus
from aioeventbus.eventbus.brokers.rabbitmq import RabbitMQEventBus
from aioeventbus.eventbus.brokers.radis import RedisEventBus


def load_config(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def create_event_bus(config_path=None):
    if config_path is None:
        config_path = Path.cwd() / "config.yaml"
    cfg = load_config(config_path)
    broker = cfg["broker"]

    if broker == "rabbitmq":
        return RabbitMQEventBus(
            host=cfg.get("host", "localhost"),
            exchange=cfg.get("exchange", "domain_events"),
            max_workers=cfg.get("max_workers", 10),
        )
    elif broker == "redis":
        return RedisEventBus(
            host=cfg.get("host", "localhost"),
            port=cfg.get("port", 6379),
            channel=cfg.get("channel", "domain_events"),
            max_workers=cfg.get("max_workers", 10),
        )
    elif broker == "nats":
        servers = f"nats://{cfg.get('host', 'localhost')}:{cfg.get('port', 4222)}"
        return NatsEventBus(
            servers=servers,
            subject=cfg.get("subject", "domain_events"),
            max_workers=cfg.get("max_workers", 10),
        )
    else:
        raise ValueError(f"Unknown broker: {broker}")
