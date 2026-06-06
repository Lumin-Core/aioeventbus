import argparse
import yaml
from pathlib import Path

TEMPLATES = {
    "rabbitmq": {
        "broker": "rabbitmq",
        "host": "localhost",
        "port": 5672,
        "exchange": "domain_events",
        "max_workers": 10,
        "durable": False,
    },
    "redis": {
        "broker": "redis",
        "host": "localhost",
        "port": 6379,
        "channel": "domain_events",
        "max_workers": 10,
    },
    "nats": {
        "broker": "nats",
        "host": "localhost",
        "port": 4222,
        "subject": "domain_events",
        "max_workers": 10,
    },
}


def main():
    parser = argparse.ArgumentParser(description="Generate EventBus YAML config")
    parser.add_argument(
        "--broker",
        required=True,
        choices=["rabbitmq", "redis", "nats"],
        help="Message broker type",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("eventbus_config.yaml"),
        help="Output YAML file path (default: eventbus_config.yaml)",
    )
    parser.add_argument("--host", help="Override broker host")
    parser.add_argument("--port", type=int, help="Override broker port")
    parser.add_argument("--exchange", help="Exchange name (RabbitMQ only)")
    parser.add_argument("--channel", help="Channel name (Redis only)")
    parser.add_argument("--subject", help="Subject name (NATS only)")
    parser.add_argument("--max-workers", type=int, help="Thread pool size")

    args = parser.parse_args()

    config = TEMPLATES[args.broker].copy()

    if args.host:
        config["host"] = args.host
    if args.port:
        config["port"] = args.port
    if args.max_workers:
        config["max_workers"] = args.max_workers

    if args.broker == "rabbitmq" and args.exchange:
        config["exchange"] = args.exchange
    elif args.broker == "redis" and args.channel:
        config["channel"] = args.channel
    elif args.broker == "nats" and args.subject:
        config["subject"] = args.subject

    if args.output.exists():
        overwrite = input(f"{args.output} already exists. Overwrite? (y/N): ").strip().lower()
        if overwrite != "y":
            print("Aborted.")
            return

    with open(args.output, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)

    print(f"✅ Config file created: {args.output}")
    print("Edit it as needed.")


if __name__ == "__main__":
    main()
