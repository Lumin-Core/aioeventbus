from dataclasses import asdict
from typing import Any


class DomainEvent:
    __event_type__: str

    @property
    def event_type(self) -> str:
        return self.__event_type__

    def to_dict(self) -> dict[str, Any]:
        return {"event_type": self.event_type, "data": asdict(self)}
