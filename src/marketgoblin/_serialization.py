# JSONSerializable — shared to_dict / from_dict mixin for dataclasses
# persisted as JSON. Flat dataclasses use it directly; classes with nested
# dataclass fields inherit to_dict and override from_dict.

from dataclasses import asdict, fields, is_dataclass
from typing import Any, Self


class JSONSerializable:
    """Mixin for dataclasses that round-trip through JSON on disk.

    ``from_dict`` tolerates unknown keys in the input — fields that were
    removed in a newer schema are silently dropped rather than crashing the
    load. All fields on the target dataclass must have defaults for this to
    work when old JSON is missing newer fields.
    """

    def to_dict(self) -> dict[str, Any]:
        if not is_dataclass(self):
            raise TypeError(f"{type(self).__name__} must be a dataclass to use JSONSerializable.")
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        known = {f.name for f in fields(cls)}  # type: ignore[arg-type]
        return cls(**{k: v for k, v in data.items() if k in known})
