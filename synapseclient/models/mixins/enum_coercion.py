"""Mixins and base classes for enum handling in dataclasses."""

from enum import Enum
from typing import Any, ClassVar, Dict, Optional


class EnumCoercionMixin:
    """Mixin for dataclasses that auto-coerces string values to enum types.
    This allows strings or enums to be used interchangeably for the same field.

    Subclasses declare a class-level ``_ENUM_FIELDS`` dict mapping field names
    to their enum classes. On every ``__setattr__`` call the mixin checks
    whether the target field is listed and, if the incoming value is not
    already the correct enum type, coerces it via the enum constructor.

    Example::

        @dataclass
        class MyModel(EnumCoercionMixin):
            _ENUM_FIELDS = {"status": StatusEnum}
            status: Optional[Union[str, StatusEnum]] = None
    """

    _ENUM_FIELDS: ClassVar[Dict[str, type]] = {}

    def __setattr__(self, name: str, value: Any) -> None:
        enum_cls = self._ENUM_FIELDS.get(name)
        if (
            value is not None
            and enum_cls is not None
            and not isinstance(value, enum_cls)
        ):
            value = enum_cls(value)
        super().__setattr__(name, value)


class ForwardCompatibleStrEnum(str, Enum):
    """A string enum that accepts values it does not declare.

    Use this for enums whose values are chosen by the Synapse backend. An
    undeclared value is kept as a new member rather than raising ``ValueError``,
    so a backend that gains a value cannot break deserialization. Comparisons
    against the raw string and against declared members both work, and repeated
    lookups of the same undeclared value return the same member.

    Example::

        class MyState(ForwardCompatibleStrEnum):
            ACTIVE = "ACTIVE"

        MyState("ACTIVE") is MyState.ACTIVE     # True
        MyState("BRAND_NEW") == "BRAND_NEW"     # True, no exception
    """

    @classmethod
    def _missing_(cls, value: object) -> Optional["ForwardCompatibleStrEnum"]:
        if not isinstance(value, str):
            return None
        pseudo_member = str.__new__(cls, value)
        pseudo_member._name_ = value
        pseudo_member._value_ = value
        return cls._value2member_map_.setdefault(value, pseudo_member)
