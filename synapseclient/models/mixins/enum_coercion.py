"""Mixin for automatic enum coercion in dataclasses."""

from typing import Any, ClassVar, Dict


class EnumCoercionMixin:
    """Mixin for dataclasses that auto-coerces string values to enum types.

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
