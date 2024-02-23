"""Base class to hold data specific to Synapse entities."""

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Union


class DotDict(dict):
    """A dictionary that supports dot notation as well as bracket notation for access."""

    def __getattr__(
        self, attr
    ) -> Dict[
        str,
        Union[
            List[str],
            List[bool],
            List[float],
            List[int],
            List[date],
            List[datetime],
        ],
    ]:
        return self.get(attr)

    def __setattr__(self, key, value) -> None:
        if not isinstance(value, list):
            value = [value]
        self.__setitem__(key, value)

    def __deepcopy__(self, memo) -> "DotDict":
        return DotDict(deepcopy(dict(self), memo))

    def __delattr__(self, key) -> None:
        if key in self:
            del self[key]

    def __eq__(self, other: Union["DotDict", Dict]) -> bool:
        if isinstance(other, DotDict):
            return dict(self) == dict(other)
        elif isinstance(other, dict):
            return dict(self) == other
        return False


@dataclass()
class Entity:
    """Base class to hold data specific to Synapse entities.

    Attributes:
        annotations: Additional metadata associated with the entity. The key is the
            name of your desired annotations. The value is a list of values. To remove
            all annotations delete all keys or set this to an empty dict `{}` after
            you've retrieved the entity.
    """

    _annotations: DotDict = field(default_factory=DotDict, compare=False)

    @property
    def annotations(
        self,
    ) -> Dict[
        str,
        Union[
            List[str],
            List[bool],
            List[float],
            List[int],
            List[date],
            List[datetime],
        ],
    ]:
        """Additional metadata associated with the entity. The key is the
        name of your desired annotations. The value is a list of values. To remove
        all annotations delete all keys or set this to an empty dict `{}` after you've
        retrieved the entity.
        """
        return self._annotations

    @annotations.setter
    def annotations(
        self,
        value: Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ],
    ) -> None:
        if value is None:
            self._annotations = DotDict()
        elif not isinstance(value, DotDict):
            self._annotations = DotDict(value)
        else:
            self._annotations = value
