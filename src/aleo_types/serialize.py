import inspect
import re
from enum import IntEnum
from io import BytesIO
from typing import TYPE_CHECKING, Protocol, Self, runtime_checkable, Any, cast

if TYPE_CHECKING:
    pass

class Deserialize(Protocol):

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        ...


class Serialize(Protocol):

    def dump(self) -> bytes:
        ...


@runtime_checkable
class Serializable(Serialize, Deserialize, Protocol):
    pass

JSONType = dict[str, Any] | list[Any] | tuple[Any] | str | int | float | bool | None
name_convert_pattern = re.compile(r'(?<!^)(?<![A-Z])(?=[A-Z])')


def enum_name_convert(name: str) -> str:
    return name_convert_pattern.sub('_', name).lower()


@runtime_checkable
class JSONSerialize(Protocol):

    def __default_json(self, compatible: bool = False) -> JSONType:
        """Return a JSON-serializable object."""
        res: dict[str, Any] = {}
        for k, v in self.__dict__.items():
            if not k.startswith("_"):
                if isinstance(v, JSONSerialize):
                    if compatible:
                        res[k] = v.json_compatible()
                    else:
                        res[k] = v.json(compatible)
                elif isinstance(v, dict):
                    v = cast(dict[Any, Any], v)
                    dict_sub = {}
                    for k1, v1 in v.items():
                        if isinstance(v1, IntEnum):
                            dict_sub[str(k1)] = enum_name_convert(v1.name)
                        elif not isinstance(v1, JSONSerialize):
                            raise TypeError(f"cannot serialize {v1.__class__.__name__}")
                        else:
                            if compatible:
                                dict_sub[str(k1)] = v1.json_compatible()
                            else:
                                dict_sub[str(k1)] = v1.json(compatible)
                    res[k] = dict_sub
                elif isinstance(v, (list, tuple)):
                    v = cast(list[Any], v)
                    list_sub: list[JSONType] = []
                    for item in v:
                        if not isinstance(item, JSONSerialize):
                            raise TypeError(f"cannot serialize {item.__class__.__name__}")
                        if compatible:
                            list_sub.append(item.json_compatible())
                        else:
                            list_sub.append(item.json(compatible))
                    res[k] = list_sub
                elif isinstance(v, IntEnum):
                    res[k] = enum_name_convert(v.name)
                else:
                    raise TypeError(f"cannot serialize {v.__class__.__name__}")
        for k, v in self.__class__.__dict__.items():
            if not k.startswith("_") and not inspect.isfunction(v):
                if isinstance(v, IntEnum):
                    res[k] = enum_name_convert(v.name)
        return res

    def json(self, compatible: bool = False) -> JSONType:
        if compatible and self.__class__.json_compatible is not JSONSerialize.json_compatible:
            return self.json_compatible()
        return self.__default_json(compatible)

    def json_compatible(self) -> JSONType:
        if self.__class__.json is not JSONSerialize.json:
            return self.json(compatible=True)
        return self.__default_json(compatible=True)