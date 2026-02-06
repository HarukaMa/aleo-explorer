import functools
import sys
from types import GenericAlias
from typing import Generic, TypeVar, TypeVarTuple, TypeGuard, cast, Callable, Hashable

from .basic import *
from .serialize import JSONSerialize


class FixedSize(int, JSONSerialize):
    def __class_getitem__(cls, item: int):
        return FixedSize(item)

    def json(self, compatible: bool = False) -> JSONType:
        return int(self)


T = TypeVar('T', bound=Serializable)
TP = TypeVarTuple('TP')
L = TypeVar('L', bound=Int | FixedSize)
I_co = TypeVar('I_co', bound=Int, covariant=True)

# from cpython 3.11.6
def tp_cache(func: Optional[Callable[..., Any]] = None, /, *, typed: bool = False):
    """Internal wrapper caching __getitem__ of generic types.

    For non-hashable arguments, the original function is used as a fallback.
    """
    def decorator(func: Callable[..., Any]):
        cached = functools.lru_cache(maxsize=None, typed=typed)(func)

        @functools.wraps(func)
        def inner(*args: Hashable, **kwds: Hashable):
            try:
                return cached(*args, **kwds)
            except TypeError:
                pass  # All real errors (not unhashable args) are raised below.
            return func(*args, **kwds)
        return inner

    if func is not None:
        return decorator(func)

    return decorator

def is_serializable(t: Any) -> TypeGuard[TType[Serializable]]:
    if sys.version_info >= (3, 12) and isinstance(t, GenericAlias):
        t = t.__origin__
    return isinstance(t, Serializable)

class Tuple(tuple[*TP], Serializable, JSONSerialize):
    types: tuple[TType[Serializable], ...]

    def __new__(cls, value: tuple[*TP]):
        return tuple.__new__(cls, value)

    @tp_cache
    def __class_getitem__(cls, key: Any | tuple[Any, ...]) -> GenericAlias: # type: ignore
        if not isinstance(key, tuple):
            key = (key,)
        for t in key:
            if not is_serializable(t):
                raise TypeError(f"expected Serializable type, got {t}")
        param_type = type(
            f"Tuple[{', '.join(cast(TType[Serializable], t).__name__ for t in key)}]",
            (Tuple,),
            {"types": key},
        )
        return GenericAlias(param_type, key)

    def dump(self) -> bytes:
        return b"".join(cast(Serializable, t).dump() for t in self)

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        value: list[Serializable] = []
        for t in cls.types:
            value.append(t.load(data))
        return cls(tuple(value)) # type: ignore

    def json(self, compatible: bool = False) -> JSONType:
        res: list[JSONType] = []
        for item in self:
            if not isinstance(item, JSONSerialize):
                raise TypeError(f"cannot serialize {item.__class__.__name__}")
            res.append(item.json(compatible))
        return res


class Vec(list[T], Serializable, JSONSerialize, Generic[T, L]):
    types: tuple[TType[T], TType[L]]

    # noinspection PyMissingConstructor
    def __init__(self, value: list[T]):
        list[T].__init__(self, value)
        self._type = self.types[0]
        if isinstance(self.types[1], FixedSize):
            self._size = self.types[1]
        else:
            self._size_type = self.types[1]
            self._size = self._size_type(len(value))

    @tp_cache
    def __class_getitem__(cls, key: Any | tuple[Any, ...]) -> GenericAlias: # type: ignore
        if not isinstance(key, tuple):
            raise TypeError("expected type and size")
        if len(key) != 2:
            raise TypeError("expected type and size")
        var_type, size_type = key
        if not is_serializable(var_type):
            raise TypeError(f"expected Serializable type, got {var_type}")
        if not (isinstance(size_type, FixedSize) or issubclass(size_type, Int)):
            raise TypeError(f"expected Int or FixedSize type, got {size_type}")
        if isinstance(size_type, int):
            class_name = f"Vec[{var_type.__name__}, {size_type}]"
        else:
            class_name = f"Vec[{var_type.__name__}, {size_type.__name__}]"
        param_type = type(
            class_name,
            (Vec,),
            {"types": key},
        )
        return GenericAlias(param_type, key)

    def dump(self) -> bytes:
        res = b""
        if isinstance(self._size, Int):
            res += self._size.dump()
        for item in self:
            res += item.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        value_type, size_type = cls.types
        if isinstance(size_type, FixedSize):
            size = size_type
        else:
            size = cast(Int, size_type).load(data)
        return cls(list(value_type.load(data) for _ in range(size)))

    def json(self, compatible: bool = False) -> JSONType:
        if isinstance(self._type, GenericAlias) and issubclass(self._type.__origin__, Tuple):
            res1: dict[str, Any] = {}
            for tup in self:
                if not isinstance(tup, Tuple):
                    raise TypeError(f"bad type in Vec: {tup.__class__.__name__}, expected {self._type.__name__}")
                if len(tup) == 2:
                    res1[str(tup[0])] = tup[1].json(compatible)
                else:
                    obj: list[JSONType] = []
                    for item in tup[1:]:
                        if not isinstance(item, JSONSerialize):
                            raise TypeError(f"cannot serialize {item.__class__.__name__}")
                        obj.append(item.json(compatible))
                    res1[str(tup[0])] = obj
            return res1
        res: list[JSONType] = []
        for item in self:
            if not isinstance(item, JSONSerialize):
                raise TypeError(f"cannot serialize {item.__class__.__name__}")
            res.append(item.json(compatible))
        return res

    def __str__(self):
        return f"[{', '.join(str(item) for item in self)}]"


class Option(Serializable, JSONSerialize, Generic[T]):
    types: TType[T]

    def __init__(self, value: Optional[T]):
        self.value = value

    @tp_cache
    def __class_getitem__(cls, key: Any | tuple[Any, ...]) -> GenericAlias:
        if isinstance(key, tuple):
            raise TypeError("expected one type")
        param_type = type(
            f"Option[{key.__name__}]",
            (Option,),
            {"types": key},
        )
        return GenericAlias(param_type, (key,))

    def dump(self) -> bytes:
        if self.value is None:
            return b"\x00"
        else:
            return b"\x01" + self.value.dump()

    def dumps(self) -> str | None:
        if self.value is None:
            return None
        else:
            return str(self.value)

    def dump_nullable(self):
        if self.value is None:
            return None
        else:
            return self.value.dump()

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        is_some = bool_.load(data)
        if is_some:
            value = cls.types.load(data)
        else:
            value = None
        return cls(value)

    def json(self, compatible: bool = False) -> JSONType:
        if self.value is None:
            return None
        if not isinstance(self.value, JSONSerialize):
            raise TypeError(f"cannot serialize {self.types.__name__}")
        return self.value.json(compatible)
