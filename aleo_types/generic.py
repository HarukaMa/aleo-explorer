import functools
from types import GenericAlias
from typing import Generic, TypeVar, Optional, TypeVarTuple, TypeGuard, cast, Callable, Hashable

from .basic import *


class FixedSize(int):
    def __class_getitem__(cls, item: int):
        return FixedSize(item)


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
    return issubclass(t, Serializable)

class Tuple(tuple[*TP], Serializable):
    types: tuple[TType[T], ...]

    def __new__(cls, value: tuple[*TP]):
        return tuple.__new__(cls, value)

    @tp_cache
    def __class_getitem__(cls, key) -> GenericAlias:
        if not isinstance(key, tuple):
            key = (key,)
        param_type = type(
            f"Tuple[{', '.join(t.__name__ for t in key)}]",
            (Tuple,),
            {"types": key},
        )
        return GenericAlias(param_type, key)

    def dump(self) -> bytes:
        return b"".join(t.dump() for t in self if is_serializable(t))

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        if cls.types is None:
            raise TypeError("expected types")
        value: list[Serializable] = []
        for t in cls.types:
            if not is_serializable(t):
                raise TypeError(f"expected Serializable type, got {t}")
            value.append(t.load(data))
        return cls(tuple(value)) # type: ignore


class Vec(list[T], Serializable, Generic[T, L]):
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
    def __class_getitem__(cls, key) -> GenericAlias: # type: ignore
        if not isinstance(key, tuple):
            raise TypeError("expected tuple")
        if isinstance(key[1], int):
            class_name = f"Vec[{key[0].__name__}, {key[1]}]"
        else:
            class_name = f"Vec[{key[0].__name__}, {key[1].__name__}]"
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
        if cls.types is None:
            raise TypeError("expected types")
        value_type, size_type = cls.types
        if isinstance(size_type, FixedSize):
            size = size_type
        else:
            size = size_type.load(data)
        return cls(list(value_type.load(data) for _ in range(size)))

    def __str__(self):
        return f"[{', '.join(str(item) for item in self)}]"


class Option(Serializable, Generic[T]):
    types: TType[T]

    def __init__(self, value: Optional[T]):
        self.value = value

    @tp_cache
    def __class_getitem__(cls, key) -> GenericAlias:
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
        if cls.types is None:
            raise TypeError("expected types")
        is_some = bool_.load(data)
        if is_some:
            value = cls.types[0].load(data)
        else:
            value = None
        return cls(value)
