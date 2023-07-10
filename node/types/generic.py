from functools import partial, lru_cache
from types import GenericAlias, MethodType
from typing import Generic, TypeVar, get_args, Optional, Callable

from .basic import *

T = TypeVar('T', bound=Serializable)
L = TypeVar('L', bound=Type[Int] | int)
I_co = TypeVar('I_co', bound=Int, covariant=True)


class TypedGenericAlias(GenericAlias):
    def __getattribute__(self, item: str):
        attr = super().__getattribute__(item)
        if isinstance(attr, MethodType):
            return partial(attr, types=get_args(self))
        return attr

    def __call__(self, *args: Any, **kwargs: Any):
        kwargs["types"] = get_args(self)
        return super().__call__(*args, **kwargs)

def access_generic_type(c: Any):

    def _tp_cache(func: Callable[..., Any], *, max_size: int | None = None):
        cache = lru_cache(max_size)(func)

        def wrapper(*args: Any, **kwargs: Any):
            try:
                return cache(*args, **kwargs)
            except TypeError:
                return func(*args, **kwargs)

        return wrapper

    # noinspection PyUnresolvedReferences
    def __class_getitem__(cls: Any, item: str):
        if not (hasattr(super(cls, cls), "__class_getitem__") and callable(super(cls, cls).__class_getitem__)): # type: ignore
            raise TypeError
        __generic_alias = super(cls, cls).__class_getitem__(item) # type: ignore
        args = get_args(__generic_alias)
        if not args:
            raise TypeError
        return TypedGenericAlias(cls, get_args(__generic_alias))

    def inject_types(f: Callable[..., Any]):
        def wrapper(self: Any, *args: Any, **kwargs: Any):
            if f is object.__new__:
                return f(self)
            if "types" in kwargs:
                types = kwargs.pop("types")
                self.types = types
            return f(self, *args, **kwargs)
        return wrapper

    c.__class_getitem__ = _tp_cache(__class_getitem__.__get__(c), max_size=1024)
    c.__new__ = inject_types(c.__new__)
    c.__init__ = inject_types(c.__init__)
    return c


# noinspection PyTypeHints
@access_generic_type
class Tuple(tuple[T, ...], Serializable):
    types: tuple[Type[T], ...]

    def __new__(cls, value: tuple[T, ...]) -> Self:
        return tuple.__new__(cls, value)

    def __init__(self, _): # type: ignore[reportInconsistentConstructor]
        pass

    def dump(self) -> bytes:
        # noinspection PyTypeChecker
        return b"".join(t.dump() for t in self)

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[Type[T], ...]] = None) -> Self:
        if types is None:
            raise TypeError("expected types")
        value = tuple(t.load(data) for t in types)
        return cls[*types](value)


@access_generic_type
class Vec(list[T], Serializable, Generic[T, L]):
    types: tuple[Type[T], L]

    # noinspection PyMissingConstructor
    def __init__(self, value: list[T]):
        list[T].__init__(self, value)
        self.type = self.types[0]
        print(self.types)
        if isinstance(self.types[1], int):
            self.size = self.types[1]
        else:
            self.size_type = self.types[1]
            self.size = self.size_type(len(value))

    def dump(self) -> bytes:
        res = b""
        if isinstance(self.size, Int):
            res += self.size.dump()
        for item in self:
            res += item.dump()
        return res

    @classmethod
    def load(cls, data: BytesIO, *, types: Optional[tuple[type[T], L]] = None) -> Self:
        if types is None:
            raise TypeError("expected types")
        value_type, size_type = types
        if isinstance(size_type, int):
            size = size_type
        else:
            size = size_type.load(data)
        return cls[*types](list(value_type.load(data) for _ in range(size)))


class Option(Serializable, Generic[T]):
    types: tuple[Type[T]]

    def __init__(self, value: Optional[T]):
        self.value = value

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
    def load(cls, data: BytesIO, *, types: Optional[tuple[Type[T]]] = None) -> Self:
        if types is None:
            raise TypeError("expected types")
        is_some = bool_.load(data)
        if is_some:
            value = types[0].load(data)
        else:
            value = None
        return cls[*types](value)