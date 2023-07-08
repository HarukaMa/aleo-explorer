import typing
from types import UnionType, GenericAlias
from typing import Generic, TypeVar

from .basic import *

T = TypeVar('T')
L = TypeVar('L', bound=type|int)


class TypedGenericAlias(GenericAlias):
    def __call__(self, *args, **kwargs):
        kwargs["types"] = typing.get_args(self)
        return super().__call__(*args, **kwargs)

def access_generic_type(c):
    def __class_getitem__(cls, item):
        if not (hasattr(super(cls, cls), "__class_getitem__") and callable(super(cls, cls).__class_getitem__)):
            raise TypeError
        __generic_alias = super(cls, cls).__class_getitem__(item)
        if not isinstance(__generic_alias, GenericAlias):
            raise TypeError
        return TypedGenericAlias(cls, typing.get_args(__generic_alias))

    def inject_types(f):
        def wrapper(self, *args, **kwargs):
            if "types" in kwargs:
                types = kwargs.pop("types")
                self.types = types
            return f(self, *args, **kwargs)
        return wrapper

    c.__class_getitem__ = __class_getitem__.__get__(c)
    c.__init__ = inject_types(c.__init__)
    return c

@access_generic_type
class Tuple(Serialize, Deserialize, tuple[T]):

    def __init__(self, value: tuple[T]):
        super().__init__(value)

    def dump(self) -> bytes:
        if not all(isinstance(x, Serialize) for x in self):
            raise TypeError("value must be serializable")
        return b"".join(x.dump() for x in self)

    # @type_check
    def load(self, data: BytesIO):
        types = self.__orig_class__.__args__
        if not all(issubclass(x, Deserialize) for x in types):
            raise TypeError("value must be deserializable")
        self.value = tuple(t.load(data) for t in types)
        return self


class Vec(Serialize, Deserialize, list[T], Generic[T, L]):

    def __init__(self, types):
        if len(types) != 2:
            raise TypeError("expected 2 types for Vec")
        self.type = types[0]
        if isinstance(types[1], int):
            self.size = types[1]
        elif issubclass(types[1], Int):
            self.size_type = types[1]
        else:
            raise TypeError("expected int or Int as size type")
        super().__init__(types)

    def __call__(self, value):
        self._list = value
        if hasattr(self, "size_type"):
            self.size = len(value)
        return self

    def __len__(self):
        return len(self._list)

    def __getitem__(self, index):
        if isinstance(index, int) or isinstance(index, slice):
            return self._list[index]
        else:
            raise TypeError("index must be int or slice")

    def __setitem__(self, index, value):
        if isinstance(index, int):
            if isinstance(self.type, type) or isinstance(self.type, UnionType):
                if not isinstance(value, self.type):
                    raise TypeError("value must be of type {}".format(self.type))
            else:
                if not isinstance(value, type(self.type)):
                    raise TypeError("value must be of type {}".format(type(self.type)))
            self._list[index] = value
        else:
            raise TypeError("index must be int")

    def dump(self) -> bytes:
        res = b""
        if hasattr(self, "size_type"):
            res += self.size_type.dump(self.size)
        for item in self._list:
            res += item.dump()
        return res

    # @type_check
    def load(self, data: BytesIO):
        if isinstance(self.type, type):
            if not issubclass(self.type, Deserialize):
                raise TypeError(f"{self.type.__name__} must be Deserialize")
        else:
            if not issubclass(type(self.type), Deserialize):
                raise TypeError(f"{type(self.type).__name__} must be Deserialize")
        if hasattr(self, "size_type"):
            if data.tell() + self.size_type.size > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            self.size = self.size_type.load(data)
        self._list = []
        for i in range(self.size):
            if isinstance(self.type, type):
                self._list.append(self.type.load(data))
            elif isinstance(self.type, Generic):
                # Python version 3.10 does not support starred expressions in subscriptions
                self._list.append(self.type.__class__[*self.type.types].load(data))
            else:
                # What else can be here?
                raise TypeError(f"cannot handle type {self.type} in Generic.load")
        return self

    def __iter__(self):
        return iter(self._list)


class VarInt(Generic[T], Serialize, Deserialize):

    def __init__(self, types):
        if len(types) != 1:
            raise TypeError("expected 1 type for VarInt")
        self.type = types[0]
        super().__init__(types)

    def __call__(self, value):
        if not isinstance(value, self.type):
            raise TypeError("value must be of type {}".format(self.type))
        self.value = value
        return self

    def dump(self) -> bytes:
        if 0 <= self.value <= 0xfc:
            return self.value.to_bytes(1, "little")
        elif 0xfd <= self.value <= 0xffff:
            return b"\xfd" + self.value.to_bytes(2, "little")
        elif 0x10000 <= self.value <= 0xffffffff:
            return b"\xfe" + self.value.to_bytes(4, "little")
        elif 0x100000000 <= self.value <= 0xffffffffffffffff:
            return b"\xff" + self.value.to_bytes(8, "little")
        else:
            raise ValueError("unreachable")

    # @type_check
    def load(self, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("data is too short")
        value = data.read(1)[0]
        if value == 0xfd:
            if data.tell() + 2 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            self.value = u16.load(data)
        elif value == 0xfe:
            if data.tell() + 4 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            self.value = u32.load(data)
        elif value == 0xff:
            if data.tell() + 8 > data.getbuffer().nbytes:
                raise ValueError("data is too short")
            self.value = u64.load(data)
        else:
            self.value = u8(value)
        self.value = self.type(self.value)
        return self

    def __str__(self):
        return str(self.value)

    def __int__(self):
        return int(self.value)

class Option(Generic[T], Serialize, Deserialize):

    def __init__(self, types):
        if len(types) != 1:
            raise TypeError("expected 1 type for Option")
        self.type = types[0]
        super().__init__(types)

    def __call__(self, value):
        if value is None:
            self.value = None
        elif issubclass(type(self.type), Generic):
            if isinstance(self.type, Vec):
                if value.type != self.type.type and not (isinstance(value.type, Tuple) and isinstance(self.type.type, Tuple)):
                    raise TypeError(f"value should be {self.type}, but got {type(value)}")
            if isinstance(self.type, Tuple):
                if value.types != self.type.types:
                    raise TypeError(f"value should be {self.type}, but got {type(value)}")
        elif not isinstance(value, self.type):
            raise TypeError("value must be of type {} or None".format(self.type))
        self.value = value
        return self

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

    # @type_check
    def load(self, data: BytesIO):
        is_some = bool_.load(data)
        if is_some:
            self.value = self.type.load(data)
        else:
            self.value = None
        return self

def generic_type_check(func):
    def wrapper(*args, **kwargs):
        hints = get_type_hints(func)
        for v, t in hints.items():
            arg = kwargs.get(v)
            if isinstance(t, Vec):
                if arg.type != t.type and not (isinstance(arg.type, Tuple) and isinstance(t.type, Tuple)) and not (isinstance(arg.type, Vec) and isinstance(t.type, Vec)):
                    raise TypeError(f"{v} should be {t}, but got {type(arg)}")
            if isinstance(t, Tuple):
                if arg.types != t.types:
                    raise TypeError(f"{v} should be {t}, but got {type(arg)}")
        return func(*args, **kwargs)

    return wrapper