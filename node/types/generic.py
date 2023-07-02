from collections.abc import Sequence
from copy import deepcopy
from types import UnionType

from .basic import *


class Generic(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, types):
        self.types = types

    def __class_getitem__(cls, item):
        if not isinstance(item, tuple):
            item = item,
        ## Unfortunately we have sized vec, so we can't have this check anymore
        # if not all(isinstance(x, type) or isinstance(x, Generic) for x in item):
        #     raise TypeError("expected type or generic types as generic types")
        return cls(item)


class TypeParameter(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, _):
        raise NotImplementedError

    def __class_getitem__(cls, item):
        if not isinstance(item, tuple):
            item = item,
        return cls(item)


class Tuple(Generic, Serialize, Deserialize, Sequence):

    def __init__(self, types):
        self.types = types

    def __call__(self, value: Sequence):
        if not isinstance(value, Sequence):
            raise TypeError("value must be Sequence")
        if len(value) != len(self.types):
            raise ValueError("value must have the same length as the tuple")
        for i, (t, v) in enumerate(zip(self.types, value)):
            if not isinstance(v, t):
                raise TypeError(f"value[{i}] must be {t}")
        self.value = tuple(value)
        return self

    def __len__(self):
        return len(self.value)

    def __iter__(self):
        return iter(self.value)

    def __getitem__(self, item):
        if not isinstance(item, int):
            raise TypeError("index must be int")
        return self.value[item]

    def dump(self) -> bytes:
        if not all(isinstance(x, Serialize) for x in self.value):
            raise TypeError("value must be serializable")
        return b"".join(x.dump() for x in self.value)

    # @type_check
    def load(self, data: BytesIO):
        if not all(issubclass(x, Deserialize) for x in self.types):
            raise TypeError("value must be deserializable")
        self.value = tuple(t.load(data) for t in self.types)
        return self


class Vec(Generic, Serialize, Deserialize, Sequence):

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
        if not isinstance(value, list):
            raise TypeError("value must be list")
        if isinstance(self.type, type) or isinstance(self.type, UnionType):
            if not all(isinstance(x, self.type) for x in value):
                raise TypeError("value must be of type {}".format(self.type))
        else:
            if not all(isinstance(x, type(self.type)) for x in value):
                raise TypeError("value must be of type {}".format(type(self.type)))
        if hasattr(self, "size") and len(value) != self.size:
            raise ValueError("value must be of size {}".format(self.size))
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
                self._list.append(deepcopy(self.type).load(data))
            else:
                # What else can be here?
                raise TypeError(f"cannot handle type {self.type} in Generic.load")
        return self

    def __iter__(self):
        return iter(self._list)


class VarInt(Generic, Serialize, Deserialize):

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

class Option(Generic, Serialize, Deserialize):

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