import struct
from decimal import Decimal
from enum import IntEnum
from typing import Protocol, runtime_checkable, Self, Type

from .utils import *


@runtime_checkable
class Deserialize(Protocol):

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        ...


@runtime_checkable
class Serialize(Protocol):

    def dump(self) -> bytes:
        ...


@runtime_checkable
class Serializable(Serialize, Deserialize, Protocol):
    pass


@runtime_checkable
class Sized(Protocol):
    size: int

@runtime_checkable
class Comparable(Protocol):
    def __eq__(self, other: Any) -> bool:
        ...

    def __lt__(self, other: Any) -> bool:
        ...

    def __gt__(self, other: Any) -> bool:
        ...

    def __le__(self, other: Any) -> bool:
        ...

    def __ge__(self, other: Any) -> bool:
        ...


class Add(Protocol):
    def __add__(self, other: Any) -> Self:
        ...

class Sub(Protocol):
    def __sub__(self, other: Any) -> Self:
        ...

class Mul(Protocol):
    def __mul__(self, other: Any) -> Self:
        ...

class Div(Protocol):
    def __div__(self, other: Any) -> Self:
        ...

class IntProtocol(Sized, Protocol):
    min: int
    max: int

class Int(int, Serializable, IntProtocol):
    size = -1
    min = 2**256
    max = -2**256

    def __new__(cls, value: int | Decimal = 0):
        if isinstance(value, Decimal):
            value = int(value)
        if not cls.min <= value <= cls.max:
            raise OverflowError(f"value {value} out of range for {cls.__name__}")
        return int.__new__(cls, value)

    @classmethod
    def loads(cls, value: str):
        value = value.replace(cls.__name__, "")
        return cls(int(value))

    def __add__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__add__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for +: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__add__(self, other))

    @classmethod
    def wrap_value(cls, value: int):
        if value < 0:
            value &= cls.max
        elif value > cls.max:
            if cls.min == 0:
                value = value & cls.max
            else:
                value = value & ((cls.max << 1) + 1)
                value = (value & cls.max) + cls.min
        return value

    def add_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) + other
        return self.__class__(self.wrap_value(value))


    def __sub__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__sub__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for -: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__sub__(self, other))

    def sub_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) - other
        return self.__class__(self.wrap_value(value))

    def __mul__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__mul__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for *: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__mul__(self, other))

    def mul_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) * other
        return self.__class__(self.wrap_value(value))

    def __eq__(self, other: object):
        if type(other) is int:
            return int.__eq__(self, other)
        if type(other) is not type(self):
            return False
        return int.__eq__(self, other)

    def __hash__(self):
        return int.__hash__(self)

    def __invert__(self):
        if self.min == 0:
            return self.__class__(~int(self) & self.max)
        return self.__class__(~int(self))

    # we are deviating from python's insane behavior here
    # this is actually __truncdiv__
    def __floordiv__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int(self / other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for //: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int(self / other))

    def div_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        value = int(int(self) / other)
        return self.__class__(self.wrap_value(value))

    def __lshift__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__lshift__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for <<: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__lshift__(self, other))

    def __rshift__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__rshift__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for >>: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__rshift__(self, other))

    def __and__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__and__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for &: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__and__(self, other))

    def __or__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__or__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for |: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__or__(self, other))

    def dump(self) -> bytes:
        raise TypeError("cannot deserialize Int base class")

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        raise TypeError("cannot serialize Int base class")


class IntEnumu8(Serializable, IntEnum, metaclass=ProtocolEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<B", self.value)

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<B", data.read(1))[0])
        return self


class IntEnumu16(Serializable, IntEnum, metaclass=ProtocolEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<H", self.value)

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 2 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<H", data.read(2))[0])
        return self


class IntEnumu32(Serializable, IntEnum, metaclass=ProtocolEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<I", self.value)

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 4 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<I", data.read(4))[0])
        return self


class RustEnum(Protocol):
    Type: Type[IntEnum]

class EnumBaseSerialize(Serialize):

    def dump(self) -> bytes:
        raise TypeError("cannot serialize base class")
