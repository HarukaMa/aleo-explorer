import struct
from abc import abstractmethod
from decimal import Decimal
from enum import IntEnum

from .utils import *


class Deserialize(metaclass=ABCMeta):

    @abstractmethod
    def load(self, data: BytesIO):
        raise NotImplementedError


class Serialize(metaclass=ABCMeta):

    @abstractmethod
    def dump(self) -> bytes:
        raise NotImplementedError


class Sized(metaclass=ABCMeta):
    @property
    @abstractmethod
    def size(self):
        raise NotImplementedError


class Int(Sized, Serialize, Deserialize, int, metaclass=ABCMeta):

    def __new__(cls, value=0):
        return int.__new__(cls, value)

    def __init__(self, value=0):
        if not isinstance(value, (int, Decimal)):
            raise TypeError("value must be int or Decimal")
        if isinstance(value, Decimal):
            value = int(value)
        if not self.min <= value <= self.max:
            raise OverflowError(f"value {value} out of range for {self.__class__.__name__}")

    @classmethod
    def loads(cls, value: str):
        if not isinstance(value, str):
            raise TypeError("value must be str")
        value = value.replace(cls.__name__, "")
        return cls(int(value))

    def __add__(self, other):
        if type(other) is int:
            return self.__class__(int.__add__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for +: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__add__(self, other))

    @classmethod
    def wrap_value(cls, value):
        if value < 0:
            value &= cls.max
        elif value > cls.max:
            if cls.min == 0:
                value = value & cls.max
            else:
                value = value & ((cls.max << 1) + 1)
                value = (value & cls.max) + cls.min
        return value

    def add_wrapped(self, other):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) + other
        return self.__class__(self.wrap_value(value))


    def __sub__(self, other):
        if type(other) is int:
            return self.__class__(int.__sub__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for -: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__sub__(self, other))

    def sub_wrapped(self, other):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) - other
        return self.__class__(self.wrap_value(value))

    def __mul__(self, other):
        if type(other) is int:
            return self.__class__(int.__mul__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for *: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__mul__(self, other))

    def mul_wrapped(self, other):
        if isinstance(other, Int):
            other = int(other)
        value = int(self) * other
        return self.__class__(self.wrap_value(value))

    def __eq__(self, other):
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
    def __floordiv__(self, other):
        if type(other) is int:
            return self.__class__(int(self / other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for //: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int(self / other))

    def div_wrapped(self, other):
        if isinstance(other, Int):
            other = int(other)
        value = int(int(self) / other)
        return self.__class__(self.wrap_value(value))

    def __lshift__(self, other):
        if type(other) is int:
            return self.__class__(int.__lshift__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for <<: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__lshift__(self, other))

    def __rshift__(self, other):
        if type(other) is int:
            return self.__class__(int.__rshift__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for >>: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__rshift__(self, other))

    def __and__(self, other):
        if type(other) is int:
            return self.__class__(int.__and__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for &: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__and__(self, other))

    def __or__(self, other):
        if type(other) is int:
            return self.__class__(int.__or__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for |: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__or__(self, other))


class IntEnumu8(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<B", self.value)

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        if data.tell() >= data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<B", data.read(1))[0])
        return self


class IntEnumu16(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<H", self.value)

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        if data.tell() + 2 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<H", data.read(2))[0])
        return self


class IntEnumu32(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<I", self.value)

    @classmethod
    # @type_check
    def load(cls, data: BytesIO):
        if data.tell() + 4 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<I", data.read(4))[0])
        return self