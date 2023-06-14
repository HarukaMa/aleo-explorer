import struct
from abc import abstractmethod
from decimal import Decimal
from enum import IntEnum

from .utils import *


class Deserialize(metaclass=ABCMeta):

    @abstractmethod
    def load(self, data: bytearray):
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
            raise ValueError("value must be between {} and {}".format(self.min, self.max))

    @classmethod
    def loads(cls, value: int):
        return cls(value)

    def __add__(self, other):
        if type(other) is int:
            return self.__class__(int.__add__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for +: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__add__(self, other))

    def __sub__(self, other):
        if type(other) is int:
            return self.__class__(int.__sub__(self, other))
        if type(other) is not type(self):
            raise TypeError("unsupported operand type(s) for -: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__sub__(self, other))

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




class IntEnumu8(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<B", self.value)

    @classmethod
    # @type_check
    def load(cls, data: bytearray):
        if len(data) < 1:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<B", data[:1])[0])
        del data[:1]
        return self


class IntEnumu16(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<H", self.value)

    @classmethod
    # @type_check
    def load(cls, data: bytearray):
        if len(data) < 2:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<H", data[:2])[0])
        del data[:2]
        return self


class IntEnumu32(Serialize, Deserialize, IntEnum, metaclass=ABCEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<I", self.value)

    @classmethod
    # @type_check
    def load(cls, data: bytearray):
        if len(data) < 4:
            raise ValueError("incorrect length")
        self = cls(struct.unpack("<I", data[:4])[0])
        del data[:4]
        return self