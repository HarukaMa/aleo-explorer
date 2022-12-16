import struct
from abc import abstractmethod
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

    @abstractmethod
    def __init__(self, _):
        raise NotImplementedError


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