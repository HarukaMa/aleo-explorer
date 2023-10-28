import math
import socket
import struct
from decimal import Decimal

from .traits import *


class Bech32m:

    def __init__(self, data: bytes, prefix: str):
        self.data = data
        self.prefix = prefix

    def __str__(self):
        return aleo_explorer_rust.bech32_encode(self.prefix, self.data)

    def __repr__(self):
        return str(self)


class IntProtocol(Sized, Compare, AddWrapped, SubWrapped, MulWrapped, DivWrapped, And, Or, Xor, Not, ShlWrapped, ShrWrapped, RemWrapped, PowWrapped, Protocol):
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
        if value < cls.min:
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
        if other == 0:
            raise ZeroDivisionError("division by zero")
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

    def shl_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        return self.__class__(self.wrap_value(int.__lshift__(self, other)))

    def __rshift__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__rshift__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for >>: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__rshift__(self, other))

    def shr_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        return self.__class__(self.wrap_value(int.__rshift__(self, other)))

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

    def __xor__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__xor__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for ^: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__xor__(self, other))

    def __mod__(self, other: int | Self):
        if type(other) is int:
            return self.__class__(int.__mod__(self, other))
        if not issubclass(type(other), Int):
            raise TypeError("unsupported operand type(s) for %: '{}' and '{}'".format(type(self), type(other)))
        return self.__class__(int.__mod__(self, other))

    def rem_wrapped(self, other: int | Self):
        return self.__mod__(other)

    def __pow__(self, power: int | Self):
        if isinstance(power, Int) and not isinstance(power, (u8, u16, u32)):
            raise TypeError(f"unsupported operand type(s) for **: '{type(self)}' and '{type(power)}'")
        power = int(power)
        max_digits = math.ceil(math.log10(self.max))
        res_digits = math.log10(abs(self)) * power
        if res_digits > max_digits:
            raise OverflowError(f"value *too large* is out of range for {self.__class__.__name__}")
        return self.__class__(int.__pow__(self, power))

    def pow_wrapped(self, other: int | Self):
        if isinstance(other, Int):
            other = int(other)
        if self.min == 0:
            return self.__class__(int.__pow__(self, other, self.max + 1))
        else:
            if self < 0 and other % 2 == 0:
                return self.__class__(int.__pow__(self, other, self.max + 1))
            return self.__class__(int.__pow__(self, other, (self.max + 1) * 2) + (self.min * 2))

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
        self = cls(struct.unpack("<B", data.read(1))[0])
        return self


class IntEnumu16(Serializable, IntEnum, metaclass=ProtocolEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<H", self.value)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<H", data.read(2))[0])
        return self


class IntEnumu32(Serializable, IntEnum, metaclass=ProtocolEnumMeta):

    def dump(self) -> bytes:
        return struct.pack("<I", self.value)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<I", data.read(4))[0])
        return self


class u8(Int, Mod):
    size = 1
    min = 0
    max = 255

    def dump(self) -> bytes:
        return struct.pack("<B", self)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<B", data.read(1))[0])
        return self


class u16(Int, Mod):
    size = 2
    min = 0
    max = 65535

    def dump(self) -> bytes:
        return struct.pack("<H", self)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<H", data.read(2))[0])
        return self


class u32(Int, Mod):
    size = 4
    min = 0
    max = 4294967295

    def dump(self) -> bytes:
        return struct.pack("<I", self)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<I", data.read(4))[0])
        return self


class u64(Int, Mod):
    size = 8
    min = 0
    max = 18446744073709551615

    def dump(self) -> bytes:
        return struct.pack("<Q", self)

    @classmethod
    def load(cls, data: BytesIO):
        self = cls(struct.unpack("<Q", data.read(8))[0])
        return self

# Obviously we only support 64bit
usize = u64

class u128(Int, Mod):
    size = 16
    min = 0
    max = 340282366920938463463374607431768211455

    def dump(self) -> bytes:
        return struct.pack("<QQ", self & 0xFFFF_FFFF_FFFF_FFFF, self >> 64)

    @classmethod
    def load(cls, data: BytesIO):
        lo, hi = struct.unpack("<QQ", data.read(16))
        self = cls((hi << 64) | lo)
        return self


class i8(Int, AbsWrapped, Neg):
    size = 1
    min = -128
    max = 127

    def dump(self) -> bytes:
        return struct.pack("<b", self)

    @classmethod
    def load(cls, data: BytesIO):
        return cls(struct.unpack("<b", data.read(1))[0])

    def __abs__(self):
        return i8(abs(int(self)))

    def abs_wrapped(self):
        return self.__class__(self.wrap_value(abs(int(self))))

    def __neg__(self):
        return i8(-int(self))


class i16(Int, AbsWrapped, Neg):
    size = 2
    min = -32768
    max = 32767

    def dump(self) -> bytes:
        return struct.pack("<h", self)

    @classmethod
    def load(cls, data: BytesIO):
        return cls(struct.unpack("<h", data.read(2))[0])

    def __abs__(self):
        return i16(abs(int(self)))

    def abs_wrapped(self):
        return self.__class__(self.wrap_value(abs(int(self))))

    def __neg__(self):
        return i16(-int(self))


class i32(Int, AbsWrapped, Neg):
    size = 4
    min = -2147483648
    max = 2147483647

    def dump(self) -> bytes:
        return struct.pack("<i", self)

    @classmethod
    def load(cls, data: BytesIO):
        return cls(struct.unpack("<i", data.read(4))[0])

    def __abs__(self):
        return i32(abs(int(self)))

    def abs_wrapped(self):
        return self.__class__(self.wrap_value(abs(int(self))))

    def __neg__(self):
        return i32(-int(self))


class i64(Int, AbsWrapped, Neg):
    size = 8
    min = -9223372036854775808
    max = 9223372036854775807

    def dump(self) -> bytes:
        return struct.pack("<q", self)

    @classmethod
    def load(cls, data: BytesIO):
        return cls(struct.unpack("<q", data.read(8))[0])

    def __abs__(self):
        return i64(abs(int(self)))

    def abs_wrapped(self):
        return self.__class__(self.wrap_value(abs(int(self))))

    def __neg__(self):
        return i64(-int(self))


class i128(Int, AbsWrapped, Neg):
    size = 16
    min = -170141183460469231731687303715884105728
    max = 170141183460469231731687303715884105727

    def dump(self) -> bytes:
        return self.to_bytes(16, "little", signed=True)

    @classmethod
    def load(cls, data: BytesIO):
        return cls(int.from_bytes(data.read(16), "little", signed=True))

    def __abs__(self):
        return i128(abs(int(self)))

    def abs_wrapped(self):
        return self.__class__(self.wrap_value(abs(int(self))))

    def __neg__(self):
        return i128(-int(self))


class bool_(Sized, Serializable, And, Or, Not, Xor, Nand, Nor):

    size = 1

    def __init__(self, value: bool = False):
        self.value = value

    def dump(self) -> bytes:
        return struct.pack("<B", 1 if self else 0)

    @classmethod
    def load(cls, data: BytesIO):
        value = data.read(1)[0]
        if value == 0:
            value = False
        elif value == 1:
            value = True
        else:
            breakpoint()
            raise ValueError("invalid value for bool")
        self = cls(value)
        return self

    @classmethod
    def loads(cls, value: str):
        if value.lower() == "true":
            return cls(True)
        if value.lower() == "false":
            return cls()
        raise ValueError("invalid value for bool")

    def __str__(self):
        if self:
            return "true"
        return "false"

    def __repr__(self):
        if self:
            return "True"
        return "False"

    def __invert__(self):
        return bool_(not self)

    def __and__(self, other: bool | Self):
        if isinstance(other, bool):
            return bool_(self.value and other)
        return bool_(self.value and other.value)

    def __or__(self, other: bool | Self):
        if isinstance(other, bool):
            return bool_(self.value or other)
        return bool_(self.value or other.value)

    def __xor__(self, other: bool | Self):
        if isinstance(other, bool):
            return bool_(self.value ^ other)
        return bool_(self.value ^ other.value)

    def nand(self, other: bool | Self) -> Self:
        if isinstance(other, bool):
            return bool_(not (self.value and other))
        return bool_(not (self.value and other.value))

    def nor(self, other: bool | Self) -> Self:
        if isinstance(other, bool):
            return bool_(not (self.value or other))
        return bool_(not (self.value or other.value))

    def __bool__(self):
        return self.value

    def __eq__(self, other: object):
        if isinstance(other, bool):
            return self.value == other
        if isinstance(other, bool_):
            return self.value == other.value
        return False

    __match_args__ = ("value",)



class SocketAddr(Serializable):
    def __init__(self, *, ip: int, port: int):
        if ip < 0 or ip > 4294967295:
            raise ValueError("ip must be between 0 and 4294967295")
        if port < 0 or port > 65535:
            raise ValueError("port must be between 0 and 65535")
        self.ip = ip
        self.port = port

    def dump(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def load(cls, data: BytesIO):
        data.read(4)
        ip = u32.load(data)
        port = u16.load(data)
        return cls(ip=ip, port=port)

    def __str__(self):
        return ":".join(self.ip_port())

    def ip_port(self):
        return socket.inet_ntoa(struct.pack('<L', self.ip)), str(self.port)