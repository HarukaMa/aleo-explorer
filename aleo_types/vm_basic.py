from .generic import *


class AleoIDProtocol(Sized, Serializable, Protocol):
    size: int
    _prefix: str

class AleoID(AleoIDProtocol):
    size = 32
    _prefix = ""

    def __init__(self, data: bytes):
        if len(self._prefix) != 2:
            raise ValueError("locator_prefix must be 2 bytes")
        self._data = data
        self._bech32m = Bech32m(data, self._prefix)

    def dump(self) -> bytes:
        return self._data

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + cls.size > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        size = cls.size
        self = cls(data.read(size))
        return self

    @classmethod
    def loads(cls, data: str):
        hrp, raw = aleo.bech32_decode(data)
        if hrp != cls._prefix:
            raise ValueError("incorrect hrp")
        if len(raw) != cls.size:
            raise ValueError("incorrect length")
        return cls(bytes(raw))

    def __str__(self):
        return str(self._bech32m)

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"

    def __eq__(self, other: object):
        if not isinstance(other, AleoID):
            return False
        return self._data == other._data


class AleoObject(AleoIDProtocol):
    size = 0
    _prefix = ""

    def __init__(self, data: bytes):
        self._data = data
        self._bech32m = Bech32m(data, self._prefix)

    def dump(self) -> bytes:
        return self._data

    @classmethod
    def load(cls, data: BytesIO):
        size = cls.size
        # noinspection PyTypeChecker
        if data.tell() + size > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        self = cls(data.read(size))
        return self

    @classmethod
    def loads(cls, data: str):
        hrp, raw = aleo.bech32_decode(data)
        if hrp != cls._prefix:
            raise ValueError("incorrect hrp")
        if len(raw) != cls.size:
            raise ValueError("incorrect length")
        return cls(bytes(raw))

    def __str__(self):
        return str(self._bech32m)

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"

    def __eq__(self, other: object):
        if not isinstance(other, AleoObject):
            return False
        return self._data == other._data


class BlockHash(AleoID):
    _prefix = "ab"


class StateRoot(AleoID):
    _prefix = "ar"


class TransactionID(AleoID):
    _prefix = "at"


class TransitionID(AleoID):
    _prefix = "as"


## Saved for reference
# class RecordCiphertext(AleoObject):
#     _prefix = "recd"
#     size = 288
#
#     def __init__(self, data):
#         AleoObject.__init__(self, data)
#
#     def get_commitment(self):
#         return aleo.get_record_ciphertext_commitment(self.data)


class Address(AleoObject):
    # Should work like this...

    _prefix = "aleo"
    size = 32


class Field(Serializable, Add, Sub, Mul, Div, Compare, Pow, Cast):
    # Fr, Fp256
    # Just store as a large integer now
    # Hopefully this will not be used later...
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 32 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("field")))

    def __str__(self):
        return str(self.data) + "field"

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"

    def __eq__(self, other: object):
        if not isinstance(other, Field):
            return False
        return self.data == other.data

    def __hash__(self):
        return hash(self.data)

    def __add__(self, other: Self):
        return Field.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "add")))

    def __sub__(self, other: Self):
        return Field.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "sub")))

    def __mul__(self, other: Self):
        return Field.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "mul")))

    def __floordiv__(self, other: Self):
        return Field.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "div")))

    def __gt__(self, other: Self):
        return bool_.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "gt"))).value

    def __lt__(self, other: Self):
        return bool_.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "lt"))).value

    def __ge__(self, other: Self):
        return bool_.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "gte"))).value

    def __le__(self, other: Self):
        return bool_.load(BytesIO(aleo.field_ops(self.dump(), other.dump(), "lte"))).value

    def __pow__(self, power: Self):
        return Field.load(BytesIO(aleo.field_ops(self.dump(), power.dump(), "pow")))

    def cast(self, destination_type: Any, *, lossy: bool) -> Any:
        from .vm_instruction import LiteralType
        if not isinstance(destination_type, LiteralType):
            raise ValueError("invalid type")
        return destination_type.primitive_type.load(BytesIO(aleo.field_cast(self.dump(), destination_type.dump(), lossy)))


class Group(Serializable):
    # This is definitely wrong, but we are not using the internals
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 32 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("group")))

    def __str__(self):
        return str(self.data) + "group"

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"


class Scalar(Serializable):
    # Could be wrong as well
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 32 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("scalar")))

    def __str__(self):
        return str(self.data) + "scalar"

    def __repr__(self):
        return self.__class__.__name__ + "(" + str(self) + ")"


class Fq(Serializable):
    # Fp384, G1
    def __init__(self, value: int):
        self.value = value

    def dump(self) -> bytes:
        return self.value.to_bytes(48, "little")

    @classmethod
    def load(cls, data: BytesIO):
        if data.tell() + 48 > data.getbuffer().nbytes:
            raise ValueError("incorrect length")
        value = int.from_bytes(data.read(48), "little")
        return cls(value=value)

    def __str__(self):
        return str(self.value)

class G1Affine(Serializable):

    def __init__(self, *, x: Fq, flags: bool):
        self.x = x
        self.flags = flags

    def dump(self) -> bytes:
        res = bytearray(self.x.dump())
        res[-1] |= self.flags << 7
        return bytes(res)

    @classmethod
    def load(cls, data: BytesIO):
        data_ = bytearray(data.read(48))
        flags = bool(data_[-1] >> 7)
        data_[-1] &= 0x7f
        return cls(x=Fq.load(BytesIO(data_)), flags=flags)

class Fq2(Serializable):

    def __init__(self, c0: Fq, c1: Fq, flags: bool):
        self.c0 = c0
        self.c1 = c1
        self.flags = flags

    def dump(self) -> bytes:
        res = bytearray(self.c0.dump() + self.c1.dump())
        res[-1] |= self.flags << 7
        return res

    @classmethod
    def load(cls, data: BytesIO):
        data_ = bytearray(data.read(96))
        flags = bool(data_[-1] >> 7)
        data_[-1] &= 0x7f
        c0 = Fq.load(BytesIO(data_))
        c1 = Fq.load(BytesIO(data_))
        return cls(c0=c0, c1=c1, flags=flags)

class G2Affine(Serializable):

    def __init__(self, *, x: Fq2):
        self.x = x

    def dump(self) -> bytes:
        return self.x.dump()

    @classmethod
    def load(cls, data: BytesIO):
        x = Fq2.load(data)
        return cls(x=x)

class G2Prepared(Serializable):

    def __init__(self, *, ell_coeffs: Vec[Tuple[Fq2, Fq2, Fq2], u64], infinity: bool_):
        self.ell_coeffs = ell_coeffs
        self.infinity = infinity

    def dump(self) -> bytes:
        return self.ell_coeffs.dump() + self.infinity.dump()

    @classmethod
    def load(cls, data: BytesIO):
        ell_coeffs = Vec[Tuple[Fq2, Fq2, Fq2], u64].load(data)
        infinity = bool_.load(data)
        return cls(ell_coeffs=ell_coeffs, infinity=infinity)