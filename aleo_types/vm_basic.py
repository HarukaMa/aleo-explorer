from __future__ import annotations

from .generic import *


class AleoIDProtocol(Sized, Serializable, Protocol):
    size: int
    _prefix: str

class AleoID(AleoIDProtocol):
    size = 32
    _prefix = ""

    def __init__(self, data: bytes):
        if len(self._prefix) != 2:
            if self._prefix != "solution":
                raise ValueError("locator_prefix must be 2 bytes")
        self._data = data
        self._bech32m = Bech32m(data, self._prefix)

    def dump(self) -> bytes:
        return self._data

    @classmethod
    def load(cls, data: BytesIO):
        size = cls.size
        self = cls(data.read(size))
        return self

    @classmethod
    def loads(cls, data: str):
        hrp, raw = aleo_explorer_rust.bech32_decode(data)
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
        self = cls(data.read(size))
        return self

    @classmethod
    def loads(cls, data: str):
        hrp, raw = aleo_explorer_rust.bech32_decode(data)
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
    _prefix = "sr"


class TransactionID(AleoID):
    _prefix = "at"


class TransitionID(AleoID):
    _prefix = "au"

class SolutionID(AleoID):
    _prefix = "solution"

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


class Address(AleoObject, Cast):
    # Should work like this...

    _prefix = "aleo"
    size = 32

    def cast(self, destination_type: Any, *, lossy: bool) -> Any:
        from .vm_instruction import LiteralType
        if not isinstance(destination_type, LiteralType):
            raise ValueError("invalid type")
        return destination_type.primitive_type.load(BytesIO(aleo_explorer_rust.cast(str(self), LiteralType.Address, destination_type, lossy)))

    def __hash__(self):
        return hash(self._data)

    def __eq__(self, other: object):
        if not isinstance(other, Address):
            return False
        return self._data == other._data


class Field(Serializable, Double, Sub, Square, Div, Sqrt, Compare, Pow, Inv, Neg, Cast):
    # Fr, Fp256
    # Just store as a large integer now
    # Hopefully this will not be used later...
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("field")))

    def __str__(self):
        return str(self.data) + "field"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"

    def __eq__(self, other: object):
        if not isinstance(other, Field):
            return False
        return self.data == other.data

    def __hash__(self):
        return hash(self.data)

    def __add__(self, other: Self):
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "add")))

    def double(self) -> Field:
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, self, "double")))

    def __sub__(self, other: Self):
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "sub")))

    def __mul__(self, other: Self):
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "mul")))

    def square(self) -> Field:
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, self, "square")))

    def sqrt(self) -> Field:
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, self, "sqrt")))

    def __floordiv__(self, other: Self):
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "div")))

    def __gt__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "gt"))).value

    def __lt__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "lt"))).value

    def __ge__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "gte"))).value

    def __le__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.field_ops(self, other, "lte"))).value

    def __pow__(self, power: Self, mod: None = None):
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, power, "pow")))

    def inv(self) -> Field:
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, self, "inv")))

    def __neg__(self) -> Field:
        return Field.load(BytesIO(aleo_explorer_rust.field_ops(self, self, "neg")))

    def cast(self, destination_type: Any, *, lossy: bool) -> Any:
        from .vm_instruction import LiteralType
        if not isinstance(destination_type, LiteralType):
            raise ValueError("invalid type")
        return destination_type.primitive_type.load(BytesIO(aleo_explorer_rust.cast(str(self), LiteralType.Field, destination_type, lossy)))


class Group(Serializable, Add, Sub, Mul, Neg, Cast):
    # This is definitely wrong, but we are not using the internals
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("group")))

    def __str__(self):
        return str(self.data) + "group"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"

    def __add__(self, other: Self):
        return Group.load(BytesIO(aleo_explorer_rust.group_ops(self, other, "add")))

    def double(self) -> Group:
        return Group.load(BytesIO(aleo_explorer_rust.group_ops(self, self, "double")))

    def __sub__(self, other: Self):
        return Group.load(BytesIO(aleo_explorer_rust.group_ops(self, other, "sub")))

    def __mul__(self, other: Scalar):
        return Group.load(BytesIO(aleo_explorer_rust.group_ops(self, other, "mul")))

    def __neg__(self) -> Group:
        return Group.load(BytesIO(aleo_explorer_rust.group_ops(self, self, "neg")))

    def cast(self, destination_type: Any, *, lossy: bool) -> Any:
        from .vm_instruction import LiteralType
        if not isinstance(destination_type, LiteralType):
            raise ValueError("invalid type")
        return destination_type.primitive_type.load(BytesIO(aleo_explorer_rust.cast(str(self), LiteralType.Group, destination_type, lossy)))


class Scalar(Serializable, Add, Sub, Mul, Compare, Cast):
    # Could be wrong as well
    def __init__(self, data: int):
        self.data = data

    def dump(self) -> bytes:
        return self.data.to_bytes(32, "little")

    @classmethod
    def load(cls, data: BytesIO):
        data_ = int.from_bytes(data.read(32), "little")
        return cls(data_)

    @classmethod
    def loads(cls, data: str):
        return cls(int(data.removesuffix("scalar")))

    def __str__(self):
        return str(self.data) + "scalar"

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"

    def __add__(self, other: Self):
        return Scalar.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "add")))

    def __sub__(self, other: Self):
        return Scalar.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "sub")))

    def __mul__(self, other: Group):
        return Group.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "mul")))

    def __gt__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "gt"))).value

    def __lt__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "lt"))).value

    def __ge__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "gte"))).value

    def __le__(self, other: Self):
        return bool_.load(BytesIO(aleo_explorer_rust.scalar_ops(self, other, "lte"))).value

    def __eq__(self, other: Self):
        return self.data == other.data

    def cast(self, destination_type: Any, *, lossy: bool) -> Any:
        from .vm_instruction import LiteralType
        if not isinstance(destination_type, LiteralType):
            raise ValueError("invalid type")
        return destination_type.primitive_type.load(BytesIO(aleo_explorer_rust.cast(str(self), LiteralType.Scalar, destination_type, lossy)))



class Fq(Serializable):
    # Fp384, G1
    def __init__(self, value: int):
        self.value = value

    def dump(self) -> bytes:
        return self.value.to_bytes(48, "little")

    @classmethod
    def load(cls, data: BytesIO):
        value = int.from_bytes(data.read(48), "little")
        return cls(value=value)

    def __str__(self):
        return str(self.value)

class G1Affine(Serializable):

    def __init__(self, *, x: Fq, y_is_positive: bool):
        self.x = x
        self.y_is_positive = y_is_positive

    def dump(self) -> bytes:
        res = bytearray(self.x.dump())
        res[-1] |= self.y_is_positive << 7
        return bytes(res)

    @classmethod
    def load(cls, data: BytesIO):
        data_ = bytearray(data.read(48))
        y_is_positive = bool(data_[-1] >> 7)
        data_[-1] &= 0x7f
        x = Fq.load(BytesIO(data_))
        return cls(x=x, y_is_positive=y_is_positive)


class Fq2(Serializable):

    def __init__(self, c0: Fq, c1: Fq, flags: bool):
        self.c0 = c0
        self.c1 = c1
        self.flags = flags

    def dump(self) -> bytes:
        res = bytearray(self.c0.dump() + self.c1.dump())
        res[-1] |= self.flags << 7
        return bytes(res)

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


class ComputeKey(Serializable):

    def __init__(self, *, pk_sig: Group, pr_sig: Group):
        self.pk_sig = pk_sig
        self.pr_sig = pr_sig

    def dump(self) -> bytes:
        return self.pk_sig.dump() + self.pr_sig.dump()

    @classmethod
    def load(cls, data: BytesIO):
        pk_sig = Group.load(data)
        pr_sig = Group.load(data)
        return cls(pk_sig=pk_sig, pr_sig=pr_sig)


class Signature(Serializable):

    def __init__(self, *, challenge: Scalar, response: Scalar, compute_key: ComputeKey):
        self.challenge = challenge
        self.response = response
        self.compute_key = compute_key

    def dump(self) -> bytes:
        return self.challenge.dump() + self.response.dump() + self.compute_key.dump()

    @classmethod
    def load(cls, data: BytesIO):
        challange = Scalar.load(data)
        response = Scalar.load(data)
        compute_key = ComputeKey.load(data)
        return cls(challenge=challange, response=response, compute_key=compute_key)

    @classmethod
    def loads(cls, data: str):
        return cls.load(bech32_to_bytes(data))

    def __str__(self):
        return str(Bech32m(self.dump(), "sign"))

    def __repr__(self):
        return str(self)



class Data(Serializable, Generic[T]):
    types: TType[T]
    version = u8(1)

    def __init__(self, value: T):
        self.value = value

    @tp_cache
    def __class_getitem__(cls, key) -> GenericAlias:
        param_type = type(
            f"Data[{key.__name__}]",
            (Data,),
            {"types": key},
        )
        return GenericAlias(param_type, key)

    def dump(self) -> bytes:
        data = self.value.dump()
        return self.version.dump() + len(data).to_bytes(4, "little") + data

    @classmethod
    def load(cls, data: BytesIO) -> Self:
        if cls.types is None:
            raise TypeError("expected types")
        version = u8.load(data)
        if version != cls.version:
            raise ValueError(f"expected version {cls.version}, got {version}")
        size = u32.load(data)
        value = cls.types.load(BytesIO(data.read(size)))
        return cls(value)
