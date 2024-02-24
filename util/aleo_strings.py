from typing import cast

from aleo_types import ArrayPlaintext, LiteralPlaintext, Literal, u128, u32, Plaintext, Vec


def string_from_u128_array_le(array: ArrayPlaintext) -> str:
    string = ""
    for i in range(len(array)):
        element = array[i]
        if not isinstance(element, LiteralPlaintext):
            raise ValueError(f"Expected array of plaintext literals, got {element}")
        if element.literal.type != Literal.Type.U128:
            raise ValueError(f"Expected array of u128 literals, got {element.literal.type}")
        value = cast(u128, element.literal.primitive)
        string += value.to_bytes(16, "little").decode("utf8")
    return string.rstrip("\0")

def string_to_u128_array_le(string: str, size: int) -> ArrayPlaintext:
    data = string.encode("utf8")
    if len(data) > size * 16:
        raise ValueError(f"String is too long, expected at most {size * 16} bytes, got {len(data)}")
    data += b"\0" * (size * 16 - len(data))
    array: list[Plaintext] = []
    for i in range(size):
        value = u128.from_bytes(data[i * 16:(i + 1) * 16], "little")
        array.append(LiteralPlaintext(literal=Literal(type_=Literal.Type.U128, primitive=u128(value))))
    return ArrayPlaintext(elements=Vec[Plaintext, u32](array))