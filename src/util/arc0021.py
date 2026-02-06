from typing import TypedDict, cast

from aleo_types import Field, Address, Literal
from aleo_types.basic import u128, u8
from aleo_types.serialize import Serializable
from aleo_types.vm_block import PlaintextValue, StructPlaintext, LiteralPlaintext, Plaintext
from db import Database
from util.aleo_strings import string_from_u128_list_be


class TokenMetadata(TypedDict):
    token_id: Field
    name: str
    symbol: str
    decimals: int
    supply: int
    max_supply: int
    admin: Address
    external_authorization_required: bool
    external_authorization_party: Address

def _u128_plaintext_to_string(value: Plaintext):
    if not isinstance(value, LiteralPlaintext):
        raise ValueError(f"Expected u128 literal, got {value}")
    if value.literal.type != Literal.Type.U128:
        raise ValueError(f"Expected u128 literal, got {value.literal.type}")
    try:
        return string_from_u128_list_be([cast(u128, value.literal.primitive)])
    except UnicodeDecodeError:
        return "[Invalid name]"

async def token_list(db: Database):
    registered_tokens = await db.get_mapping_cache("token_registry.aleo", "registered_tokens")
    result: list[TokenMetadata] = []

    def unwrap_plaintext(v: Plaintext) -> Serializable:
        return cast(LiteralPlaintext, v).literal.primitive

    for metadata in registered_tokens.values():
        value: PlaintextValue = metadata["value"]
        st = cast(StructPlaintext, value.plaintext)
        result.append({
            "token_id": cast(Field, unwrap_plaintext(st["token_id"])),
            "name": _u128_plaintext_to_string(st["name"]),
            "symbol": _u128_plaintext_to_string(st["symbol"]),
            "decimals": int(cast(u8, unwrap_plaintext(st["decimals"]))),
            "supply": int(cast(u128, unwrap_plaintext(st["supply"]))),
            "max_supply": int(cast(u128, unwrap_plaintext(st["max_supply"]))),
            "admin": cast(Address, unwrap_plaintext(st["admin"])),
            "external_authorization_required": bool(unwrap_plaintext(st["external_authorization_required"])),
            "external_authorization_party": cast(Address, unwrap_plaintext(st["external_authorization_party"])),
        })

    return result

