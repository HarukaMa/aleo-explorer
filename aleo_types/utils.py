from enum import EnumMeta
from functools import lru_cache
from io import BytesIO
# noinspection PyUnresolvedReferences,PyProtectedMember
from typing import get_type_hints, _ProtocolMeta, Any  # type: ignore[reportPrivateUsage]

import aleo_explorer_rust


# Metaclass Helper

class ProtocolEnumMeta(_ProtocolMeta, EnumMeta):
    # https://stackoverflow.com/questions/56131308/create-an-abstract-enum-class/56135108#56135108
    def __new__(cls, *args: Any, **kw: Any):
        abstract_enum_cls = super().__new__(cls, *args, **kw)
        # Only check abstractions if members were defined.
        # noinspection PyProtectedMember
        if abstract_enum_cls._member_map_:
            try:  # Handle existence of undefined abstract methods.
                absmethods = list(abstract_enum_cls.__abstractmethods__)
                if absmethods:
                    missing = ', '.join(f'{method!r}' for method in absmethods)
                    plural = 's' if len(absmethods) > 1 else ''
                    raise TypeError(
                        f"cannot instantiate abstract class {abstract_enum_cls.__name__!r}"
                        f" with abstract method{plural} {missing}")
            except AttributeError:
                pass
        return abstract_enum_cls


def bech32_to_bytes(s: str) -> BytesIO:
    return BytesIO(aleo_explorer_rust.bech32_decode(s)[1])

@lru_cache(maxsize=1048576)
def cached_get_key_id(program_id: str, mapping_name: str, key: bytes) -> str:
    return aleo_explorer_rust.get_key_id(program_id, mapping_name, key)

@lru_cache(maxsize=1048576)
def cached_get_mapping_id(program_id: str, mapping: str) -> str:
    return aleo_explorer_rust.get_mapping_id(program_id, mapping)
