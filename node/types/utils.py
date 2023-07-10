from enum import EnumMeta
from io import BytesIO
# noinspection PyUnresolvedReferences,PyProtectedMember
from typing import get_type_hints, _ProtocolMeta

import aleo


# Metaclass Helper

class ProtocolEnumMeta(_ProtocolMeta, EnumMeta):
    # https://stackoverflow.com/questions/56131308/create-an-abstract-enum-class/56135108#56135108
    def __new__(cls, *args, **kw):
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
    return BytesIO(aleo.bech32_decode(s)[1])