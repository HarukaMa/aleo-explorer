import aleo
from abc import ABCMeta
from enum import EnumMeta
from io import BytesIO
from typing import get_type_hints


# Metaclass Helper

class ABCEnumMeta(ABCMeta, EnumMeta):
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


# Type check decorator

def type_check(func):
    def wrapper(*args, **kwargs):
        hints = get_type_hints(func)
        for v, t in hints.items():
            arg = kwargs.get(v)
            if arg is None and v == "data":
                arg = args[1]
            if isinstance(t, type) and not isinstance(arg, t):
                raise TypeError(f"{v} should be {t}, but got {type(arg)}")
        return func(*args, **kwargs)

    return wrapper

def bech32_to_bytes(s: str) -> BytesIO:
    return BytesIO(bytes(aleo.bech32_decode(s)[1]))