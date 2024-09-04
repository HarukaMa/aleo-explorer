from typing import Self, Optional, Any, cast

from db import Database
from util import arc0137


class UIAddress:

    def __init__(self, address: str, name: Optional[str] = None, tag: Optional[str] = None):
        self.address = address
        self.name = name
        self.tag = tag

    async def resolve(self, db: Database) -> Self:
        if self.name is None:
            self.name = await arc0137.get_primary_name_from_address(db, self.address)
        # TODO: implement custom address tag on explorer
        return self

    def to_partial_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "tag": self.tag,
        }

    def __str__(self) -> str:
        return self.tag or self.name or self.address

    def address_trunc(self) -> str:
        return self.address[:11] + "..." + self.address[-6:]

    def trunc(self) -> str:
        return self.tag or self.name or self.address_trunc()

    @staticmethod
    async def resolve_recursive_detached(data: Any, db: Database, partial: dict[str, dict[str, Any]]) -> dict[str, Any]:
        def is_address_str(s: Any):
            if not isinstance(s, str):
                return False
            return s.startswith("aleo1") and len(s) == 63
        if isinstance(data, dict):
            data = cast(dict[Any, Any], data)
            for k, v in data.items():
                if is_address_str(k) and k not in partial:
                    partial[k] = (await UIAddress(k).resolve(db)).to_partial_json()
                partial = await UIAddress.resolve_recursive_detached(v, db, partial)
        elif isinstance(data, list):
            data = cast(list[Any], data)
            for v in data:
                partial = await UIAddress.resolve_recursive_detached(v, db, partial)
        elif isinstance(data, UIAddress):
            if data.address not in partial:
                partial[data.address] = (await data.resolve(db)).to_partial_json()
        elif is_address_str(data) and data not in partial:
            partial[data] = (await UIAddress(data).resolve(db)).to_partial_json()
        return partial