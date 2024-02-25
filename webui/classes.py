from typing import Self, Optional

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

    def __str__(self) -> str:
        return self.tag or self.name or self.address

    def address_trunc(self) -> str:
        return self.address[:11] + "..." + self.address[-6:]

    def trunc(self) -> str:
        return self.tag or self.name or self.address_trunc()