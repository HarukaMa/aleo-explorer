from typing import Self, Optional

from db import Database
from util import arc0137


class ANSAddress:

    def __init__(self, address: str, name: Optional[str] = None):
        self.address = address
        self.name = name

    async def resolve(self, db: Database) -> Self:
        if self.name is None:
            self.name = await arc0137.get_primary_name_from_address(db, self.address)
        return self

    def __str__(self) -> str:
        return self.name or self.address