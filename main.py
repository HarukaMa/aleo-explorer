import asyncio
from typing import _BaseGenericAlias

from dotenv import load_dotenv

from explorer import Explorer


def new_call(self, *args, **kwargs):
    if not self._inst:
        raise TypeError(f"Type {self._name} cannot be instantiated; "
                        f"use {self.__origin__.__name__}() instead")
    new = self.__new__(self)
    result = self.__origin__(*args, **kwargs)
    try:
        result.__orig_class__ = self
    except AttributeError:
        pass
    return result

_BaseGenericAlias.__call__ = new_call

load_dotenv()

async def main():
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())