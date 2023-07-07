import time
from collections import OrderedDict

from typing import TypeVar, Generic

KT = TypeVar('KT')
VT = TypeVar('VT')

class FetchError(Exception):
    pass

class Cache(Generic[KT, VT]):
    def __init__(self, *, max_lifetime: int = 3600, max_size: int = 100, fetch_func: callable = None):
        self._content: dict[KT, VT] = {}
        self.max_lifetime = max_lifetime
        self.max_size = max_size
        self.__lru: OrderedDict[int: KT] = OrderedDict()
        self.__fetch_func = fetch_func

    def purge(self):
        if len(self._content) > self.max_size:
            for key in self.__lru:
                del self._content[key]
                del self.__lru[key]
                break
        for key, value in list(self.__lru.items()):
            if time.monotonic() - value > self.max_lifetime:
                del self._content[key]
                del self.__lru[key]
            else:
                break

    def __getitem__(self, key: KT) -> VT:
        if key not in self._content:
            if self.__fetch_func:
                try:
                    self._content[key] = self.__fetch_func(key)
                    self.__lru[key] = time.monotonic()
                except FetchError:
                    raise KeyError(key)
            else:
                raise KeyError(key)
        else:
            self.__lru[key] = time.monotonic()
        self.purge()
        return self._content[key]

    def __setitem__(self, key: KT, value: VT):
        self._content[key] = value
        self.__lru[key] = time.monotonic()
        self.purge()

    def __delitem__(self, key: KT):
        del self._content[key]
        del self.__lru[key]
        self.purge()
