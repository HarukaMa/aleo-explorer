import asyncio
from asyncio import Future as AFuture
from copy import deepcopy

from aleo_types import *
from aleo_types.cached import cached_get_mapping_id

MappingCacheDict = dict[Field, dict[str, Any]]

CacheMappingContent = TypedDict(
    "CacheMappingContent", {
        "key": Plaintext,
        "value": Value,
    }
)
CacheMappingDict = dict[Field, CacheMappingContent | None]
CacheDict = dict[Field, "MappingCacheMapping"]

global_mapping_cache: dict[Field, MappingCacheDict] = {}
global_program_cache: dict[str, Program] = {}

committee_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "committee"))
delegated_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "delegated"))
bonded_mapping_id = Field.loads(cached_get_mapping_id("credits.aleo", "bonded"))

class MappingCacheMapping:
    def __init__(self, db: "Database", mapping_id: Field):
        self.db = db
        self.mapping_id = mapping_id
        self.mapping_data: CacheMappingDict = {}

    async def async_getitem(self, future: AFuture[CacheMappingContent | None], key_id: Field) -> CacheMappingContent | None:
        try:
            future.set_result(self.mapping_data[key_id])
        except KeyError:
            data = await self.db.get_mapping_cache_key_value(str(self.mapping_id), str(key_id))
            if data is None:
                self.mapping_data[key_id] = None
                future.set_result(None)
                return
            self.mapping_data[key_id] = {
                "key": Plaintext.load(BytesIO(data["key"])),
                "value": Value.load(BytesIO(data["value"]))
            }
            future.set_result(self.mapping_data[key_id])

    def __getitem__(self, key_id: Field):
        loop = asyncio.get_event_loop()
        future: AFuture[CacheMappingContent | None] = loop.create_future()
        loop.create_task(self.async_getitem(future, key_id))
        return future

    def __setitem__(self, key_id: Field, value: CacheMappingContent | None):
        self.mapping_data[key_id] = value

    def __iter__(self):
        return iter(self.mapping_data.items())

    def __contains__(self, key_id: Field):
        return key_id in self.mapping_data

    def clear(self):
        self.mapping_data.clear()

    async def populate(self):
        if self.mapping_id == committee_mapping_id:
            data = await self.db.get_mapping_cache("credits.aleo", "committee")
        elif self.mapping_id == delegated_mapping_id:
            data = await self.db.get_mapping_cache("credits.aleo", "delegated")
        elif self.mapping_id == bonded_mapping_id:
            data = await self.db.get_mapping_cache("credits.aleo", "bonded")
        else:
            raise ValueError("no need to populate this mapping")
        self.mapping_data.update(data)

    def copy(self):
        new = object.__new__(self.__class__)
        new.db = self.db
        new.mapping_id = self.mapping_id
        new.mapping_data = deepcopy(self.mapping_data)
        return new

class MappingCache:
    _instance: "MappingCache | None" = None
    db: "Database | None"

    def __new__(cls, db: "Database | None" = None):
        if cls._instance is None:
            if db is None:
                raise ValueError("Database instance is required for the first call")
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self, db: "Database | None" = None):
        if getattr(self, "db", None) is None:
            self.db = db
            self.mapping_data: CacheDict = {}

    def __getitem__(self, mapping_id: Field):
        try:
            return self.mapping_data[mapping_id]
        except KeyError:
            if self.db is None:
                raise ValueError("database is not set up for mapping cache")
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError("mapping cache must be initialized in an async context")
            result = loop.run_until_complete(self.db.mapping_id_exists(str(mapping_id)))
            if not result:
                raise KeyError(f"mapping not found: {mapping_id}")
            self.mapping_data[mapping_id] = MappingCacheMapping(self.db, mapping_id)
            return self.mapping_data[mapping_id]

    def __contains__(self, mapping_id: Field):
        return mapping_id in self.mapping_data

    async def pre_populate(self):
        if self.db is None:
            raise ValueError("database is not set up for mapping cache")

        await self[committee_mapping_id].populate()
        await self[delegated_mapping_id].populate()
        await self[bonded_mapping_id].populate()

    def clear(self):
        self.mapping_data.clear()

    def copy(self):
        new = object.__new__(self.__class__)
        new.db = self.db
        new.mapping_data = {k: v.copy() for k, v in self.mapping_data.items()}
        return new

async def get_program(db: "Database", program_id: str) -> Program | None:
    try:
        return global_program_cache[program_id]
    except KeyError:
        program = await db.get_program(program_id)
        if not program:
            return None
        program = Program.load(BytesIO(program))
        global_program_cache[program_id] = program
        return program