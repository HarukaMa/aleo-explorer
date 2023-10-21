
from aleo_types import *

MappingCacheDict = dict[Field, dict[str, Any]]

global_mapping_cache: dict[Field, MappingCacheDict] = {}
global_program_cache: dict[str, Program] = {}

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