
from .address import DatabaseAddress
from .block import DatabaseBlock
from .insert import DatabaseInsert
from .migrate import DatabaseMigrate
from .program import DatabaseProgram
from .search import DatabaseSearch
from .util import DatabaseUtil
from .validator import DatabaseValidator

class Database(DatabaseAddress, DatabaseBlock, DatabaseInsert, DatabaseMigrate, DatabaseProgram, DatabaseSearch,
               DatabaseUtil, DatabaseValidator):
    pass