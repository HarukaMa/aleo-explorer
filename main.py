import asyncio

import aleo
# import rocksdb
import struct

from explorer import Explorer
from type import BlockHeader, Vec, TransactionID


# class Extractor(rocksdb.ISliceTransform):
#     def name(self):
#         return b'FixedPrefix'
#
#     def transform(self, src):
#         return 0, 4
#
#     def in_domain(self, src):
#         return len(src) >= 4
#
#     def in_range(self, dst):
#         return len(dst) == 4


# class MapTypes:
#     BlockHeaders = 0
#     BlockHeights = 1
#     BlockTransactions = 2
#     Commitments = 3
#     LedgerRoots = 4
#     Records = 5
#     SerialNumbers = 6
#     Transactions = 7
#     Transitions = 8
#     Shares = 9

# def _get_prefix(map_type):
#     return struct.pack("<HH", 2, map_type)

# class DB:
#     def __init__(self, path):
#         opts = rocksdb.Options()
#         opts.prefix_extractor = Extractor()
#         self.db = rocksdb.DB(path, opts, read_only=True)
#
#     def get(self, map_type, key):
#         return self.db.get(_get_prefix(map_type) + key)


# def main():
#     db = DB("/Users/MrX/.aleo/storage/ledger-2")
#     h = db.get(MapTypes.BlockHeights, struct.pack("<I", 200000))
#     header = BlockHeader().load(db.get(MapTypes.BlockHeaders, h))
#     print(header)
#     transactions = db.get(MapTypes.BlockTransactions, h)
#     v = Vec[TransactionID].load(transactions)
#     pass

async def main():
    e = Explorer()
    e.start()
    while True:
        await asyncio.sleep(3600)


if __name__ == '__main__':
    asyncio.run(main())