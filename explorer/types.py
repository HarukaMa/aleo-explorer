from enum import IntEnum
from typing import Any

from node.types import Block


class Message:
    class Type(IntEnum):
        NodeConnectError = 0
        NodeConnected = 1
        NodeDisconnected = 2

        DatabaseConnectError = 100
        DatabaseConnected = 101
        DatabaseDisconnected = 102
        DatabaseError = 103
        DatabaseBlockAdded = 104

    def __init__(self, type_: Type, data: Any):
        self.type = type_
        self.data = data

class ExplorerRequest:
    pass

class Request:

    class ProcessBlock(ExplorerRequest):
        def __init__(self, block: Block):
            self.block = block

    class GetLatestHeight(ExplorerRequest):
        pass

    class GetLatestWeight(ExplorerRequest):
        pass

    class GetLatestBlock(ExplorerRequest):
        pass

    class GetBlockByHeight(ExplorerRequest):
        def __init__(self, height: int):
            self.height = height

    class GetBlockHashByHeight(ExplorerRequest):
        def __init__(self, height: int):
            self.height = height

    class GetBlockHeaderByHeight(ExplorerRequest):
        def __init__(self, height: int):
            self.height = height

    class RevertToBlock(ExplorerRequest):
        def __init__(self, height: int):
            self.height = height

    class GetDevMode(ExplorerRequest):
        pass

    # class
