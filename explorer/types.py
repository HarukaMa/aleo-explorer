from enum import IntEnum

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

    def __init__(self, type_: Type, data: any):
        self.type = type_
        self.data = data


class Request:

    class ProcessBlock("Request"):
        def __init__(self, block: Block):
            self.block = block

    class GetLatestHeight("Request"):
        pass

    class GetLatestWeight("Request"):
        pass

    class GetLatestBlock("Request"):
        pass

    class GetBlockByHeight("Request"):
        def __init__(self, height: int):
            self.height = height

    class GetBlockHashByHeight("Request"):
        def __init__(self, height: int):
            self.height = height

    class GetBlockHeaderByHeight("Request"):
        def __init__(self, height: int):
            self.height = height

    class RevertToBlock("Request"):
        def __init__(self, height: int):
            self.height = height

    class GetDevMode("Request"):
        pass

    # class
