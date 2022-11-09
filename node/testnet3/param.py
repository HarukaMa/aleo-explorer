import os

from ..types import u16, Block


class Testnet3:
    edition = u16()
    network_id = u16(3)

    genesis_block = Block.load(bytearray(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))