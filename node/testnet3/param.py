import os

from ..types import u16, Block, u32


class Testnet3:
    edition = u16()
    network_id = u16(3)
    version = u32(5)

    genesis_block = Block.load(bytearray(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))

    block_locator_num_recents = 100
    block_locator_recent_interval = 1
    block_locator_checkpoint_interval = 10000