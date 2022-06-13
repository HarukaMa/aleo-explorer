import os

from ..type import u32, Block


class Testnet2:
    version = u32(12)
    fork_depth = u32(4096)

    genesis_block = Block.load(bytearray(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))

    maximum_linear_block_locators = 64
    maximum_quadratic_block_locators = 32
