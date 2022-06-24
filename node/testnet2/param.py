import os

from ..type import u32, Block, ProgramID


class Testnet2:
    version = u32(12)
    fork_depth = u32(4096)

    genesis_block = Block.load(bytearray(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))

    maximum_linear_block_locators = 64
    maximum_quadratic_block_locators = 32

    noop_program_id = ProgramID.loads(
        "ap1lhj3g5uzervu3km7rl0rsd0u5j6pj9ujum6yxrvms4mx8r2qhew88ga849hnjypghswxceh02frszs45qmd")
