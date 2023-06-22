import os

import aleo

from ..types import u16, Block, u32, Program


def load_program(program_id: str) -> Program:
    return Program.load(bytearray(aleo.get_program_from_str(open(os.path.join(os.path.dirname(__file__), program_id), "r").read())))

class Testnet3:
    edition = u16()
    network_id = u16(3)
    version = u32(7)

    genesis_block = Block.load(bytearray(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))
    builtin_programs = [
        load_program("credits.aleo"),
    ]

    block_locator_num_recents = 100
    block_locator_recent_interval = 1
    block_locator_checkpoint_interval = 10000