import os
from io import BytesIO

import aleo_explorer_rust

from aleo_types import u16, Block, u32, Program


def load_program(program_id: str) -> Program:
    return Program.load(BytesIO(aleo_explorer_rust.parse_program(open(os.path.join(os.path.dirname(__file__), program_id)).read())))

class Testnet3:
    edition = u16()
    network_id = u16(3)
    version = u32(11)

    genesis_block = Block.load(BytesIO(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))
    dev_genesis_block = Block.load(BytesIO(open(os.path.join(os.path.dirname(__file__), "dev.genesis"), "rb").read()))
    
    builtin_programs = [
        load_program("credits.aleo"),
    ]

    block_locator_num_recents = 100
    block_locator_recent_interval = 1
    block_locator_checkpoint_interval = 10000

    deployment_fee_multiplier = 1000