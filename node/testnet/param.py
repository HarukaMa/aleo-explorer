import os
from io import BytesIO

import aleo_explorer_rust

from aleo_types import u16, Block, u32, Program, Field


def load_program(program_id: str) -> Program:
    return Program.load(BytesIO(aleo_explorer_rust.parse_program(open(os.path.join(os.path.dirname(os.path.dirname(__file__)), program_id)).read())))

class Testnet:
    edition = u16()
    network_id = u16(1)
    version = u32(23)

    genesis_block = Block.load(BytesIO(open(os.path.join(os.path.dirname(__file__), "block.genesis"), "rb").read()))
    dev_genesis_block = Block.load(BytesIO(open(os.path.join(os.path.dirname(__file__), "dev.genesis"), "rb").read()))
    
    builtin_programs = [
        (load_program("credits.aleo"), 0),
        (load_program("credits_v1.aleo"), 1),
    ]

    block_locator_num_recents = 100
    block_locator_recent_interval = 1
    block_locator_checkpoint_interval = 10000

    deployment_fee_multiplier = 1000
    synthesis_fee_multiplier = 25

    ans_registry = "aleo_name_service_registry_v2.aleo"

    restrictions_id = Field(7562506206353711030068167991213732850758501012603348777370400520506564970105)

    consensus_v2_height = 2_950_000
    consensus_v3_height = 4_800_000
    consensus_v4_height = 6_625_000
    consensus_v8_height = 9_173_000
    consensus_v9_height = 9_800_000
    consensus_v12_height = 12_669_000