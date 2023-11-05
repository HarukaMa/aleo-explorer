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

    ignore_deploy_txids = [
        "at12enkvgct4ssyp9ggq87q60748h3gx69hwe3s8cay5q2fnreatypsuyw9jw",
        "at1dpe2vvskn99avv0zknnu0chex9wyy4rq5ax2v39nk29sw38nauxsc2ra7z",
        "at1ak70x90pnwszdaxehwnt99ta2w4q8kn40tepau68r6yeaar77sxsaxwelu",
        "at1zevn9a9d7q5pfm7z3flfzkata4m93dg6v3chqd2km0vdtfcnqyqq8fzjez",
        "at18s8ld4qpzkg5qp7hn2jluraj3ygjavjml440zj5tkcw2xqg6hcrq7havxh",
        "at1l4qkzhyk73vp2y2gk9rrg4ld7elgvy9r4a7u2rz3rtgcjrpuk5gq4denps",
        "at13pw2yxkn7t76lwq2dy4xng4gs4tlre96y2xvgu3we4wjxz9yqcpsat024p",
        "at1yqpwv0tc6349jg5405vz0acczesgwrsgj8ml75vqjq5xvlhjnsgq23nnzj",
    ]