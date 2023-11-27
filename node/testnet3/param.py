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
        "at16xptx2nmy3esnq0czrlmw0455tr0eh5lwsy799rh09c2p5hjvyxswgsxlz",
        "at17hhces6pyq6zpldjjjglagvuugkekrwx3feeajz3ltuqd50vyszsjjhc49",
        "at1y9qwar42k4tfczuae3pcxayg9rx5hgdhjeem5vgw0etaun79xsxsu9r597",
        "at1pu2sx90vkwpg75gx8nn6adw36tf52f5aepjm0dxeqfpwx9a9qqgqcpdzp0",
        "at1rw3zedpapn06eshz4tysjskqcnu5c8ttryffau5jmz40gly8qu9q4eaxt9",
        "at1jutwx9slyl3nvrjje7a89h30ka9yznv2lhnaaahtx38rc3sxxv8qnr5aay",
        "at19c4j4jfh0kezksfllc3pvs0lka60mkpzxdzq2grvmgqnaxawpuzsg7klcw",
        "at1dxptf7fe5czsapywq4v3wqssdtszgaufma7a747m7z3fyc22ysyq5v3yyp",
        "at1z32dca66erulmpetw9yvtwccqkhf292dpqefc3c0ev0u8eskmcpqym0fkq",
        "at1l2ts29g7zwk9w3uke5w83slhapr8v7zajw4d73xezf8fpszljqzq0y4ck5",
        "at1zfjxaj0dtve4707cr74yvz57ahg7y3s80e888g36k7k0u0fe75gqqjck8j",
        "at12ucxq7f77xazujku3nk9ewvc68tw0rten6dlsalj3edvdsz4gy9qx3jtwc",
        "at15rq5mg88ex6mwy4wjjcn2zv655r4qcp6gn30jfkf5nrmngfljuyskxvacl",
        "at10jxxtckj7n7fxs3thpdaxhn5l9v939agrjz7nvrtrturuxrg45xqtdqpku",
        "at1jf37fyjlzg5tzazdaqqg734jx9sm0glktklsaw8m4hh5t6nlavpsveh88w",
        "at1gp2tjas244jlr5jv8undlvjgtdwhv9fu2hdf0f9z8y6pqepfqugsta57j2",
        "at1lgp8lsmpt0ccc6ajqc6hacsl22htpk7008ufeut9srgse373xgrsjs4nw0",
    ]