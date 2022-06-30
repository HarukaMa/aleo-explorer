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

    non_canonical_ledger_roots = [
        "al1gesxhq6vwwa2xx3uh8w5pcfsg7zh942c3tlqhdn5c8wt8lh5tvqqpu882x",

        "al1c2agd5u90kpvcjvk70rgrhnkagfw4kfgusym9t0mjyr8yl0m6qyqez0q6j",
        "al1cp6qpzgt0elcpyk95p9vfme7p29ytp0rccnctx602fdc9uf2zv9qjtx35r",
        "al1tm27a5wukgxv6ks5fr7aa90zsvstu2ruplxkp5tena3tae8nxgfqldxqec",
        "al1fyspkzz7uq594k5wkmktwujlawkl0tf0w3tatarpwq5gs37lj5rs06f30h",
        "al1rwsj7nydhunppu268aqdaxd465rel9wx9wrqmamtymmrxt3j0ypsg25usd",

        "al1r9ulfc9kcdmj9hpjf2e2yqgahuj0ffx0tns6awp6nqa2lzm6l59qe3sm29",
        "al14xmsrdexxj2ce2jejt2ely0pludyarfwmal32zn44tfvqyttcyxqw27m75",
    ]
