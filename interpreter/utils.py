from node.types import *
from .environment import Registers

MappingCacheTuple = tuple[Field, Field, Plaintext, Value]

class FinalizeState:
    def __init__(self, block: Block):
        self.block_height = block.height
        self.random_seed = aleo.finalize_random_seed(
            block.round,
            block.height,
            block.cumulative_weight,
            block.cumulative_proof_target,
            block.previous_hash.dump(),
        )
        if len(self.random_seed) != 32:
            raise RuntimeError("invalid random seed length")

def load_plaintext_from_operand(operand: Operand, registers: Registers, finalize_state: FinalizeState) -> Plaintext:
    if isinstance(operand, LiteralOperand):
        return LiteralPlaintext(literal=operand.literal)
    elif isinstance(operand, RegisterOperand):
        register = operand.register
        if isinstance(register, LocatorRegister):
            return registers[int(register.locator)]
        elif isinstance(register, MemberRegister):
            struct_ = registers[int(register.locator)]
            if not isinstance(struct_, StructPlaintext):
                raise TypeError("register is not struct")
            for i, identifier in enumerate(register.identifiers):
                if i == len(register.identifiers) - 1:
                    return struct_.get_member(identifier)
                else:
                    struct_ = struct_.get_member(identifier)
                    if not isinstance(struct_, StructPlaintext):
                        raise TypeError("register is not struct")
            raise ValueError("unreachable")
        else:
            raise NotImplementedError
    elif isinstance(operand, BlockHeightOperand):
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.U32,
                primitive=finalize_state.block_height
            )
        )
    else:
        raise NotImplementedError

def store_plaintext_to_register(plaintext: Plaintext, register: Register, registers: Registers):
    if isinstance(register, LocatorRegister):
        registers[int(register.locator)] = plaintext
    elif isinstance(register, MemberRegister):
        struct_ = registers[int(register.locator)]
        if not isinstance(struct_, StructPlaintext):
            raise TypeError("register is not struct")
        for i, identifier in enumerate(register.identifiers):
            if i == len(register.identifiers) - 1:
                struct_.set_member(identifier, plaintext)
            else:
                struct_ = struct_.get_member(identifier)
                if not isinstance(struct_, StructPlaintext):
                    raise TypeError("register is not struct")
    else:
        raise NotImplementedError