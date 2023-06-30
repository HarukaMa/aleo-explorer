from node.types import *
from .environment import Registers

class FinalizeState:
    def __init__(self, block: Block):
        self.block_height = block.height
        self.random_seed = bytes(aleo.finalize_random_seed(
            block.round,
            block.height,
            block.cumulative_weight,
            block.cumulative_proof_target,
            block.previous_hash.dump(),
        ))
        if len(self.random_seed) != 32:
            raise RuntimeError("invalid random seed length")

def load_plaintext_from_operand(operand: Operand, registers: Registers, finalize_state: FinalizeState) -> Plaintext:
    match operand.type:
        case Operand.Type.Literal:
            operand: LiteralOperand
            return LiteralPlaintext(literal=operand.literal)
        case Operand.Type.Register:
            operand: RegisterOperand
            register: Register = operand.register
            match register.type:
                case Register.Type.Locator:
                    register: LocatorRegister
                    return registers[int(register.locator)]
                case Register.Type.Member:
                    register: MemberRegister
                    struct_: StructPlaintext = registers[int(register.locator)]
                    if not isinstance(struct_, StructPlaintext):
                        raise TypeError("register is not struct")
                    for i, identifier in enumerate(register.identifiers):
                        if i == len(register.identifiers) - 1:
                            return struct_.get_member(identifier)
                        else:
                            struct_ = struct_.get_member(identifier)
        case Operand.Type.BlockHeight:
            return LiteralPlaintext(
                literal=Literal(
                    type_=Literal.Type.U32,
                    primitive=finalize_state.block_height
                )
            )
        case _:
            raise NotImplementedError

def store_plaintext_to_register(plaintext: Plaintext, register: Register, registers: Registers):
    match register.type:
        case Register.Type.Locator:
            register: LocatorRegister
            registers[int(register.locator)] = plaintext
        case Register.Type.Member:
            register: MemberRegister
            struct_: StructPlaintext = registers[int(register.locator)]
            if not isinstance(struct_, StructPlaintext):
                raise TypeError("register is not struct")
            for i, identifier in enumerate(register.identifiers):
                if i == len(register.identifiers) - 1:
                    struct_.set_member(identifier, plaintext)
                else:
                    struct_ = struct_.get_member(identifier)
        case _:
            raise NotImplementedError