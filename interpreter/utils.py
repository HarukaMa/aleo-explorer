from aleo_types import *
from node.testnet import Testnet as Network
from .environment import Registers


class FinalizeState:
    def __init__(self, block: Block):
        self.block_height = block.height
        self.random_seed = aleo_explorer_rust.finalize_random_seed(
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
            value = registers[int(register.locator)]
            if not isinstance(value, PlaintextValue):
                raise TypeError("register is not plaintext")
            return value.plaintext
        elif isinstance(register, AccessRegister):
            value = registers[int(register.locator)]
            if not isinstance(value, PlaintextValue):
                raise TypeError("register is not plaintext")
            plaintext = value.plaintext
            for access in register.accesses:
                if isinstance(access, MemberAccess):
                    if not isinstance(plaintext, StructPlaintext):
                        raise TypeError("register is not struct")
                    plaintext = plaintext.get_member(access.identifier)
                elif isinstance(access, IndexAccess):
                    if not isinstance(plaintext, ArrayPlaintext):
                        raise TypeError("register is not array")
                    plaintext = plaintext[access.index]
            return plaintext
        else:
            raise NotImplementedError
    elif isinstance(operand, BlockHeightOperand):
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.U32,
                primitive=finalize_state.block_height
            )
        )
    elif isinstance(operand, ProgramIDOperand):
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads(aleo_explorer_rust.program_id_to_address(str(operand.program_id)))
            )
        )
    elif isinstance(operand, NetworkIDOperand):
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.U16,
                primitive=Network.network_id
            )
        )
    else:
        raise NotImplementedError

def load_future_from_operand(operand: Operand, registers: Registers, finalize_state: FinalizeState) -> Future:
    if not isinstance(operand, RegisterOperand):
        raise ValueError("operand is not register")
    register = operand.register
    return load_future_from_register(register, registers, finalize_state)

def load_future_from_register(register: Register, registers: Registers, finalize_state: FinalizeState) -> Future:
    if not isinstance(register, LocatorRegister):
        raise ValueError("register is not locator")
    value = registers[int(register.locator)]
    if not isinstance(value, FutureValue):
        raise TypeError("register is not future")
    return value.future

def store_plaintext_to_register(plaintext: Plaintext, register: Register, registers: Registers):
    if isinstance(register, LocatorRegister):
        registers[int(register.locator)] = PlaintextValue(plaintext=plaintext)
    # elif isinstance(register, AccessRegister):
    #     struct_ = registers[int(register.locator)]
    #     if not isinstance(struct_, StructPlaintext):
    #         raise TypeError("register is not struct")
    #     for i, identifier in enumerate(register.identifiers):
    #         if i == len(register.identifiers) - 1:
    #             struct_.set_member(identifier, plaintext)
    #         else:
    #             struct_ = struct_.get_member(identifier)
    #             if not isinstance(struct_, StructPlaintext):
    #                 raise TypeError("register is not struct")
    else:
        raise NotImplementedError