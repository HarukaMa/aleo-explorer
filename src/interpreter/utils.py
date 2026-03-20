from hashlib import sha3_256

from aleo_types import *
from db import Database
from node import Network
from .environment import Registers


class FinalizeState:
    def __init__(self, block: Block):
        self.block_height = block.height
        if block.height > Network.consensus_v12_height:
            self.block_timestamp = block.header.metadata.timestamp
        else:
            self.block_timestamp = None
        self.random_seed = aleo_explorer_rust.finalize_random_seed(
            block.round,
            block.height,
            block.cumulative_weight,
            block.cumulative_proof_target,
            block.previous_hash.dump(),
            self.block_timestamp
        )
        if len(self.random_seed) != 32:
            raise RuntimeError("invalid random seed length")

async def load_plaintext_from_operand(operand: Operand, registers: Registers, finalize_state: FinalizeState, db: Database, program: Program) -> Plaintext:
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
            if isinstance(value, PlaintextValue):
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
            elif isinstance(value, FutureValue):
                next_value: Plaintext | Future = value.future
                for access in register.accesses:
                    if isinstance(access, MemberAccess):
                        if not isinstance(next_value, StructPlaintext):
                            raise TypeError("register is not struct")
                        next_value = next_value.get_member(access.identifier)
                    elif isinstance(access, IndexAccess):
                        if isinstance(next_value, Future):
                            argument = next_value.arguments[access.index]
                            if isinstance(argument, FutureArgument):
                                next_value = argument.future
                            elif isinstance(argument, PlaintextArgument):
                                next_value = argument.plaintext
                        if isinstance(next_value, ArrayPlaintext):
                            next_value = next_value[access.index]
                if not isinstance(next_value, Plaintext):
                    raise TypeError("register is not plaintext")
                return next_value
            else:
                raise NotImplementedError
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
    elif isinstance(operand, ChecksumOperand):
        if operand.program_id.value is not None:
            program_id = str(operand.program_id.value)
            latest_edition = await db.get_program_latest_edition(program_id)
            if latest_edition is None:
                raise RuntimeError("program not found")
            program_bytes = await db.get_program(program_id, latest_edition)
            if program_bytes is None:
                raise RuntimeError("program not found")
        else:
            program_bytes = program.dump()
        program_string = aleo_explorer_rust.program_to_string(program_bytes)
        checksum = sha3_256(program_string.encode("utf-8")).digest()
        return ArrayPlaintext(
            elements=Vec[Plaintext, u32]([
                LiteralPlaintext(
                    literal=Literal(
                        type_=Literal.Type.U8,
                        primitive=u8(checksum[i])
                    )
                ) for i in range(32)
            ])
        )
    elif isinstance(operand, EditionOperand):
        if operand.program_id.value is not None:
            program_id = str(operand.program_id.value)
            current_edition = await db.get_program_latest_edition(program_id)
            if current_edition is None:
                raise RuntimeError("program not found")
        else:
            program_id = str(program.id)
            latest_edition = await db.get_program_latest_edition(program_id)
            if latest_edition is None:
                current_edition = 0
            else:
                current_edition = latest_edition + 1
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.U16,
                primitive=u16(current_edition)
            )
        )
    elif isinstance(operand, ProgramOwnerOperand):
        if operand.program_id.value is not None:
            program_id = str(operand.program_id.value)
            latest_edition = await db.get_program_latest_edition(program_id)
            if latest_edition is None:
                raise RuntimeError("program not found")
            program_owner = await db.get_program_owner(program_id, latest_edition)
            if program_owner is None:
                raise AssertionError("program owner is not available")
        else:
            program_owner = registers.owner
            if program_owner is None:
                # from normal finalize
                latest_edition = await db.get_program_latest_edition(str(program.id))
                if latest_edition is None:
                    raise RuntimeError("program not found")
                program_owner = await db.get_program_owner(str(program.id), latest_edition)
                if program_owner is None:
                    raise AssertionError("program owner is not available")
            else:
                program_owner = str(program_owner)
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.Address,
                primitive=Address.loads(program_owner)
            )
        )
    elif isinstance(operand, BlockTimestampOperand):
        return LiteralPlaintext(
            literal=Literal(
                type_=Literal.Type.I64,
                primitive=cast(i64, finalize_state.block_timestamp)
            )
        )
    else:
        raise NotImplementedError

def identifier_from_field(field: Field) -> Identifier:
    """Decode a field element to an Identifier (reverse of Identifier::to_field)."""
    field_bytes = field.dump()
    # Field LE bytes encode the ASCII identifier, terminated by zero bytes
    name_bytes = field_bytes.rstrip(b"\x00")
    if len(name_bytes) == 0:
        raise ValueError("empty identifier")
    return Identifier(value=name_bytes.decode("ascii"))


async def resolve_dynamic_program_mapping(
    operands: list[Operand], registers: Registers, finalize_state: FinalizeState, db: Database, program: Program
) -> tuple[ProgramID, Identifier]:
    """Resolve program_name, program_network, mapping_name operands to (ProgramID, mapping Identifier)."""
    program_name_plaintext = await load_plaintext_from_operand(operands[0], registers, finalize_state, db, program)
    program_network_plaintext = await load_plaintext_from_operand(operands[1], registers, finalize_state, db, program)
    mapping_name_plaintext = await load_plaintext_from_operand(operands[2], registers, finalize_state, db, program)

    if not isinstance(program_name_plaintext, LiteralPlaintext) or not isinstance(program_network_plaintext, LiteralPlaintext) or not isinstance(mapping_name_plaintext, LiteralPlaintext):
        raise TypeError("dynamic operands must be literals")

    program_name = identifier_from_field(cast(Field, program_name_plaintext.literal.primitive))
    program_network = identifier_from_field(cast(Field, program_network_plaintext.literal.primitive))
    mapping_name = identifier_from_field(cast(Field, mapping_name_plaintext.literal.primitive))

    program_id = ProgramID(name=program_name, network=program_network)
    return program_id, mapping_name


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