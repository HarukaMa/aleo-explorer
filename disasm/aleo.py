from .utils import *


def disasm_entry_type(value: EntryType) -> str:
    match value.type:
        case EntryType.Type.Constant:
            visibility = "constant"
        case EntryType.Type.Public:
            visibility = "public"
        case EntryType.Type.Private:
            visibility = "private"
    # noinspection PyUnboundLocalVariable
    return f"{plaintext_type_to_str(value.plaintext_type)}.{visibility}"

def disasm_register(value: Register) -> str:
    if isinstance(value, LocatorRegister):
        return f"r{value.locator}"
    elif isinstance(value, MemberRegister):
        locator = f"r{value.locator}."
        identifiers = ".".join(map(str, value.identifiers))
        return locator + identifiers
    else:
        raise TypeError("invalid register type")

def disasm_register_type(value: RegisterType) -> str:
    if isinstance(value, PlaintextRegisterType):
        return plaintext_type_to_str(value.plaintext_type)
    elif isinstance(value, RecordRegisterType):
        return str(value.identifier) + ".record"
    elif isinstance(value, ExternalRecordRegisterType):
        return str(value.locator) + ".record"
    else:
        raise TypeError("invalid register type")

def disasm_value_type(value: ValueType) -> str:
    if isinstance(value, ConstantValueType):
        return plaintext_type_to_str(value.plaintext_type) + ".constant"
    elif isinstance(value, PublicValueType):
        return plaintext_type_to_str(value.plaintext_type) + ".public"
    elif isinstance(value, PrivateValueType):
        return plaintext_type_to_str(value.plaintext_type) + ".private"
    elif isinstance(value, RecordValueType):
        return str(value.identifier) + ".record"
    elif isinstance(value, ExternalRecordValueType):
        return str(value.locator) + ".record"
    else:
        raise TypeError("invalid value type")

def disasm_command(value: Command) -> str:
    if isinstance(value, InstructionCommand):
        return disasm_instruction(value.instruction)
    elif isinstance(value, ContainsCommand):
        return f"contains {value.mapping}[{disasm_operand(value.key)}] into {disasm_register(value.destination)}"
    elif isinstance(value, GetCommand):
        return f"get {value.mapping}[{disasm_operand(value.key)}] into {disasm_register(value.destination)}"
    elif isinstance(value, GetOrUseCommand):
        return f"get.or_use {value.mapping}[{disasm_operand(value.key)}] {disasm_operand(value.default)} into {disasm_register(value.destination)}"
    elif isinstance(value, RandChaChaCommand):
        operands: list[str] = []
        for i in value.operands:
            operands.append(disasm_operand(i) + " ")
        return f"rand.chacha {''.join(operands)}into {disasm_register(value.destination)} as {value.destination_type}"
    elif isinstance(value, RemoveCommand):
        return f"remove {value.mapping}[{disasm_operand(value.key)}]"
    elif isinstance(value, SetCommand):
        return f"set {disasm_operand(value.value)} into {value.mapping}[{disasm_operand(value.key)}]"
    elif isinstance(value, BranchEqCommand):
        return f"branch.eq {disasm_operand(value.first)} {disasm_operand(value.second)} to {value.position}"
    elif isinstance(value, BranchNeqCommand):
        return f"branch.neq {disasm_operand(value.first)} {disasm_operand(value.second)} to {value.position}"
    elif isinstance(value, PositionCommand):
        return f"position {value.position}"
    else:
        raise TypeError("invalid command type")

def disasm_literal(value: Literal) -> str:
    LT = Literal.Type
    match value.type:
        case LT.I8 | LT.I16 | LT.I32 | LT.I64 | LT.I128 | LT.U8 | LT.U16 | LT.U32 | LT.U64 | LT.U128:
            return str(value.primitive) + value.type.name.lower()
        case LT.Address:
            return aleo.bech32_encode("aleo", value.primitive.dump())
        case LT.Field | LT.Group | LT.Scalar | LT.Boolean:
            return str(value.primitive)
        case _:
            raise NotImplementedError

def disasm_operand(value: Operand) -> str:
    if isinstance(value, LiteralOperand):
        return disasm_literal(value.literal)
    elif isinstance(value, RegisterOperand):
        return disasm_register(value.register)
    elif isinstance(value, ProgramIDOperand):
        return str(value.program_id)
    elif isinstance(value, CallerOperand):
        return "self.caller"
    elif isinstance(value, BlockHeightOperand):
        return "block.height"
    else:
        raise ValueError("unknown operand type")

def disasm_call_operator(value: CallOperator) -> str:
    if isinstance(value, LocatorCallOperator):
        return str(value.locator)
    elif isinstance(value, ResourceCallOperator):
        return str(value.resource)
    else:
        raise ValueError("unknown call operator type")

def disasm_literals(value: Literals[Any]) -> str:
    operands: list[str] = []
    for i in range(value.num_operands):
        operands.append(disasm_operand(value.operands[i]))
    return f"{' '.join(operands)} into {disasm_register(value.destination)}"

def disasm_assert(value: AssertInstruction[Any]) -> str:
    return " ".join(map(disasm_operand, value.operands))

def disasm_call(value: CallInstruction) -> str:
    return f"{disasm_call_operator(value.operator)} {' '.join(map(disasm_operand, value.operands))} into {' '.join(map(disasm_register, value.destinations))}"

def disasm_cast(value: CastInstruction) -> str:
    cast_type: CastType = value.cast_type
    if isinstance(cast_type, GroupXCoordinateCastType):
        destination_type = "group.x"
    elif isinstance(cast_type, GroupYCoordinateCastType):
        destination_type = "group.y"
    elif isinstance(cast_type, RegisterTypeCastType):
        destination_type = disasm_register_type(cast_type.register_type)
    else:
        raise ValueError(f"unknown cast type")
    return f"{' '.join(map(disasm_operand, value.operands))} into {disasm_register(value.destination)} as {destination_type}"

def disasm_commit(value: CommitInstruction[Any]) -> str:
    return f"{' '.join(map(disasm_operand, value.operands))} into {disasm_register(value.destination)} as {value.destination_type}"

def disasm_hash(value: HashInstruction[Any]) -> str:
    operands: list[str] = []
    for i in range(value.num_operands(value.type)):
        operand = value.operands[i]
        if operand is None:
            raise ValueError("invalid operand")
        operands.append(disasm_operand(operand))
    return f"{' '.join(operands)} into {disasm_register(value.destination)} as {value.destination_type}"

def disasm_instruction(value: Instruction) -> str:
    inst_str = f"{instruction_type_to_str(value.type)} "
    literals = value.literals
    if isinstance(literals, Literals):
        return inst_str + disasm_literals(literals)
    elif isinstance(literals, AssertInstruction):
        return inst_str + disasm_assert(literals)
    elif isinstance(literals, CallInstruction):
        return inst_str + disasm_call(literals)
    elif isinstance(literals, CastInstruction):
        return inst_str + disasm_cast(literals)
    elif isinstance(literals, CommitInstruction):
        return inst_str + disasm_commit(literals)
    else:
        return inst_str + disasm_hash(literals)

def disassemble_program(program: Program) -> str:
    res = disasm_str()
    for i in program.imports:
        res.insert_line(f"import {i.program_id};")
    res.insert_line("")
    res.insert_line(f"program {program.id};")
    res.insert_line("")
    for identifier, definition in program.identifiers:
        if definition == ProgramDefinition.Mapping:
            m = program.mappings[identifier]
            res.insert_line(f"mapping {m.name}:")
            res.indent()
            res.insert_line(f"key as {plaintext_type_to_str(m.key.plaintext_type)};")
            res.insert_line(f"value as {plaintext_type_to_str(m.value.plaintext_type)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Struct:
            s = program.structs[identifier]
            res.insert_line(f"struct {s.name}:")
            res.indent()
            for m, t in s.members:
                res.insert_line(f"{m} as {plaintext_type_to_str(t)}.public;")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Record:
            r = program.records[identifier]
            res.insert_line(f"record {r.name}:")
            res.indent()
            res.insert_line(f"owner as address.{public_or_private_to_str(r.owner)};")
            for identifier_, entry in r.entries:
                res.insert_line(f"{identifier_} as {disasm_entry_type(entry)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Closure:
            c = program.closures[identifier]
            res.insert_line(f"closure {c.name}:")
            res.indent()
            for i in c.inputs:
                res.insert_line(f"input {disasm_register(i.register)} as {disasm_register_type(i.register_type)};")
            for i in c.instructions:
                res.insert_line(f"{disasm_instruction(i)};")
            for o in c.outputs:
                res.insert_line(f"output {disasm_operand(o.operand)} as {disasm_register_type(o.register_type)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Function:
            f = program.functions[identifier]
            res.insert_line(f"function {f.name}:")
            res.indent()
            for i in f.inputs:
                res.insert_line(f"input {disasm_register(i.register)} as {disasm_value_type(i.value_type)};")
            for i in f.instructions:
                res.insert_line(f"{disasm_instruction(i)};")
            for o in f.outputs:
                res.insert_line(f"output {disasm_operand(o.operand)} as {disasm_value_type(o.value_type)};")
            if f.finalize.value is not None:
                finalize_command, finalize = f.finalize.value
                if len(finalize_command.operands) > 0:
                    res.insert_line(f"finalize {' '.join(map(disasm_operand, finalize_command.operands))};")
                else:
                    res.insert_line("finalize;")
                res.unindent()
                res.insert_line("")
                res.insert_line(f"finalize {finalize.name}:")
                res.indent()
                for i in finalize.inputs:
                    res.insert_line(f"input {disasm_register(i.register)} as {plaintext_type_to_str(i.plaintext_type)};")
                for c in finalize.commands:
                    res.insert_line(f"{disasm_command(c)};")
            res.unindent()
            res.insert_line("")

    return str(res)