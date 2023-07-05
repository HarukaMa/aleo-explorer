from .utils import *


def disasm_entry_type(value: EntryType) -> str:
    match value.type:
        case EntryType.Type.Constant:
            visibility = "constant"
        case EntryType.Type.Public:
            visibility = "public"
        case EntryType.Type.Private:
            visibility = "private"
        case _:
            raise ValueError("dead branch")
    return f"{plaintext_type_to_str(value.plaintext_type)}.{visibility}"

def disasm_register(value: Register) -> str:
    match value.type:
        case Register.Type.Locator:
            value: LocatorRegister
            return f"r{value.locator}"
        case Register.Type.Member:
            value: MemberRegister
            locator = f"r{value.locator}."
            identifiers = ".".join(map(str, value.identifiers))
            return locator + identifiers

def disasm_register_type(value: RegisterType) -> str:
    match value.type:
        case RegisterType.Type.Plaintext:
            value: PlaintextRegisterType
            return plaintext_type_to_str(value.plaintext_type)
        case RegisterType.Type.Record:
            value: RecordRegisterType
            return str(value.identifier) + ".record"
        case RegisterType.Type.ExternalRecord:
            value: ExternalRecordRegisterType
            return str(value.locator) + ".record"

def disasm_value_type(value: ValueType) -> str:
    match value.type:
        case ValueType.Type.Constant:
            value: ConstantValueType
            return plaintext_type_to_str(value.plaintext_type) + ".constant"
        case ValueType.Type.Public:
            value: PublicValueType
            return plaintext_type_to_str(value.plaintext_type) + ".public"
        case ValueType.Type.Private:
            value: PrivateValueType
            return plaintext_type_to_str(value.plaintext_type) + ".private"
        case ValueType.Type.Record:
            value: RecordValueType
            return str(value.identifier) + ".record"
        case ValueType.Type.ExternalRecord:
            value: ExternalRecordValueType
            return str(value.locator) + ".record"

def disasm_command(value: Command) -> str:
    match value.type:
        case Command.Type.Instruction:
            value: InstructionCommand
            return disasm_instruction(value.instruction)
        case Command.Type.Contains:
            value: ContainsCommand
            return f"contains {value.mapping}[{disasm_operand(value.key)}] into {disasm_register(value.destination)}"
        case Command.Type.Get:
            value: GetCommand
            return f"get {value.mapping}[{disasm_operand(value.key)}] into {disasm_register(value.destination)}"
        case Command.Type.GetOrUse:
            value: GetOrUseCommand
            return f"get.or_use {value.mapping}[{disasm_operand(value.key)}] {disasm_operand(value.default)} into {disasm_register(value.destination)}"
        case Command.Type.RandChaCha:
            value: RandChaChaCommand
            operands = []
            for i in value.operands:
                operands.append(disasm_operand(i) + " ")
            return f"rand.chacha {''.join(operands)}into {disasm_register(value.destination)} as {value.destination_type}"
        case Command.Type.Remove:
            value: RemoveCommand
            return f"remove {value.mapping}[{disasm_operand(value.key)}]"
        case Command.Type.Set:
            value: SetCommand
            return f"set {disasm_operand(value.value)} into {value.mapping}[{disasm_operand(value.key)}]"
        case Command.Type.BranchEq:
            value: BranchEqCommand
            return f"branch.eq {disasm_operand(value.first)} {disasm_operand(value.second)} to {value.position}"
        case Command.Type.BranchNeq:
            value: BranchNeqCommand
            return f"branch.neq {disasm_operand(value.first)} {disasm_operand(value.second)} to {value.position}"
        case Command.Type.Position:
            value: PositionCommand
            return f"position {value.position}"

def disasm_literal(value: Literal) -> str:
    T = Literal.Type
    match value.type:
        case T.I8 | T.I16 | T.I32 | T.I64 | T.I128 | T.U8 | T.U16 | T.U32 | T.U64 | T.U128:
            return str(value.primitive) + value.type.name.lower()
        case T.Address:
            return aleo.bech32_encode("aleo", value.primitive.dump())
        case T.Field | T.Group | T.Scalar | T.Boolean:
            return str(value.primitive)
    raise NotImplementedError

def disasm_operand(value: Operand) -> str:
    match value.type:
        case Operand.Type.Literal:
            value: LiteralOperand
            return disasm_literal(value.literal)
        case Operand.Type.Register:
            value: RegisterOperand
            return disasm_register(value.register)
        case Operand.Type.ProgramID:
            value: ProgramIDOperand
            return str(value.program_id)
        case Operand.Type.Caller:
            return "self.caller"
        case Operand.Type.BlockHeight:
            return "block.height"
        case _:
            raise ValueError("unknown operand type")

def disasm_call_operator(value: CallOperator) -> str:
    match value.type:
        case CallOperator.Type.Locator:
            value: LocatorCallOperator
            return str(value.locator)
        case CallOperator.Type.Resource:
            value: ResourceCallOperator
            return str(value.resource)

def disasm_literals(value: Literals) -> str:
    operands = []
    for i in range(value.num_operands):
        operands.append(disasm_operand(value.operands[i]))
    return f"{' '.join(operands)} into {disasm_register(value.destination)}"

def disasm_assert(value: AssertInstruction) -> str:
    return " ".join(map(disasm_operand, value.operands))

def disasm_call(value: CallInstruction) -> str:
    return f"{disasm_call_operator(value.operator)} {' '.join(map(disasm_operand, value.operands))} into {' '.join(map(disasm_register, value.destinations))}"

def disasm_cast(value: CastInstruction) -> str:
    cast_type: CastType = value.cast_type
    match cast_type.type:
        case CastType.Type.GroupXCoordinate:
            cast_type: GroupXCoordinateCastType
            destination_type = "group.x"
        case CastType.Type.GroupYCoordinate:
            cast_type: GroupYCoordinateCastType
            destination_type = "group.y"
        case CastType.Type.RegisterType:
            cast_type: RegisterTypeCastType
            destination_type = disasm_register_type(cast_type.register_type)
        case _:
            raise ValueError(f"unknown cast type {cast_type.type}")
    return f"{' '.join(map(disasm_operand, value.operands))} into {disasm_register(value.destination)} as {destination_type}"

def disasm_commit(value: CommitInstruction) -> str:
    return f"{' '.join(map(disasm_operand, value.operands))} into {disasm_register(value.destination)} as {value.destination_type}"

def disasm_hash(value: HashInstruction) -> str:
    return f"{' '.join(map(disasm_operand, value.operands))} into {disasm_register(value.destination)} as {value.destination_type}"

def disasm_instruction(value: Instruction) -> str:
    inst_str = f"{instruction_type_to_str(value.type)} "
    instruction_type = Instruction.type_map[value.type]
    if isinstance(instruction_type, Literals):
        return inst_str + disasm_literals(value.literals)
    elif isinstance(instruction_type, AssertInstruction):
        return inst_str + disasm_assert(value.literals)
    elif instruction_type is CallInstruction:
        return inst_str + disasm_call(value.literals)
    elif instruction_type is CastInstruction:
        return inst_str + disasm_cast(value.literals)
    elif isinstance(instruction_type, CommitInstruction):
        return inst_str + disasm_commit(value.literals)
    elif isinstance(instruction_type, HashInstruction):
        return inst_str + disasm_hash(value.literals)
    else:
        raise NotImplementedError

def disassemble_program(program: Program) -> str:
    res = disasm_str()
    for i in program.imports:
        i: Import
        res.insert_line(f"import {i.program_id};")
    res.insert_line("")
    res.insert_line(f"program {program.id};")
    res.insert_line("")
    for identifier, definition in program.identifiers:
        if definition == ProgramDefinition.Mapping:
            m = program.mappings[identifier]
            res.insert_line(f"mapping {m.name}:")
            res.indent()
            res.insert_line(f"key {m.key.name} as {plaintext_type_to_str(m.key.plaintext_type)};")
            res.insert_line(f"value {m.value.name} as {plaintext_type_to_str(m.value.plaintext_type)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Struct:
            s = program.structs[identifier]
            res.insert_line(f"struct {s.name}:")
            res.indent()
            for m, t in s.members:
                res.insert_line(f"{m} as {plaintext_type_to_str(t)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Record:
            r = program.records[identifier]
            res.insert_line(f"record {r.name}:")
            res.indent()
            res.insert_line(f"owner as address.{public_or_private_to_str(r.owner)};")
            for identifier, entry in r.entries:
                res.insert_line(f"{identifier} as {disasm_entry_type(entry)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Closure:
            c: Closure = program.closures[identifier]
            res.insert_line(f"closure {c.name}:")
            res.indent()
            for i in c.inputs:
                i: ClosureInput
                res.insert_line(f"input {disasm_register(i.register)} as {disasm_register_type(i.register_type)};")
            for i in c.instructions:
                i: Instruction
                res.insert_line(f"{disasm_instruction(i)};")
            for o in c.outputs:
                o: ClosureOutput
                res.insert_line(f"output {disasm_operand(o.operand)} as {disasm_register_type(o.register_type)};")
            res.unindent()
            res.insert_line("")
        elif definition == ProgramDefinition.Function:
            f = program.functions[identifier]
            res.insert_line(f"function {f.name}:")
            res.indent()
            for i in f.inputs:
                i: FunctionInput
                res.insert_line(f"input {disasm_register(i.register)} as {disasm_value_type(i.value_type)};")
            for i in f.instructions:
                i: Instruction
                res.insert_line(f"{disasm_instruction(i)};")
            for o in f.outputs:
                o: FunctionOutput
                res.insert_line(f"output {disasm_operand(o.operand)} as {disasm_value_type(o.value_type)};")
            if f.finalize.value is not None:
                finalize: Finalize
                finalize_command: FinalizeCommand
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
                    i: FinalizeInput
                    res.insert_line(f"input {disasm_register(i.register)} as {plaintext_type_to_str(i.plaintext_type)};")
                for c in finalize.commands:
                    c: Command
                    res.insert_line(f"{disasm_command(c)};")
            res.unindent()
            res.insert_line("")

    return str(res)