from node.types import *
from .utils import *


def disasm_finalize_type(value: FinalizeType) -> str:
    match value.type:
        case FinalizeType.Type.Public:
            value: PublicFinalize
            return plaintext_type_to_str(value.plaintext_type) + ".public"
        case FinalizeType.Type.Record:
            value: RecordFinalize
            return str(value.identifier) + ".record"
        case FinalizeType.Type.ExternalRecord:
            raise NotImplementedError

def disassemble_program(program: Program) -> str:
    res = disasm_str()
    for i in program.imports:
        i: Import
        res.insert_line(f"import {i.program_id};")
    res.insert_line("")
    res.insert_line(f"program {program.id};")
    res.insert_line("")
    for m in program.mappings.values():
        m: Mapping
        res.insert_line(f"mapping {m.name}:")
        res.indent()
        res.insert_line(f"key {m.key.name} as {disasm_finalize_type(m.key.finalize_type)};")
        res.insert_line(f"value {m.value.name} as {disasm_finalize_type(m.value.finalize_type)};")
        res.unindent()
        res.insert_line("")
    for i in program.interfaces.values():
        i: Interface
        res.insert_line(f"struct {i.name}:")
        res.indent()
        for m, t in i.members:
            res.insert_line(f"{m} as {plaintext_type_to_str(t)};")
        res.unindent()
        res.insert_line("")
    for r in program.records.values():
        r: RecordType
        res.insert_line(f"record {r.name}:")

    return res