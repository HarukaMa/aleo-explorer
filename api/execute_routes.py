from io import BytesIO
from typing import cast, Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from aleo_types import Program, Identifier, Finalize, LiteralPlaintextType, \
    LiteralPlaintext, Literal, StructPlaintextType, StructPlaintext, FinalizeOperation, Value, \
    PlaintextFinalizeType, FutureFinalizeType, PlaintextValue, Future, FinalizeInput, Argument, PlaintextArgument, \
    FutureValue, FutureArgument, u8, Vec
from api.utils import use_program_cache
from db import Database
from interpreter.finalizer import ExecuteError
from interpreter.interpreter import preview_finalize_execution


class LoadError(Exception):
    def __init__(self, error, status_code):
        self.error = error
        self.status_code = status_code

async def _load_program_finalize_inputs(db, program_id, program_cache, function_name) -> (Program, list[FinalizeInput]):
    try:
        try:
            program = program_cache[program_id]
        except KeyError:
            program_bytes = await db.get_program(program_id)
            if not program_bytes:
                raise LoadError("Program not found", 404)
            program = Program.load(BytesIO(program_bytes))
            program_cache[program_id] = program
    except:
        raise LoadError("Program not found", 404)
    if function_name not in program.functions:
        return JSONResponse({"error": "Transition not found"}, status_code=404)
    function = program.functions[function_name]
    if function.finalize.value is None:
        return JSONResponse({"error": "Transition does not have a finalizer"}, status_code=400)
    finalize: Finalize = function.finalize.value
    return program, finalize.inputs

async def _load_args(db, program, program_cache, input_, finalize_type, index) -> Value:
    if isinstance(finalize_type, PlaintextFinalizeType):
        plaintext_type = finalize_type.plaintext_type
        if isinstance(plaintext_type, LiteralPlaintextType):
            primitive_type = plaintext_type.literal_type.primitive_type
            try:
                value = primitive_type.loads(str(input_))
            except:
                raise LoadError(f"Invalid input for index {index}", 400)
            return PlaintextValue(plaintext=LiteralPlaintext(literal=Literal(type_=Literal.reverse_primitive_type_map[primitive_type], primitive=value)))
        elif isinstance(plaintext_type, StructPlaintextType):
            structs = program.structs
            struct_type = structs[plaintext_type.struct]
            try:
                value = StructPlaintext.loads(input_, struct_type, structs)
            except Exception as e:
                raise LoadError(f"Invalid input for index {index}: {e}", 400)
            return PlaintextValue(plaintext=value)
        else:
            raise RuntimeError("Unknown plaintext type", 500)
    elif isinstance(finalize_type, FutureFinalizeType):
        locator = finalize_type.locator
        program_id = locator.id
        function_name = locator.resource
        args = input_
        if not isinstance(args, list):
            raise LoadError(f"Invalid input for index {index} (future arguments should be an array)", 400)

        future_program, finalize_inputs = await _load_program_finalize_inputs(db, str(program_id), program_cache, function_name)
        arguments: list[Argument] = []
        for arg_index, finalize_input in enumerate(finalize_inputs):
            arg_finalize_type = finalize_input.finalize_type
            if arg_index >= len(args):
                raise LoadError(f"Missing input for index {index}, program {program_id}", 400)
            value = await _load_args(db, future_program, program_cache, args[arg_index], arg_finalize_type, arg_index)
            if isinstance(value, PlaintextValue):
                arguments.append(PlaintextArgument(plaintext=value.plaintext))
            elif isinstance(value, FutureValue):
                arguments.append(FutureArgument(future=value.future))
            else:
                raise RuntimeError("Unknown argument type", 500)

        future = Future(
            program_id=program_id,
            function_name=function_name,
            arguments=Vec[Argument, u8](arguments),
        )
        return FutureValue(future=future)

@use_program_cache
async def preview_finalize_route(request: Request, program_cache: dict[str, Program]):
    db: Database = request.app.state.db
    _ = request.path_params["version"]
    json = await request.json()
    program_id = json.get("program_id")
    transition_name = json.get("transition_name")
    inputs = json.get("inputs")
    if not program_id:
        return JSONResponse({"error": "Missing program_id"}, status_code=400)
    if not transition_name:
        return JSONResponse({"error": "Missing transition_name"}, status_code=400)
    if inputs is None:
        return JSONResponse({"error": "Missing inputs (pass empty array for no input)"}, status_code=400)
    if not isinstance(inputs, list):
        return JSONResponse({"error": "Inputs must be an array"}, status_code=400)
    inputs = cast(list[Any], inputs)

    function_name = Identifier.loads(transition_name)
    try:
        program, finalize_inputs = await _load_program_finalize_inputs(db, program_id, program_cache, function_name)
    except LoadError as e:
        return JSONResponse({"error": e.error}, status_code=e.status_code)
    except Exception as e:
        return JSONResponse({"error": f"Unknown error loading program: {e}"}, status_code=500)
    values: list[Value] = []
    for index, finalize_input in enumerate(finalize_inputs):
        finalize_type = finalize_input.finalize_type
        if index >= len(inputs):
            return JSONResponse({"error": f"Missing input for index {index}"}, status_code=400)
        try:
            values.append(await _load_args(db, program, program_cache, inputs[index], finalize_type, index))
        except LoadError as e:
            return JSONResponse({"error": e.error}, status_code=e.status_code)
        except Exception as e:
            return JSONResponse({"error": f"Unknown error parsing inputs: {e}"}, status_code=500)
    try:
        result = await preview_finalize_execution(db, program, function_name, values)
    except ExecuteError as e:
        return JSONResponse({"error": f"Execution error on instruction \"{e.instruction}\": {e}, at function {e.program}/{e.function_name}"}, status_code=400)
    updates: list[dict[str, str]] = []
    for operation in result:
        operation_type = operation["type"]
        upd = {"type": operation_type.name}
        if operation_type == FinalizeOperation.Type.InitializeMapping:
            raise RuntimeError("InitializeMapping should not be returned by preview_finalize_execution (only used in deployments)")
        elif operation_type == FinalizeOperation.Type.InsertKeyValue:
            raise RuntimeError("InsertKeyValue should not be returned by preview_finalize_execution (only used in tests)")
        elif operation_type == FinalizeOperation.Type.UpdateKeyValue:
            upd.update({
                "mapping_id": str(operation["mapping_id"]),
                "key_id": str(operation["key_id"]),
                "value_id": str(operation["value_id"]),
                "mapping": str(operation["mapping_name"]),
                "key": str(operation["key"]),
                "value": str(operation["value"]),
            })
        elif operation_type == FinalizeOperation.Type.RemoveKeyValue:
            raise NotImplementedError("operation not implemented in the interpreter")
        elif operation_type == FinalizeOperation.Type.RemoveMapping:
            raise RuntimeError("RemoveMapping should not be returned by preview_finalize_execution (only used in tests)")
        else:
            raise RuntimeError("Unknown operation type")
        updates.append(upd)
    return JSONResponse({"mapping_updates": updates})