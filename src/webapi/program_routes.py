from io import BytesIO
from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request

import disasm.aleo
from aleo_types import DeployTransaction, Program, Deployment, AcceptedDeploy
from db import Database
from webapi.utils import public_cache_seconds, CJSONResponse, function_signature
from webui.classes import UIAddress


@public_cache_seconds(15)
async def programs_route(request: Request) -> CJSONResponse:
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except ValueError:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    no_helloworld = request.query_params.get("no_helloworld", False)
    try:
        no_helloworld = bool(int(no_helloworld))
    except ValueError:
        no_helloworld = False

    total_programs = await db.get_program_count(no_helloworld=no_helloworld)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    programs = await db.get_programs(start, start + 50, no_helloworld=no_helloworld)
    builtin_programs = await db.get_builtin_programs()
    ps: list[dict[str, Any]] = []
    for program in programs + builtin_programs:
        ps.append({
            "id": program["program_id"],
            "called": program["called"],
            "height": program.get("height"),
            "edition": program.get("edition"),
            "transaction_id": program.get("transaction_id"),
        })
    result: dict[str, Any] = {
        "programs": ps,
        "total_pages": total_pages,
        "total_programs": total_programs,
    }
    return CJSONResponse(result)

@public_cache_seconds(15)
async def program_route(request: Request) -> CJSONResponse:
    db: Database = request.app.state.db
    program_id = request.path_params.get("id")
    if program_id is None:
        return CJSONResponse({"error": "Missing program id"}, status_code=400)
    edition = request.path_params.get("edition", "0")
    try:
        edition = int(edition)
    except ValueError:
        return CJSONResponse({"error": "Invalid edition"}, status_code=400)
    if edition < 0:
        return CJSONResponse({"error": "Invalid edition"}, status_code=400)

    latest_edition = await db.get_program_latest_edition(program_id)
    if latest_edition is None:
        return CJSONResponse({"error": "Program not found"}, status_code=404)
    if edition > latest_edition:
        return CJSONResponse({"error": "Edition not found"}, status_code=404)
    block = await db.get_block_by_program_id(program_id, edition)
    if block:
        transaction: DeployTransaction | None = None
        for ct in block.transactions:
            if isinstance(ct, AcceptedDeploy):
                tx = ct.transaction
                if isinstance(tx, DeployTransaction) and str(tx.deployment.program.id) == program_id:
                    transaction = tx
                    break
        if transaction is None:
            raise HTTPException(status_code=550, detail="Deploy transaction not found")
        deployment: Deployment = transaction.deployment
        program: Program = deployment.program
    else:
        program_bytes = await db.get_program(program_id, edition)
        if not program_bytes:
            raise HTTPException(status_code=404, detail="Program not found")
        program = Program.load(BytesIO(program_bytes))
        transaction = None
    functions: list[str] = []
    for f in program.functions.keys():
        functions.append((await function_signature(db, str(program.id), str(f), edition)).split("/", 1)[-1])
    leo_source = await db.get_program_leo_source_code(program_id, edition)
    if leo_source is not None:
        source = leo_source
        has_leo_source = True
    else:
        source = disasm.aleo.disassemble_program(program)
        has_leo_source = False
    mappings: list[dict[str, str]] = []
    for name, mapping in program.mappings.items():
        mappings.append({
            "name": str(name),
            "key_type": str(mapping.key.plaintext_type),
            "value_type": str(mapping.value.plaintext_type)
        })
    address = await db.get_program_address(program_id)
    result: dict[str, Any] = {
        "program_id": str(program.id),
        "times_called": await db.get_program_called_times(program_id),
        "imports": list(map(lambda i: str(i.program_id), program.imports)),
        "mappings": mappings,
        "structs": list(map(str, program.structs.keys())),
        "records": list(map(str, program.records.keys())),
        "closures": list(map(str, program.closures.keys())),
        "functions": functions,
        "source": source,
        "has_leo_source": has_leo_source,
        "recent_calls": await db.get_program_calls(program_id, 0, 50),
        "similar_count": await db.get_program_similar_count(program_id, edition),
        "address": address,
    }
    if transaction:
        result.update({
            "transaction_id": str(transaction.id),
            "owner": str(transaction.owner.address),
            "signature": str(transaction.owner.signature),
        })
    else:
        result.update({
            "transaction_id": None,
            "owner": None,
            "signature": None,
        })

    result["resolved_addresses"] = \
        await UIAddress.resolve_recursive_detached(
            result, db, {}
        )
    return CJSONResponse(result)

@public_cache_seconds(15)
async def similar_programs_route(request: Request) -> CJSONResponse:
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except ValueError:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    program_id = request.path_params.get("id")
    if program_id is None:
        return CJSONResponse({"error": "Missing program id"}, status_code=400)
    edition = request.path_params.get("edition", "0")
    try:
        edition = int(edition)
    except ValueError:
        return CJSONResponse({"error": "Invalid edition"}, status_code=400)
    if edition < 0:
        return CJSONResponse({"error": "Invalid edition"}, status_code=400)
    latest_edition = await db.get_program_latest_edition(program_id)
    if latest_edition is None:
        return CJSONResponse({"error": "Program not found"}, status_code=404)
    if edition > latest_edition:
        return CJSONResponse({"error": "Edition not found"}, status_code=404)
    feature_hash = await db.get_program_feature_hash(program_id, edition)
    if feature_hash is None:
        return CJSONResponse({"error": "Program not found"}, status_code=404)
    total_programs = await db.get_program_similar_count(program_id, edition)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    start = 50 * (page - 1)
    programs = await db.get_programs_with_feature_hash(feature_hash, start, start + 50)

    result = {
        "programs": programs,
        "total_programs": total_programs,
        "total_pages": total_pages,
    }
    return CJSONResponse(result)
