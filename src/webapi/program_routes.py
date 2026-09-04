from io import BytesIO
from typing import Any

from starlette.exceptions import HTTPException
from starlette.requests import Request

import disasm.aleo
from aleo_types import DeployTransaction, Program, Deployment, AcceptedDeploy
from db import Database
from webapi.utils import public_cache_seconds, CJSONResponse, function_signature
from webui.classes import UIAddress

async def _resolve_program_edition(
    request: Request,
    db: Database,
    program_id: str,
) -> tuple[int, int, list[int]]:
    editions = await db.get_program_editions(program_id)
    if not editions:
        raise HTTPException(status_code=404, detail="Program not found")
    latest_edition = editions[-1]
    edition_param = request.path_params.get("edition")
    if edition_param is None:
        height_param = request.query_params.get("height")
        index_param = request.query_params.get("transaction_index")
        if height_param is None and index_param is None:
            return latest_edition, latest_edition, editions
        if height_param is None or index_param is None:
            raise HTTPException(status_code=400, detail="Height and transaction index must be provided together")
        try:
            height = int(height_param)
            transaction_index = int(index_param)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid program context") from None
        if height < 0 or transaction_index < 0:
            raise HTTPException(status_code=400, detail="Invalid program context")
        edition = await db.get_program_edition_at_context(program_id, height, transaction_index)
        if edition is None:
            raise HTTPException(status_code=404, detail="Program not found at transaction")
        return edition, latest_edition, editions
    try:
        edition = int(edition_param)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid edition") from None
    if edition not in editions:
        raise HTTPException(status_code=404, detail="Edition not found")
    return edition, latest_edition, editions


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
    edition, latest_edition, editions = await _resolve_program_edition(request, db, program_id)
    block = await db.get_block_by_program_id(program_id, edition)
    if block:
        transaction: DeployTransaction | None = None
        transaction_index: int | None = None
        for ct in block.transactions:
            if isinstance(ct, AcceptedDeploy):
                tx = ct.transaction
                if isinstance(tx, DeployTransaction) and str(tx.deployment.program.id) == program_id:
                    transaction = tx
                    transaction_index = int(ct.index)
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
        transaction_index = None
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
    imports: list[dict[str, Any]] = []
    for imported in program.imports:
        imported_program_id = str(imported.program_id)
        if block is not None and transaction_index is not None:
            imported_edition = await db.get_program_edition_at_context(
                imported_program_id,
                int(block.height),
                transaction_index,
            )
        else:
            imported_edition = await db.get_program_latest_edition(imported_program_id)
        if imported_edition is None:
            raise HTTPException(status_code=550, detail=f"Imported program {imported_program_id} not found")
        imports.append({"program_id": imported_program_id, "edition": imported_edition})
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
        "edition": edition,
        "latest_edition": latest_edition,
        "editions": editions,
        "times_called": await db.get_program_called_times(program_id, edition),
        "imports": imports,
        "mappings": mappings,
        "structs": list(map(str, program.structs.keys())),
        "records": list(map(str, program.records.keys())),
        "closures": list(map(str, program.closures.keys())),
        "functions": functions,
        "source": source,
        "has_leo_source": has_leo_source,
        "recent_calls": await db.get_program_calls(program_id, 0, 50, edition),
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
    edition, latest_edition, _ = await _resolve_program_edition(request, db, program_id)
    feature_hash = await db.get_program_feature_hash(program_id, edition)
    if feature_hash is None:
        return CJSONResponse({"error": "Program not found"}, status_code=404)
    total_programs = await db.get_program_similar_count(program_id, edition)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        return CJSONResponse({"error": "Invalid page"}, status_code=400)
    start = 50 * (page - 1)
    programs = await db.get_programs_with_feature_hash(feature_hash, program_id, start, start + 50)

    result = {
        "programs": programs,
        "edition": edition,
        "latest_edition": latest_edition,
        "total_programs": total_programs,
        "total_pages": total_pages,
    }
    return CJSONResponse(result)
