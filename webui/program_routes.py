import aleo
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

import disasm.aleo
from db import Database
from node.types import Transaction, DeployTransaction, Deployment, Program, \
    ConfirmedTransaction, AcceptedDeploy, Import
from .template import templates
from .utils import function_signature, out_of_sync_check


async def programs_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    no_helloworld = request.query_params.get("no_helloworld")
    try:
        no_helloworld = bool(int(no_helloworld))
    except:
        no_helloworld = False
    total_programs = await db.get_program_count(no_helloworld=no_helloworld)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    programs = await db.get_programs(start, start + 50, no_helloworld=no_helloworld)

    maintenance, info = await out_of_sync_check(db)
    ctx = {
        "request": request,
        "programs": programs,
        "page": page,
        "total_pages": total_pages,
        "no_helloworld": no_helloworld,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('programs.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def program_route(request: Request):
    db: Database = request.app.state.db
    program_id = request.query_params.get("id")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    block = await db.get_block_by_program_id(program_id)
    if block is None:
        raise HTTPException(status_code=404, detail="Program not found")
    transaction: DeployTransaction | None = None
    for ct in block.transactions:
        if ct.type == ConfirmedTransaction.Type.AcceptedDeploy:
            ct: AcceptedDeploy
            tx: Transaction = ct.transaction
            tx: DeployTransaction
            if str(tx.deployment.program.id) == program_id:
                transaction = tx
                break
    if transaction is None:
        raise HTTPException(status_code=550, detail="Deploy transaction not found")
    deployment: Deployment = transaction.deployment
    program: Program = deployment.program
    functions = []
    for f in program.functions.keys():
        functions.append((await function_signature(db, str(program.id), str(f))).split("/", 1)[-1])
    leo_source = await db.get_program_leo_source_code(program_id)
    if leo_source is not None:
        source = leo_source
        has_leo_source = True
    else:
        source = disasm.aleo.disassemble_program(program)
        has_leo_source = False
    ctx = {
        "request": request,
        "program_id": str(program.id),
        "transaction_id": str(transaction.id),
        "owner": str(transaction.owner.address),
        "signature": str(transaction.owner.signature),
        "times_called": await db.get_program_called_times(program_id),
        "imports": list(map(lambda i: str(i.program_id), program.imports)),
        "mappings": list(map(str, program.mappings.keys())),
        "structs": list(map(str, program.structs.keys())),
        "records": list(map(str, program.records.keys())),
        "closures": list(map(str, program.closures.keys())),
        "functions": functions,
        "source": source,
        "has_leo_source": has_leo_source,
        "recent_calls": await db.get_program_calls(program_id, 0, 30),
        "similar_count": await db.get_program_similar_count(program_id),
    }
    return templates.TemplateResponse('program.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def similar_programs_route(request: Request):
    db: Database = request.app.state.db
    try:
        page = request.query_params.get("p")
        if page is None:
            page = 1
        else:
            page = int(page)
    except:
        raise HTTPException(status_code=400, detail="Invalid page")
    program_id = request.query_params.get("id")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    feature_hash = await db.get_program_feature_hash(program_id)
    if feature_hash is None:
        raise HTTPException(status_code=404, detail="Program not found")
    total_programs = await db.get_program_similar_count(program_id)
    total_pages = (total_programs // 50) + 1
    if page < 1 or page > total_pages:
        raise HTTPException(status_code=400, detail="Invalid page")
    start = 50 * (page - 1)
    programs = await db.get_programs_with_feature_hash(feature_hash, start, start + 50)

    maintenance, info = await out_of_sync_check(db)
    ctx = {
        "request": request,
        "program_id": program_id,
        "programs": programs,
        "page": page,
        "total_pages": total_pages,
        "maintenance": maintenance,
        "info": info,
    }
    return templates.TemplateResponse('similar_programs.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})


async def upload_source_route(request: Request):
    db: Database = request.app.state.db
    program_id = request.query_params.get("id")
    if program_id is None:
        raise HTTPException(status_code=400, detail="Missing program id")
    program = await db.get_program_bytes(program_id)
    if program is None:
        raise HTTPException(status_code=404, detail="Program not found")
    if request.method == "POST":
        form = await request.form()
        source = form.get("source")
    else:
        source = ""
    if (await db.get_program_leo_source_code(program_id)) is not None:
        has_leo_source = True
        has_imports = None
    else:
        has_leo_source = False
        program = Program.load(bytearray(program))
        has_imports = False
        for i in program.imports:
            i: Import
            if str(i.program_id) != "credits.aleo":
                has_imports = True
                break
    message = request.query_params.get("message")
    ctx = {
        "request": request,
        "program_id": program_id,
        "has_imports": has_imports,
        "has_leo_source": has_leo_source,
        "message": message,
        "source": source,
    }
    return templates.TemplateResponse('upload_source.jinja2', ctx, headers={'Cache-Control': 'public, max-age=15'})

async def submit_source_route(request: Request):
    db: Database = request.app.state.db
    form = await request.form()
    program_id = form.get("id")
    if program_id is None:
        return RedirectResponse(url=f"/upload_source?id={program_id}&message=Missing program id")
    program = await db.get_program_bytes(program_id)
    if program is None:
        return RedirectResponse(url=f"/upload_source?id={program_id}&message=Program not found")
    source = form.get("source")
    if source is None or source == "":
        return RedirectResponse(url=f"/upload_source?id={program_id}&message=Missing source code")
    try:
        compiled = bytes(aleo.compile_program(source, program_id.split(".")[0]))
    except RuntimeError as e:
        return RedirectResponse(url=f"/upload_source?id={program_id}&message=Failed to compile source code: {e}")
    print(program)
    print(compiled)
    return RedirectResponse(url=f"/upload_source?id={program_id}&message=Test")
