from io import BytesIO
from typing import Any, cast, Optional

from starlette.requests import Request

from aleo_types import LiteralPlaintext, Literal, Address, PlaintextValue, Value, Int, StructPlaintext, u64
from aleo_types.cached import cached_get_key_id
from db import Database
from webapi.utils import CJSONResponse, public_cache_seconds
from webui.classes import UIAddress


@public_cache_seconds(5)
async def address_route(request: Request) -> CJSONResponse:
    db: Database = request.app.state.db
    address = request.path_params["address"]
    if not address:
        return CJSONResponse({"error": "Address not found"}, status_code=404)
    solutions = await db.get_recent_solutions_by_address(address)
    programs = await db.get_recent_programs_by_address(address)
    transitions = await db.get_address_recent_transitions(address)
    address_key = LiteralPlaintext(
        literal=Literal(
            type_=Literal.Type.Address,
            primitive=Address.loads(address),
        )
    )
    address_key_bytes = address_key.dump()
    account_key_id = cached_get_key_id("credits.aleo", "account", address_key_bytes)
    bonded_key_id = cached_get_key_id("credits.aleo", "bonded", address_key_bytes)
    unbonding_key_id = cached_get_key_id("credits.aleo", "unbonding", address_key_bytes)
    committee_key_id = cached_get_key_id("credits.aleo", "committee", address_key_bytes)
    delegated_key_id = cached_get_key_id("credits.aleo", "delegated", address_key_bytes)
    withdraw_key_id = cached_get_key_id("credits.aleo", "withdraw", address_key_bytes)
    public_balance_bytes = await db.get_mapping_value("credits.aleo", "account", account_key_id)
    bond_state_bytes = await db.get_mapping_value("credits.aleo", "bonded", bonded_key_id)
    unbond_state_bytes = await db.get_mapping_value("credits.aleo", "unbonding", unbonding_key_id)
    committee_state_bytes = await db.get_mapping_value("credits.aleo", "committee", committee_key_id)
    delegated_bytes = await db.get_mapping_value("credits.aleo", "delegated", delegated_key_id)
    stake_reward = await db.get_address_stake_reward(address)
    transfer_in = await db.get_address_transfer_in(address)
    transfer_out = await db.get_address_transfer_out(address)
    fee = await db.get_address_total_fee(address)
    program_name = await db.get_program_name_from_address(address)

    if (len(solutions) == 0
        and len(programs) == 0
        and len(transitions) == 0
        and public_balance_bytes is None
        and bond_state_bytes is None
        and unbond_state_bytes is None
        and committee_state_bytes is None
        and delegated_bytes is None
        and stake_reward is None
        and transfer_in is None
        and transfer_out is None
        and fee is None
        and program_name is None
    ):
        return CJSONResponse({"error": "Address not found"}, status_code=404)

    if len(solutions) > 0:
        solution_count = await db.get_solution_count_by_address(address)
        total_rewards = await db.get_puzzle_reward_by_address(address)
        speed, interval = await db.get_address_speed(address)
    else:
        solution_count = 0
        total_rewards = 0
        speed = 0
        interval = 0
    program_count = await db.get_program_count_by_address(address)
    recent_solutions: list[dict[str, Any]] = []
    for solution in solutions:
        recent_solutions.append({
            "height": solution["height"],
            "timestamp": solution["timestamp"],
            "reward": solution["reward"],
            "counter": solution["counter"],
            "target": solution["target"],
            "target_sum": solution["target_sum"],
            "solution_id": solution["solution_id"],
        })

    recent_programs: list[dict[str, Any]] = []
    for program in programs:
        deploy_info = await db.get_deploy_info_by_program_id(program)
        if deploy_info is None:
            return CJSONResponse({"error": "Program not found"}, status_code=500)
        recent_programs.append({
            "program_id": program,
            "height": deploy_info["height"],
            "timestamp": deploy_info["timestamp"],
            "transaction_id": deploy_info["transaction_id"],
        })
    if public_balance_bytes is None:
        public_balance = 0
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(public_balance_bytes)))
        plaintext = cast(LiteralPlaintext, value.plaintext)
        public_balance = int(cast(Int, plaintext.literal.primitive))
    if bond_state_bytes is None:
        bond_state = None
        withdrawal_address = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(bond_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        validator = cast(LiteralPlaintext, plaintext["validator"])
        amount = cast(LiteralPlaintext, plaintext["microcredits"])
        bond_state = {
            "validator": str(validator.literal.primitive),
            "amount": int(cast(Int, amount.literal.primitive)),
        }
        withdraw_bytes = await db.get_mapping_value("credits.aleo", "withdraw", withdraw_key_id)
        if withdraw_bytes is None:
            withdrawal_address = None
        else:
            value = cast(PlaintextValue, Value.load(BytesIO(withdraw_bytes)))
            plaintext = cast(LiteralPlaintext, value.plaintext)
            withdrawal_address = str(plaintext.literal.primitive)
    if unbond_state_bytes is None:
        unbond_state = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(unbond_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        amount = cast(LiteralPlaintext, plaintext["microcredits"])
        height = cast(LiteralPlaintext, plaintext["height"])
        unbond_state = {
            "amount": int(cast(Int, amount.literal.primitive)),
            "height": int(cast(u64, height.literal.primitive)),
        }
    if committee_state_bytes is None:
        committee_state = None
        address_stakes: Optional[dict[str, int]] = None
        uptime = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(committee_state_bytes)))
        plaintext = cast(StructPlaintext, value.plaintext)
        commission = cast(LiteralPlaintext, plaintext["commission"])
        is_open = cast(LiteralPlaintext, plaintext["is_open"])
        committee_state = {
            "commission": int(cast(Int, commission.literal.primitive)),
            "is_open": bool(is_open.literal.primitive),
        }
        bonded_mapping = await db.get_bonded_mapping_unchecked()
        bonded_mapping = sorted(bonded_mapping.items(), key=lambda x: x[1][1], reverse=True)
        address_stakes = {}
        for staker_addr, (validator_addr, stake_amount) in bonded_mapping:
            if str(validator_addr) == address:
                address_stakes[str(staker_addr)] = int(stake_amount)
                if len(address_stakes) >= 50:
                    break
        uptime = await db.get_validator_uptime(address)
    if delegated_bytes is None:
        delegated = None
    else:
        value = cast(PlaintextValue, Value.load(BytesIO(delegated_bytes)))
        plaintext = cast(LiteralPlaintext, value.plaintext)
        delegated = int(cast(Int, plaintext.literal.primitive))
    if stake_reward is None:
        stake_reward = 0
    if transfer_in is None:
        transfer_in = 0
    if transfer_out is None:
        transfer_out = 0
    if fee is None:
        fee = 0

    recent_transitions: list[dict[str, Any]] = []
    for transition_data in transitions:
        transition = await db.get_transition(transition_data["transition_id"])
        if transition is None:
            return CJSONResponse({"error": "Transition not found"}, status_code=500)
        recent_transitions.append({
            "transition_id": transition_data["transition_id"],
            "height": transition_data["height"],
            "timestamp": transition_data["timestamp"],
            "program_id": transition.program_id,
            "function_name": transition.function_name,
        })

    result = {
        "address": address,
        "solutions": recent_solutions,
        "programs": recent_programs,
        "total_rewards": total_rewards,
        "total_solutions": solution_count,
        "total_programs": program_count,
        "speed": speed,
        "speed_interval": interval,
        "public_balance": public_balance,
        "bond_state": bond_state,
        "unbond_state": unbond_state,
        "committee_state": committee_state,
        "address_stakes": address_stakes,
        "delegated": delegated,
        "withdrawal_address": withdrawal_address,
        "uptime": uptime,
        "stake_reward": stake_reward,
        "transfer_in": transfer_in,
        "transfer_out": transfer_out,
        "fee": fee,
        "transitions": recent_transitions,
        "program_name": program_name,
    }
    result["resolved_addresses"] = await UIAddress.resolve_recursive_detached(result, db, {})
    return CJSONResponse(result)