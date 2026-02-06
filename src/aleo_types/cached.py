from functools import lru_cache

import aleo_explorer_rust

from aleo_types import ComputeKey


@lru_cache(maxsize=1048576)
def cached_get_key_id(program_id: str, mapping_name: str, key: bytes) -> str:
    return aleo_explorer_rust.get_key_id(program_id, mapping_name, key)


@lru_cache(maxsize=1048576)
def cached_get_mapping_id(program_id: str, mapping: str) -> str:
    return aleo_explorer_rust.get_mapping_id(program_id, mapping)


@lru_cache(maxsize=1024)
def cached_compute_key_to_address(compute_key: ComputeKey) -> str:
    return aleo_explorer_rust.compute_key_to_address(compute_key.dump())
