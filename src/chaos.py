"""
chaos.py - Simulates late-arriving upstream record deletions.

Before the fetch loop starts, a random subset of IDs is marked as deleted.
When the pipeline tries to fetch one of these IDs, a 404 is simulated.
This reproduces the race condition where a separate process deletes a record
in the same window that the ingestion job is running.
"""

import random
from typing import Set, List

from config import CHAOS_DELETION_PROBABILITY
from logger import get_logger

log = get_logger("chaos")


def build_chaos_set(record_ids: List[int]) -> Set[int]:
    """
    Randomly select IDs to mark as deleted before the fetch loop.
    Each ID is evaluated independently against CHAOS_DELETION_PROBABILITY.
    """
    deleted: Set[int] = set()

    for rid in record_ids:
        if random.random() < CHAOS_DELETION_PROBABILITY:
            deleted.add(rid)
            log.warning("Record ID=%d marked as deleted by chaos module.", rid)

    log.info(
        "Chaos module: %d / %d records marked as deleted.",
        len(deleted),
        len(record_ids),
    )
    return deleted


def is_chaos_deleted(record_id: int, chaos_set: Set[int]) -> bool:
    return record_id in chaos_set
