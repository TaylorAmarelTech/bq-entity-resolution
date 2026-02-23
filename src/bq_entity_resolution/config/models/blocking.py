"""
Blocking configuration models.

Defines blocking path and tier-level blocking configuration used
for candidate pair generation within matching tiers.
"""

from __future__ import annotations

from pydantic import BaseModel

__all__ = [
    "BlockingPathDef",
    "TierBlockingConfig",
]


class BlockingPathDef(BaseModel):
    """A single blocking path within a tier."""

    keys: list[str]
    candidate_limit: int = 200
    lsh_min_bands: int = 1  # for LSH blocking: min matching bands


class TierBlockingConfig(BaseModel):
    """Blocking configuration for a matching tier."""

    paths: list[BlockingPathDef]
    cross_batch: bool = True  # also compare against gold canonicals
