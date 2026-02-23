"""
Domain-specific Pydantic v2 configuration models.

Sub-modules are organized by domain. The parent ``config.schema`` module
re-exports everything so existing imports remain unchanged.
"""

from bq_entity_resolution.config.models.source import *  # noqa: F401,F403
from bq_entity_resolution.config.models.features import *  # noqa: F401,F403
from bq_entity_resolution.config.models.blocking import *  # noqa: F401,F403
from bq_entity_resolution.config.models.matching import *  # noqa: F401,F403
from bq_entity_resolution.config.models.reconciliation import *  # noqa: F401,F403
from bq_entity_resolution.config.models.infrastructure import *  # noqa: F401,F403
from bq_entity_resolution.config.models.pipeline import *  # noqa: F401,F403
