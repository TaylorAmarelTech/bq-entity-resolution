"""Pipeline DAG: declarative stage graph from config.

Replaces the imperative orchestrator.run() with a directed acyclic
graph derived from stage input/output TableRef matching plus explicit
ordering for the tier chain.
"""

from __future__ import annotations

import bisect
import logging
from dataclasses import dataclass
from typing import Any

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.stages.base import Stage, TableRef
from bq_entity_resolution.stages.staging import StagingStage
from bq_entity_resolution.stages.features import (
    FeatureEngineeringStage,
    TermFrequencyStage,
)
from bq_entity_resolution.stages.blocking import BlockingStage
from bq_entity_resolution.stages.matching import MatchingStage
from bq_entity_resolution.stages.reconciliation import (
    ClusteringStage,
    GoldOutputStage,
    ClusterQualityStage,
)
from bq_entity_resolution.stages.active_learning import ActiveLearningStage
from bq_entity_resolution.stages.label_ingestion import LabelIngestionStage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StageNode:
    """A node in the pipeline DAG."""

    stage: Stage
    dependencies: tuple[str, ...] = ()


class StageDAG:
    """Directed acyclic graph of pipeline stages.

    Ordering is determined by:
    1. Auto-resolved: output TableRef.fq_name matches input TableRef.fq_name
    2. Explicit: additional edges from build_pipeline_dag()

    Topological sort determines execution order.
    """

    def __init__(self, nodes: list[StageNode]):
        self._nodes: dict[str, StageNode] = {}
        for node in nodes:
            if node.stage.name in self._nodes:
                raise ValueError(f"Duplicate stage name: {node.stage.name}")
            self._nodes[node.stage.name] = node
        self._order = self._topological_sort()

    @property
    def stages(self) -> list[Stage]:
        """Stages in topological (execution) order."""
        return [self._nodes[name].stage for name in self._order]

    @property
    def stage_names(self) -> list[str]:
        """Stage names in execution order."""
        return list(self._order)

    def get_stage(self, name: str) -> Stage:
        """Get a stage by name."""
        return self._nodes[name].stage

    def get_dependencies(self, name: str) -> list[str]:
        """Get dependency names for a stage."""
        return list(self._nodes[name].dependencies)

    def get_dependents(self, name: str) -> list[str]:
        """Get stages that depend on the given stage."""
        return [
            n.stage.name
            for n in self._nodes.values()
            if name in n.dependencies
        ]

    @classmethod
    def from_stages(
        cls,
        stages: list[Stage],
        explicit_edges: dict[str, list[str]] | None = None,
    ) -> StageDAG:
        """Build DAG from stages with auto + explicit dependencies.

        Auto-resolved: matches output fq_name to input fq_name.
        Explicit: extra edges from the caller (e.g., tier ordering).
        """
        explicit_edges = explicit_edges or {}

        # Map: output fq_name -> producing stage name
        producers: dict[str, str] = {}
        for stage in stages:
            for ref in stage.outputs.values():
                if ref.fq_name:
                    producers[ref.fq_name] = stage.name

        nodes = []
        for stage in stages:
            deps: set[str] = set()

            # Auto-resolved from TableRef matching
            for ref in stage.inputs.values():
                if ref.fq_name in producers:
                    producer = producers[ref.fq_name]
                    if producer != stage.name:
                        deps.add(producer)

            # Explicit edges
            for dep_name in explicit_edges.get(stage.name, []):
                deps.add(dep_name)

            nodes.append(StageNode(
                stage=stage,
                dependencies=tuple(sorted(deps)),
            ))

        return cls(nodes)

    def _topological_sort(self) -> list[str]:
        """Kahn's algorithm for topological ordering."""
        in_degree = {
            name: len(node.dependencies)
            for name, node in self._nodes.items()
        }

        queue = sorted(
            name for name, deg in in_degree.items() if deg == 0
        )
        order: list[str] = []

        while queue:
            name = queue.pop(0)
            order.append(name)

            for other_name, other_node in self._nodes.items():
                if name in other_node.dependencies:
                    in_degree[other_name] -= 1
                    if in_degree[other_name] == 0:
                        bisect.insort(queue, other_name)

        if len(order) != len(self._nodes):
            remaining = sorted(set(self._nodes) - set(order))
            raise ValueError(
                f"Cycle detected in stage DAG involving: {remaining}"
            )

        return order

    def __len__(self) -> int:
        return len(self._nodes)

    def __repr__(self) -> str:
        return f"StageDAG(stages={self.stage_names})"


def build_pipeline_dag(config: PipelineConfig) -> StageDAG:
    """Build a complete pipeline DAG from config.

    Creates stage instances and resolves dependencies via
    TableRef matching + explicit tier-chain ordering.
    """
    stages: list[Stage] = []
    explicit_edges: dict[str, list[str]] = {}

    # 1. Staging: one per source
    for source in config.sources:
        stages.append(StagingStage(source, config))

    # 2. Feature engineering (auto-depends on staging via TableRef)
    stages.append(FeatureEngineeringStage(config))

    # 3. Term frequencies (auto-depends on features via TableRef)
    stages.append(TermFrequencyStage(config))

    # 4. Blocking + matching per tier with explicit tier ordering
    prev_matching_name: str | None = None
    for i, tier in enumerate(config.enabled_tiers()):
        blocking = BlockingStage(tier, i, config)
        matching = MatchingStage(tier, i, config)
        stages.append(blocking)
        stages.append(matching)

        # Cross-tier exclusion: tier i's blocking depends on tier i-1's matching
        if prev_matching_name:
            explicit_edges[blocking.name] = [prev_matching_name]

        prev_matching_name = matching.name

        # Active learning per tier (if enabled)
        if getattr(tier.active_learning, "enabled", False):
            al_stage = ActiveLearningStage(tier, config)
            stages.append(al_stage)

            # Label ingestion: ingest human labels from review queue
            if getattr(tier.active_learning.label_feedback, "enabled", False):
                li_stage = LabelIngestionStage(tier, config)
                stages.append(li_stage)
                # Ingestion depends on review queue being created
                explicit_edges[li_stage.name] = [al_stage.name]

    # 5. Reconciliation
    stages.append(ClusteringStage(config))
    # Clustering depends on last matching tier completing
    if prev_matching_name:
        explicit_edges["clustering"] = [prev_matching_name]

    stages.append(GoldOutputStage(config))

    # 6. Cluster quality (optional monitoring)
    if getattr(config.monitoring.cluster_quality, "enabled", False):
        stages.append(ClusterQualityStage(config))

    return StageDAG.from_stages(stages, explicit_edges)
