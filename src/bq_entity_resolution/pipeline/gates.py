"""Data quality gates: runtime assertions after each stage.

Gates check conditions that MUST hold for the pipeline to produce
valid results. They replace the silent pass-through of empty results
with explicit, actionable errors.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from bq_entity_resolution.stages.base import TableRef

if TYPE_CHECKING:
    from bq_entity_resolution.backends.protocol import Backend


@dataclass(frozen=True)
class GateResult:
    """Result of a quality gate check."""

    passed: bool
    message: str
    severity: str = "error"  # "error" | "warning"


class DataQualityGate(ABC):
    """Base class for data quality gates."""

    @abstractmethod
    def applies_to(self, stage_name: str) -> bool:
        """Whether this gate should run for the given stage."""

    @abstractmethod
    def check(
        self,
        stage_name: str,
        backend: Backend,
        outputs: dict[str, TableRef],
    ) -> GateResult:
        """Run the gate check and return result."""


class OutputNotEmptyGate(DataQualityGate):
    """Ensure a stage's output table is not empty.

    Empty blocking = ERROR (with diagnosis).
    Empty matching = WARNING (valid but unusual).
    """

    def __init__(
        self,
        stage_prefix: str,
        severity: str = "error",
        output_key: str | None = None,
    ):
        self._stage_prefix = stage_prefix
        self._severity = severity
        self._output_key = output_key

    def applies_to(self, stage_name: str) -> bool:
        return stage_name.startswith(self._stage_prefix)

    def check(
        self,
        stage_name: str,
        backend: Backend,
        outputs: dict[str, TableRef],
    ) -> GateResult:
        for key, ref in outputs.items():
            if self._output_key and key != self._output_key:
                continue
            if not ref.fq_name:
                continue
            try:
                count = backend.row_count(ref.fq_name)
                if count == 0:
                    return GateResult(
                        passed=False,
                        message=(
                            f"Output '{key}' ({ref.fq_name}) is empty "
                            f"(0 rows)"
                        ),
                        severity=self._severity,
                    )
            except Exception:
                return GateResult(
                    passed=False,
                    message=(
                        f"Output '{key}' ({ref.fq_name}) does not exist "
                        f"or cannot be queried"
                    ),
                    severity=self._severity,
                )

        return GateResult(passed=True, message="OK")


class ClusterSizeGate(DataQualityGate):
    """Ensure no cluster exceeds the maximum allowed size.

    Large clusters indicate blocking key issues or threshold problems.
    """

    def __init__(
        self,
        max_cluster_size: int,
        abort_on_explosion: bool = False,
    ):
        self._max_cluster_size = max_cluster_size
        self._abort_on_explosion = abort_on_explosion

    def applies_to(self, stage_name: str) -> bool:
        return stage_name == "clustering"

    def check(
        self,
        stage_name: str,
        backend: Backend,
        outputs: dict[str, TableRef],
    ) -> GateResult:
        cluster_ref = outputs.get("clusters")
        if not cluster_ref or not cluster_ref.fq_name:
            return GateResult(
                passed=True, message="No cluster table to check"
            )

        try:
            rows = backend.execute_and_fetch(
                f"SELECT MAX(cluster_size) AS max_size FROM ("
                f"  SELECT cluster_id, COUNT(*) AS cluster_size "
                f"  FROM `{cluster_ref.fq_name}` "
                f"  GROUP BY cluster_id"
                f")"
            )
            if rows and rows[0].get("max_size", 0) > self._max_cluster_size:
                max_size = rows[0]["max_size"]
                severity = (
                    "error" if self._abort_on_explosion else "warning"
                )
                return GateResult(
                    passed=False,
                    message=(
                        f"Max cluster size {max_size} exceeds threshold "
                        f"{self._max_cluster_size}"
                    ),
                    severity=severity,
                )
        except Exception as e:
            return GateResult(
                passed=False,
                message=f"Failed to check cluster sizes: {e}",
                severity="warning",
            )

        return GateResult(
            passed=True, message="Cluster sizes within limits"
        )


def default_gates(config=None) -> list[DataQualityGate]:
    """Create default quality gates for the pipeline.

    Returns gates that should always be active:
    - Blocking output must not be empty (ERROR)
    - Matching output empty is a warning (WARNING)
    - Cluster explosion detection (if configured)
    """
    gates: list[DataQualityGate] = [
        OutputNotEmptyGate(
            "blocking_", severity="error", output_key="candidates"
        ),
        OutputNotEmptyGate("matching_", severity="warning"),
    ]

    if config:
        cq = getattr(config.monitoring, "cluster_quality", None)
        if cq and getattr(cq, "enabled", False):
            gates.append(ClusterSizeGate(
                max_cluster_size=getattr(
                    cq, "alert_max_cluster_size", 100
                ),
                abort_on_explosion=getattr(
                    cq, "abort_on_explosion", False
                ),
            ))

    return gates
