"""SQL builder for active learning (replaces active_learning_queue.sql.j2).

Generates SQL for review queue creation based on uncertainty sampling:
- Fellegi-Sunter: pairs nearest to 0.5 match probability
- Sum scoring: pairs nearest to threshold score
"""

from __future__ import annotations

from dataclasses import dataclass

from bq_entity_resolution.sql.expression import SQLExpression


@dataclass(frozen=True)
class ActiveLearningParams:
    """Parameters for active learning queue SQL generation."""
    review_table: str
    matches_table: str
    queue_size: int = 100
    uncertainty_window: float = 0.3
    is_fellegi_sunter: bool = False
    min_score: float = 0.0


def build_active_learning_sql(params: ActiveLearningParams) -> SQLExpression:
    """Build active learning review queue SQL.

    Selects pairs nearest to the decision boundary for human review.
    """
    lines: list[str] = []

    lines.append(f"CREATE OR REPLACE TABLE `{params.review_table}` AS")
    lines.append("")

    if params.is_fellegi_sunter:
        lines.append("SELECT")
        lines.append("  l_entity_uid,")
        lines.append("  r_entity_uid,")
        lines.append("  total_score,")
        lines.append("  match_confidence,")
        lines.append("  ABS(match_confidence - 0.5) AS uncertainty,")
        lines.append("  CAST(NULL AS BOOL) AS human_label,")
        lines.append("  CURRENT_TIMESTAMP() AS queued_at")
        lines.append(f"FROM `{params.matches_table}`")
        lines.append(
            f"WHERE ABS(match_confidence - 0.5) <= {params.uncertainty_window}"
        )
        lines.append("ORDER BY ABS(match_confidence - 0.5) ASC")
        lines.append(f"LIMIT {params.queue_size}")
    else:
        lines.append("SELECT")
        lines.append("  l_entity_uid,")
        lines.append("  r_entity_uid,")
        lines.append("  total_score,")
        lines.append("  match_confidence,")

        if params.min_score > 0:
            lines.append(
                f"  ABS(total_score - {params.min_score}) AS uncertainty,"
            )
        else:
            lines.append(
                "  ABS(match_confidence - 0.5) AS uncertainty,"
            )

        lines.append("  CAST(NULL AS BOOL) AS human_label,")
        lines.append("  CURRENT_TIMESTAMP() AS queued_at")
        lines.append(f"FROM `{params.matches_table}`")

        if params.min_score > 0:
            lines.append(
                f"ORDER BY ABS(total_score - {params.min_score}) ASC"
            )
        else:
            lines.append("ORDER BY ABS(match_confidence - 0.5) ASC")

        lines.append(f"LIMIT {params.queue_size}")

    return SQLExpression.from_raw("\n".join(lines))
