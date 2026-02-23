"""
Source-related configuration models.

Defines the schema for source tables, column mappings, and join
configurations that feed entities into the pipeline.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

__all__ = [
    "ColumnMapping",
    "JoinConfig",
    "SourceConfig",
]


class ColumnMapping(BaseModel):
    """Maps a source column to a semantic role for automatic feature engineering."""

    name: str
    type: str = "STRING"
    role: Optional[str] = None  # first_name, last_name, address_line_1, etc.
    nullable: bool = True


class JoinConfig(BaseModel):
    """Defines how to join a supplemental source to the primary source."""

    table: str
    alias: str = ""
    on: str  # SQL join condition
    type: Literal["LEFT", "INNER"] = "LEFT"


class SourceConfig(BaseModel):
    """A source table that feeds entities into the pipeline."""

    name: str
    table: str
    unique_key: str
    updated_at: str
    partition_column: Optional[str] = None
    columns: list[ColumnMapping]
    passthrough_columns: list[str] = Field(default_factory=list)
    joins: list[JoinConfig] = Field(default_factory=list)
    filter: Optional[str] = None  # optional WHERE clause fragment
    entity_type_column: Optional[str] = None
    batch_size: int = 2_000_000

    @field_validator("columns")
    @classmethod
    def unique_column_names(cls, v: list[ColumnMapping]) -> list[ColumnMapping]:
        names = [c.name for c in v]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            raise ValueError(f"Duplicate column names: {set(dupes)}")
        return v

    @model_validator(mode="after")
    def _validate_column_names(self) -> "SourceConfig":
        """Validate that column names are safe SQL identifiers."""
        from bq_entity_resolution.sql.utils import validate_identifier
        for col in self.columns:
            try:
                validate_identifier(col.name, "column name")
            except ValueError as e:
                raise ValueError(str(e)) from e
        return self

    @classmethod
    def from_table(
        cls,
        table: str,
        backend: Any = None,
        unique_key: str = "id",
        updated_at: str = "updated_at",
        name: Optional[str] = None,
        exclude_columns: Optional[set[str]] = None,
        auto_roles: bool = True,
    ) -> "SourceConfig":
        """Create a SourceConfig by discovering columns from a live table.

        Auto-detects column types from BigQuery INFORMATION_SCHEMA and
        assigns semantic roles from column names. Reduces boilerplate when
        setting up new sources.

        Args:
            table: Fully-qualified BigQuery table (project.dataset.table).
            backend: A Backend instance (BigQuery or DuckDB) for schema
                introspection. If None, columns must be provided manually.
            unique_key: Primary key column name.
            updated_at: Timestamp column for incremental processing.
            name: Source name (derived from table if not set).
            exclude_columns: Column names to skip (e.g. internal audit cols).
            auto_roles: If True, auto-detect roles from column names.

        Example:
            from bq_entity_resolution.backends.bigquery import BigQueryBackend
            backend = BigQueryBackend(project="my-project")
            source = SourceConfig.from_table(
                "my-project.raw.customers",
                backend=backend,
            )
        """
        from bq_entity_resolution.config.roles import detect_role

        if not name:
            name = table.rsplit(".", 1)[-1]

        exclude = exclude_columns or set()

        columns: list[ColumnMapping] = []
        partition_col = None

        if backend is not None:
            schema = backend.get_table_schema(table)
            for col_def in schema.columns:
                if col_def.name in exclude:
                    continue
                role = detect_role(col_def.name) if auto_roles else None
                columns.append(ColumnMapping(
                    name=col_def.name,
                    type=col_def.type,
                    role=role,
                    nullable=col_def.nullable,
                ))
                # Auto-detect partition column
                if col_def.type in ("TIMESTAMP", "DATE", "DATETIME"):
                    if col_def.name == updated_at:
                        partition_col = col_def.name
        else:
            raise ValueError(
                "backend is required for SourceConfig.from_table(). "
                "Pass a BigQueryBackend or DuckDBBackend instance."
            )

        return cls(
            name=name,
            table=table,
            unique_key=unique_key,
            updated_at=updated_at,
            partition_column=partition_col,
            columns=columns,
        )
