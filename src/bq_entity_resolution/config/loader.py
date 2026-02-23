"""
Configuration loader: YAML parsing, environment variable interpolation,
defaults merging, and Pydantic validation.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from bq_entity_resolution.config.schema import PipelineConfig
from bq_entity_resolution.exceptions import ConfigurationError

ENV_VAR_PATTERN = re.compile(r"\$\{([^}^{]+)\}")


def _interpolate_env_vars(obj: Any) -> Any:
    """Recursively replace ${VAR} and ${VAR:-default} with environment values."""
    if isinstance(obj, str):

        def _replacer(match: re.Match[str]) -> str:
            expr = match.group(1)
            if ":-" in expr:
                var_name, default = expr.split(":-", 1)
                return os.environ.get(var_name.strip(), default)
            value = os.environ.get(expr)
            if value is None:
                raise ConfigurationError(
                    f"Environment variable '${{{expr}}}' is not set. "
                    f"Set it or provide a default: ${{{expr}:-default_value}}"
                )
            return value

        return ENV_VAR_PATTERN.sub(_replacer, obj)

    if isinstance(obj, dict):
        return {k: _interpolate_env_vars(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_interpolate_env_vars(item) for item in obj]

    return obj


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge *override* into *base*. Override wins on leaf conflicts."""
    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(
    config_path: str | Path,
    defaults_path: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
    skip_env_interpolation: bool = False,
) -> PipelineConfig:
    """
    Load, merge, interpolate, and validate pipeline configuration.

    Order of precedence (highest wins):
      1. *overrides* dict
      2. User config YAML
      3. Defaults YAML
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise ConfigurationError(f"Config file not found: {config_path}")

    # Load defaults
    base: dict[str, Any] = {}
    if defaults_path:
        dp = Path(defaults_path)
        if dp.exists():
            with open(dp, encoding="utf-8") as f:
                base = yaml.safe_load(f) or {}

    # Load user config
    with open(config_path, encoding="utf-8") as f:
        user_config = yaml.safe_load(f) or {}

    # Merge: defaults <- user <- overrides
    merged = _deep_merge(base, user_config)
    if overrides:
        merged = _deep_merge(merged, overrides)

    # Environment variable interpolation
    if not skip_env_interpolation:
        try:
            merged = _interpolate_env_vars(merged)
        except ConfigurationError:
            raise
        except Exception as exc:
            raise ConfigurationError(f"Environment variable interpolation failed: {exc}") from exc

    # Validate with Pydantic
    try:
        return PipelineConfig(**merged)
    except Exception as exc:
        raise ConfigurationError(f"Configuration validation failed: {exc}") from exc
