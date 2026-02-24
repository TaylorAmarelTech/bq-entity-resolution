"""Generic function registry with plugin discovery.

Provides a type-safe, introspectable registry that replaces duplicated
@register patterns across the codebase. Implements the dict-like
protocol for backward compatibility with existing code that uses
FEATURE_FUNCTIONS["name"] or "name" in COMPARISON_FUNCTIONS.

Usage:
    from bq_entity_resolution.registry import Registry

    FUNCTIONS = Registry[Callable]("my_registry", "bq_er.my_plugins")

    @FUNCTIONS.register("my_func", cost=5)
    def my_func(inputs, **kwargs):
        return f"UPPER({inputs[0]})"

    # Dict-like access (backward compat)
    fn = FUNCTIONS["my_func"]
    assert "my_func" in FUNCTIONS

    # Rich introspection
    FUNCTIONS.list_all()        # ["my_func"]
    FUNCTIONS.get_metadata("my_func")  # {"cost": 5}

    # Plugin discovery from installed packages
    FUNCTIONS.load_plugins()
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Generic, Iterator, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


class RegistryKeyError(KeyError):
    """Raised when a registry key is not found."""

    def __init__(self, registry_name: str, key: str, available: list[str]):
        self.registry_name = registry_name
        self.key = key
        self.available = available
        close = _suggest_closest(key, available)
        hint = f" Did you mean '{close}'?" if close else ""
        super().__init__(
            f"'{key}' not found in {registry_name} registry.{hint} "
            f"Available: {', '.join(sorted(available[:20]))}"
            + ("..." if len(available) > 20 else "")
        )


def _suggest_closest(name: str, candidates: list[str], cutoff: float = 0.6) -> str | None:
    """Suggest the closest match using difflib."""
    import difflib

    matches = difflib.get_close_matches(name, candidates, n=1, cutoff=cutoff)
    return matches[0] if matches else None


class Registry(Generic[T]):
    """Type-safe, introspectable function registry with plugin discovery.

    Implements dict-like protocol for backward compatibility:
        registry["name"]      → get function
        "name" in registry    → check existence
        for name in registry  → iterate names
        registry.keys()       → all names
        registry.values()     → all functions
        registry.items()      → (name, function) pairs
    """

    def __init__(self, name: str, entry_point_group: str | None = None):
        """Initialize a registry.

        Args:
            name: Human-readable registry name (for error messages).
            entry_point_group: Optional entry point group for plugin discovery.
                e.g., "bq_er.features" enables external packages to register
                functions via pyproject.toml entry_points.
        """
        self._name = name
        self._entry_point_group = entry_point_group
        self._functions: dict[str, T] = {}
        self._metadata: dict[str, dict[str, Any]] = {}
        self._plugins_loaded = False

    @property
    def name(self) -> str:
        """Registry name."""
        return self._name

    def register(self, name: str, **metadata: Any) -> Callable[[T], T]:
        """Decorator to register a function.

        Args:
            name: Unique function name.
            **metadata: Arbitrary metadata (e.g., cost=5, returns="INT64").

        Returns:
            Decorator that registers the function and returns it unchanged.

        Raises:
            ValueError: If name is already registered.
        """
        def decorator(func: T) -> T:
            if name in self._functions:
                logger.warning(
                    "Overwriting '%s' in %s registry", name, self._name
                )
            self._functions[name] = func
            self._metadata[name] = metadata
            return func

        return decorator

    def get(self, name: str) -> T:
        """Get a registered function by name.

        Args:
            name: Function name.

        Returns:
            The registered function.

        Raises:
            RegistryKeyError: If name is not registered.
        """
        if name not in self._functions:
            self._ensure_plugins_loaded()
        if name not in self._functions:
            raise RegistryKeyError(self._name, name, list(self._functions))
        return self._functions[name]

    def list_all(self) -> list[str]:
        """List all registered function names, sorted alphabetically."""
        return sorted(self._functions.keys())

    def get_metadata(self, name: str) -> dict[str, Any]:
        """Get metadata for a registered function.

        Args:
            name: Function name.

        Returns:
            Metadata dict (may be empty).

        Raises:
            RegistryKeyError: If name is not registered.
        """
        if name not in self._metadata:
            raise RegistryKeyError(self._name, name, list(self._functions))
        return dict(self._metadata[name])

    def get_cost(self, name: str) -> int:
        """Get the cost metadata for a function (default 0)."""
        meta = self._metadata.get(name, {})
        return meta.get("cost", 0)

    def load_plugins(self) -> None:
        """Load plugins from entry_points.

        Discovers and loads entry points registered under this registry's
        entry_point_group. Each entry point should be a module that
        contains @register decorators.

        Safe to call multiple times — only loads once.
        """
        if self._plugins_loaded or not self._entry_point_group:
            return
        self._plugins_loaded = True

        try:
            from importlib.metadata import entry_points
        except ImportError:
            return

        try:
            eps = entry_points(group=self._entry_point_group)
        except TypeError:
            # Python 3.9 compat
            all_eps = entry_points()
            eps = all_eps.get(self._entry_point_group, [])

        for ep in eps:
            try:
                ep.load()
                logger.debug(
                    "Loaded plugin '%s' into %s registry", ep.name, self._name
                )
            except Exception as exc:
                logger.warning(
                    "Failed to load plugin '%s' for %s: %s",
                    ep.name, self._name, exc,
                )

    def _ensure_plugins_loaded(self) -> None:
        """Lazily load plugins on first miss."""
        if not self._plugins_loaded:
            self.load_plugins()

    # -- Dict-like protocol for backward compatibility --

    def __getitem__(self, key: str) -> T:
        return self.get(key)

    def __contains__(self, key: object) -> bool:
        if isinstance(key, str) and key not in self._functions:
            self._ensure_plugins_loaded()
        return key in self._functions

    def __iter__(self) -> Iterator[str]:
        return iter(self._functions)

    def __len__(self) -> int:
        return len(self._functions)

    def keys(self) -> list[str]:
        """All registered function names."""
        return list(self._functions.keys())

    def values(self) -> list[T]:
        """All registered functions."""
        return list(self._functions.values())

    def items(self) -> list[tuple[str, T]]:
        """All (name, function) pairs."""
        return list(self._functions.items())

    def __repr__(self) -> str:
        return f"Registry(name={self._name!r}, count={len(self._functions)})"
