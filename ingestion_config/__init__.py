"""Utilities for loading and working with the ingestion configuration."""

from .loader import (
    ConfigError,
    AppConfig,
    load_config,
)

__all__ = [
    "ConfigError",
    "AppConfig",
    "load_config",
]