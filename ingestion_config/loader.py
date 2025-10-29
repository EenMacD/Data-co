from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "filters.yaml"

class ConfigError(RuntimeError):
    """Raised when the ingestion configuration file is missing or invalid."""

@dataclass(slots=True)
class AppConfig:
    """Materialised configuration for the application."""

    search_criteria: Dict[str, Any]
    technical_config: Dict[str, Any]

def load_config(config_path: Path | None = None) -> AppConfig:
    """
    Load the YAML configuration and return the resolved configuration.
    """
    config_path = config_path or CONFIG_PATH
    data = _load_yaml(config_path)

    search_criteria = data.get("search_criteria")
    if not isinstance(search_criteria, dict):
        raise ConfigError("The configuration file must define 'search_criteria'.")

    technical_config = data.get("technical_config")
    if not isinstance(technical_config, dict):
        raise ConfigError("The configuration file must define 'technical_config'.")

    return AppConfig(
        search_criteria=search_criteria,
        technical_config=technical_config,
    )

def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Configuration file not found at {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        if not isinstance(data, dict):
            raise ConfigError("The configuration file must define a mapping at the top level.")
        return data
    except ImportError as exc:
        raise ConfigError(
            "PyYAML is required to read the ingestion configuration. "
            "Install it with `pip install pyyaml`."
        ) from exc