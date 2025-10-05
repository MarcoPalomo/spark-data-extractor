import os
import re
from typing import Any, Dict

import yaml


_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*) (?::- ([^}]*))?\}")


def _expand_env_vars(value: str) -> str:
    """Expand ${VAR} or ${VAR:-default} in a string using process env.

    Note: spaces inside the pattern above are for readability; we will handle
    a simpler replacement to support both ${VAR} and ${VAR:-default}.
    """
    def repl(match: re.Match) -> str:
        expr = match.group(0)
        # Support ${VAR} and ${VAR:-default}
        if expr.startswith("${") and expr.endswith("}"):
            inner = expr[2:-1]
            if ":-" in inner:
                var, default = inner.split(":-", 1)
                return os.getenv(var.strip(), default)
            return os.getenv(inner.strip(), "")
        return expr

    # Replace occurrences
    result = value
    # Find patterns like ${VAR} and ${VAR:-default}
    for m in re.findall(r"\$\{[^}]+\}", value):
        result = result.replace(m, repl(re.match(r".*", m)))
    return result


def _expand_in_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _expand_in_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_in_obj(v) for v in obj]
    if isinstance(obj, str):
        return _expand_env_vars(obj)
    return obj


def load_pipeline_config(path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """Load pipeline configuration from YAML, expanding environment variables.

    - Reads YAML safely
    - Expands ${VAR} and ${VAR:-default}
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return _expand_in_obj(data)

