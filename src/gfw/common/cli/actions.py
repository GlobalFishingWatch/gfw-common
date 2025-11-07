"""Module with custom argparse-compatible actions."""

import argparse
import json

from typing import Any


class NestedKeyValueAction(argparse.Action):
    """Argparse action that parses dotted KEY=VALUE pairs into nested dictionaries.

    This action allows passing command-line arguments like:

        --labels environment=prod metrics.level=high

    or even with deeper nesting:

        --labels metrics.service.latency=200

    The parsed result is stored in the destination attribute as a nested dictionary:

        {
            "environment": "prod",
            "metrics": {
                "level": "high",
                "service": {"latency": "200"}
            }
        }

    Example:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--labels",
            nargs="+",
            action=NestedKeyValueAction,
            default={},
            help="Nested key=value pairs (supports dotted keys)",
        )
    """

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        """Process and assign nested key-value pairs."""
        result = getattr(namespace, self.dest, {}) or {}

        for pair in values:
            if pair.strip().startswith("{"):
                # This is a json string, try to parse it.
                result.update(self._parse_json(pair, parser))
                continue

            # This is not a json string. Must be key=value.
            if "=" not in pair:
                parser.error(
                    f"Invalid format for {option_string or self.dest!r}. "
                    f"Got {pair!r} (expected KEY=VALUE)"
                )

            key, val = pair.split("=", 1)
            parts = key.split(".")
            d = result
            for p in parts[:-1]:
                d = d.setdefault(p, {})

            d[parts[-1]] = val

        setattr(namespace, self.dest, result)

    def _parse_json(self, value: str, parser: argparse.ArgumentParser) -> dict[str, Any]:
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            parser.error(f"Invalid JSON for {self.dest!r}: {e.msg}")
