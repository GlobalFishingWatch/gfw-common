"""Abstract base class for implementing command-line interfaces (CLIs)."""

import argparse
import json
import logging
import sys

from collections import ChainMap
from functools import cached_property
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterator, Optional, Sequence, Tuple, Type, Union

from gfw.common.dictionaries import filter_none_values
from gfw.common.io import yaml_load
from gfw.common.logging import LoggerConfig
from gfw.common.serialization import to_json

from .command import Command, ParametrizedCommand
from .formatting import default_formatter
from .option import Option


logger = logging.getLogger(__name__)

try:
    script_relative_path = Path(sys.argv[0]).resolve().relative_to(Path.cwd())
except ValueError:
    # Not relative, just use the absolute path
    # This is needed because otherwise fails in GitHub actions.
    script_relative_path = Path(sys.argv[0]).resolve()


class CLI:
    """Wrapper around argparse for building CLIs more easily.

    Key Features:
        - Supports a single command or multiple subcommands CLI.
        - Common CLI options can be easily defined once and shared across subcommands.
        - Layered configuration resolution: CLI arguments > YAML config file > command defaults.
        - Rich logging with optional plain-text fallback.
        - Optionally allows unrecognized CLI arguments for custom handling.
        - Provides builtin options to provide config file, disable rich logging, etc.

    Args:
        name:
            The main command to be run on the command line (e.g., `my-cli`).

        description:
            A brief message describing what the CLI application does.

        options:
            A tuple of `Option` instances representing CLI arguments for the main command.
            These options are inherited by every subcommand, if any.

        run:
            Callable to be run when no subcommands are defined.

        subcommands:
            A sequence containing either `Command` instances or `Command` subclasses.
            Each item represents a subcommand to be registered in the CLI.
            If a subclass is provided, it will be instantiated automatically.
            This allows flexibility in defining subcommands either by
            passing already created instances or by passing the command classes themselves.

        version:
            The version of the application.

        examples:
            Example command-line usages shown in the help footer.

        formatter:
            Callable that returns a argparse.HelpFormatter for customizing help text.

        logger_config:
            LoggerConfig instance to control logging behavior.

        allow_unknown:
            If True, unknown CLI arguments are allowed.

        use_underscore:
            If True, converts hyphens in the option name to underscores (e.g., `--log_file`).
            If False (default), converts underscores to hyphens (e.g., `--log-file`).
            This controls the naming convention of the CLI argument.

        **main_parser_kwargs:
            Extra arguments passed to ArgumentParser constructor of the main command.
    """

    HELP_CONFIG_FILE = "Path to config file."
    HELP_VERBOSE = "Set logger level to DEBUG."
    HELP_NO_RICH_LOGGING = "Disable rich logging [useful for production environments]."
    HELP_LOG_TO_STDOUT = "If True, sends logs output to sys.stdout stream."
    HELP_LOG_FILE = "File to send logging output to."
    HELP_ONLY_RENDER = "Dry run, only renders command line call and prints it."

    KEY_SUBCOMMAND = "operation"
    KEY_UNKNOWN_UNPARSED_ARGS = "unknown_unparsed_args"
    KEY_UNKNOWN_PARSED_ARGS = "unknown_parsed_args"

    def __init__(
        self,
        name: str = f"python {script_relative_path}",
        description: str = "",
        options: Tuple[Option, ...] = (),
        subcommands: Sequence[Union[Command, Type[Command]]] = (),
        run: Callable[..., Any] = lambda *x, **y: None,
        version: str = "0.1.0",
        examples: Tuple[str, ...] = (),
        formatter: Callable[..., argparse.HelpFormatter] = default_formatter(),
        logger_config: Optional[LoggerConfig] = None,
        allow_unknown: bool = False,
        use_underscore: bool = False,
        **main_parser_kwargs: Any,
    ) -> None:
        """Initializes a CLI instance."""
        self._main_command = ParametrizedCommand(name, description, options, run=run)
        self._subcommands = list(self._init_subcommands(subcommands))
        self._version = version
        self._examples = examples
        self._formatter = formatter
        self._logger_config = logger_config or LoggerConfig()
        self._allow_unknown = allow_unknown
        self._use_underscore = use_underscore
        self._main_parser_kwargs = main_parser_kwargs

    @classmethod
    def builtin_options(cls) -> list[Option]:
        """Defines built-in CLI options used across commands.

        Returns:
            A list of Option instances for config file, logging, etc.
        """
        return [
            Option("-c", "--config-file", type=str, default=None, help=cls.HELP_CONFIG_FILE),
            Option("-v", "--verbose", type=bool, default=False, help=cls.HELP_VERBOSE),
            Option("--log-file", type=str, default=None, help=cls.HELP_LOG_FILE),
            Option("--log-to-stdout", type=bool, default=False, help=cls.HELP_LOG_TO_STDOUT),
            Option("--no-rich-logging", type=bool, default=False, help=cls.HELP_NO_RICH_LOGGING),
            Option("--only-render", type=bool, default=False, help=cls.HELP_ONLY_RENDER),
        ]

    @cached_property
    def title(self) -> str:
        """Returns the CLI program title with version."""
        return "{} (v{}).".format(self._resolve_cli_name(self._main_command.name), self._version)

    @cached_property
    def common_parser(self) -> argparse.ArgumentParser:
        """Constructs the common parser containing built-in CLI options."""
        p = argparse.ArgumentParser(add_help=False)
        g = p.add_argument_group("built-in CLI options")

        for option in self.builtin_options():
            self._add_option_to_parser(g, option)

        return p

    @cached_property
    def main_parser(self) -> argparse.ArgumentParser:
        """Constructs the main argument parser."""
        # Include main command options to the common parser.
        if self._main_command.options:
            g = self.common_parser.add_argument_group(self._main_command.header)
            for option in self._main_command.options:
                self._add_option_to_parser(g, option)

        # Define if main parser inherits built in options.
        main_parser_parents = []
        if not self._subcommands:
            main_parser_parents = [self.common_parser]

        # Instantiate main parser.
        parser = argparse.ArgumentParser(
            prog=self.title,
            description=self._main_command.description,
            epilog=self._epilog(),
            parents=main_parser_parents,
            formatter_class=self._formatter,
            **self._main_parser_kwargs,
        )

        # Add subcommands to the parser if any are defined.
        if self._subcommands:
            subp = parser.add_subparsers(
                title="Available subcommands",
                dest=self.KEY_SUBCOMMAND,
                metavar="<command>",
                required=True,
            )

        for command in self._subcommands:
            p = subp.add_parser(
                self._resolve_cli_name(command.name),
                help=command.description,
                parents=[self.common_parser],
                formatter_class=self._formatter,
            )

            g = p.add_argument_group(command.header)
            for option in command.options:
                self._add_option_to_parser(g, option)

        return parser

    def execute(self, args: list[str] = sys.argv[1:], **kwargs: Any) -> tuple[Any, dict[str, Any]]:
        """Parse arguments, load config, and execute the CLI command.

        Args:
            args:
                Command-line arguments (defaults to sys.argv[1:]).

            kwargs:
                Extra keyword arguments passed to the command's `run` method.

        Returns:
            Tuple containing:
                - Result of the executed command.
                - Configuration dictionary used for execution.
        """
        args = args or ["--help"]
        unknown_unparsed_args: list[str] = []
        if self._allow_unknown:
            ns, unknown_unparsed_args = self.main_parser.parse_known_args(args=args)
        else:
            ns = self.main_parser.parse_args(args=args)

        cli_args = vars(ns)

        # Delete CLI configuration from parsed namespace.
        config_file = cli_args.pop("config_file", None)
        log_file = cli_args.pop("log_file", None)
        verbose = cli_args.pop("verbose")
        no_rich_logging = cli_args.pop("no_rich_logging")
        only_render = cli_args.pop("only_render")
        log_to_stdout = cli_args.pop("log_to_stdout")

        # Load config file if exists.
        config_file_args = {}
        if config_file is not None:
            logger.info(f"Loading config file from {config_file}.")
            config_file_args = yaml_load(config_file)

        unknown_parsed_args = self._extract_unknown_config_file_args(config_file_args, cli_args)

        for u in unknown_parsed_args:
            del config_file_args[u]

        # Resolved invoked command.
        command = self._get_invoked_command(cli_args)

        # Resolve configuration based on cli_args, config file and defaults.
        # cli_args takes precedence over config file and config file over defaults.
        common_defaults = filter_none_values(self._main_command.defaults())
        defaults_args = filter_none_values(command.defaults())
        cli_args = filter_none_values(cli_args)

        defaults_args.update(common_defaults)
        cli_args.pop(self.KEY_SUBCOMMAND, None)

        config = dict(ChainMap(cli_args, config_file_args, defaults_args))

        config[self.KEY_UNKNOWN_UNPARSED_ARGS] = unknown_unparsed_args
        config[self.KEY_UNKNOWN_PARSED_ARGS] = unknown_parsed_args

        # Setup logger.
        self._logger_config.setup(
            verbose=verbose,
            rich=not no_rich_logging,
            log_file=log_file,
            log_stream=sys.stdout if log_to_stdout else sys.stderr,
        )

        # Log relevant information.
        logger.info(f"Starting {self.title}")
        logger.info(f"Running command '{command.name}' with following arguments:")
        logger.info(json.dumps(config, indent=4, default=to_json))

        if only_render:
            rendered = self._render_command_line_call(command.name, config)
            logger.info("Equivalent command-line call: ")
            print(f"{rendered}")
            return rendered, config

        return command.run(SimpleNamespace(**config), **kwargs), config

    def _init_subcommands(self, s: Sequence[Union[Command, Type[Command]]]) -> Iterator[Command]:
        for command in s:
            if isinstance(command, type) and not issubclass(command, Command):
                raise TypeError(f"Expected subclass of Command, got class {command}.")

            if isinstance(command, type):
                yield command()
                continue

            if isinstance(command, Command):
                yield command
                continue

            raise TypeError(f"Expected subclass of Command, got {type(command)}.")

    def _add_option_to_parser(
        self,
        p: Union[argparse.ArgumentParser, argparse._ArgumentGroup],
        option: Option,
    ) -> None:
        flags = self._resolve_cli_flags(option.flags)
        kwargs = option.kwargs.copy()

        # Compose help string with real default appended
        kwargs.setdefault("help", "")
        kwargs["help"] = f"{kwargs['help']} (default: {option.default})"
        kwargs["default"] = None

        kwargs.update({"type": option.type})
        kwargs.setdefault("metavar", "")

        if option.type is bool:
            kwargs.setdefault("action", "store_true" if not option.default else "store_false")
            kwargs.pop("type")
            kwargs.pop("metavar")

        p.add_argument(*flags, dest=option.dest, **kwargs)

    def _epilog(self) -> str:
        indent = " " * 4
        examples_str = "\n".join(f"{indent}{e}" for e in self._examples)
        return f"Examples:\n{examples_str}"

    def _extract_unknown_config_file_args(
        self, config_file: dict[str, Any], args: dict[str, Any]
    ) -> dict[str, Any]:
        known_keys = set(args.keys())
        unknown_config_args = {k: v for k, v in config_file.items() if k not in known_keys}
        if unknown_config_args and not self._allow_unknown:
            raise ValueError(
                f"Invalid configuration file: parameters {list(unknown_config_args.keys())}"
                " are not recognized by any defined argument."
            )

        return unknown_config_args

    def _get_invoked_command(self, args: dict[str, Any]) -> Command:
        subcommand = args.get(self.KEY_SUBCOMMAND)
        if subcommand is None:
            return self._main_command

        # At this point, argparse guarantees it's a valid subcommand.
        return next(c for c in self._subcommands if self._resolve_cli_name(c.name) == subcommand)

    def _render_command_line_call(self, command_name: str, config: dict[str, Any]) -> str:
        config = config.copy()

        main_command = self._main_command.name
        if not main_command.startswith("python"):
            # Is not a python script. It is an installed package.
            main_command = self._resolve_cli_name(main_command)

        parts = [
            main_command,
            self._resolve_cli_name(command_name),
        ]

        unknown_unparsed = config.pop(self.KEY_UNKNOWN_UNPARSED_ARGS, [])
        unknown_parsed = config.pop(self.KEY_UNKNOWN_PARSED_ARGS, {})

        config = {**config, **unknown_parsed}

        if unknown_unparsed:
            parts.append(" ".join(unknown_unparsed))

        argument = "--{name}{sep}{value}"

        for k, v in config.items():
            name = self._resolve_cli_name(k)
            value = v
            sep = "="

            if isinstance(v, (list, tuple)):
                if len(v) == 0:
                    continue

                value = ",".join(v)

            if isinstance(v, bool):
                if not v:
                    continue

                value = ""
                sep = ""

            if isinstance(v, dict):
                value = f"'{json.dumps(v)}'"

            parts.append(argument.format(name=name, sep=sep, value=value))

        return " \\\n".join(parts)

    def _resolve_cli_flags(self, flags: tuple[str, ...]) -> tuple[str, ...]:
        resolved_flags = []

        for flag in flags:
            # Count leading dashes (usually 1 or 2)
            dash_count = len(flag) - len(flag.lstrip("-"))
            prefix = "-" * dash_count

            # Get the raw flag name without dashes
            raw_name = flag.lstrip("-")

            # Apply CLI-specific name formatting
            formatted_name = self._resolve_cli_name(raw_name)

            # Reattach the original dash prefix
            resolved_flags.append(f"{prefix}{formatted_name}")

        return tuple(resolved_flags)

    def _resolve_cli_name(self, name: str) -> str:
        if self._use_underscore:
            old, new = ("-", "_")
        else:
            old, new = ("_", "-")

        return name.replace(old, new)
