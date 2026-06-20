"""PTransforms for reading from and writing to JSON."""

import json

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Union

import apache_beam as beam

from apache_beam.pvalue import PCollection

from gfw.common.io import json_load


class ReadFromJson(beam.PTransform):
    """Beam transform to read a PCollection from a JSON file.

    This transform loads a local JSON or JSONLines file eagerly (outside the pipeline),
    then injects the resulting records into the pipeline using :class:`beam.Create`.

    Useful for testing, prototyping, or controlled ingestion.

    Args:
        input_file:
            Path to the local file to read.

        coder:
            Callable to apply to each decoded record. Defaults to :class:`dict`.

        lines:
            If True, interprets the input as newline-delimited JSON (JSONLines).

        create_kwargs:
            Optional dictionary of keyword arguments to pass to :class:`beam.Create`.
            Use this to control serialization, type hints, etc.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.

    Raises:
        ValueError:
            If the input file does not exist at pipeline construction time.

    Example:
        .. code-block:: python

            with beam.Pipeline() as p:
                pcoll = p | ReadFromJson("data/input.json", lines=True)
                pcoll | beam.Map(print)
    """

    def __init__(
        self,
        input_file: Union[str, Path],
        coder: Callable = dict,
        lines: bool = False,
        create_kwargs: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Builds a ReadFromJson instance."""
        super().__init__(**kwargs)
        self._input_file = Path(input_file)
        self._coder = coder
        self._lines = lines
        self._create_kwargs = create_kwargs or {}

    def expand(self, p: PCollection) -> PCollection:
        """Apply transform to pipeline ``p``: create PCollection from loaded JSON data."""
        # Why not use beam.io.ReadFromJson instead of (beam.Create + json_load)?
        # Because ReadFromJson returns BeamSchema objects, not plain dicts,
        # and requires conversion like: dict(x._asdict()).
        # inputs = (
        #     p
        #     | beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
        #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
        # )
        # In our case, json_load + beam.Create gives us full control over parsing
        # and works better for small, local test/config files where eager loading is acceptable.

        if not self._input_file.exists():
            raise ValueError(f"Input file does not exist: {self._input_file}")

        data = json_load(self._input_file, lines=self._lines, coder=self._coder)
        return p | beam.Create(data, **self._create_kwargs).with_output_types(self._coder)


class WriteToJson(beam.PTransform):
    """Writes PCollection as JSON.

    Args:
        output_dir:
            Output directory.

        output_prefix:
            Prefix to use in filename/s.

        **kwargs:
            Additional keyword arguments passed to base PTransform class.
    """

    WORKDIR_DEFAULT = "workdir"

    def __init__(
        self, output_dir: str = WORKDIR_DEFAULT, output_prefix: str = "", **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._output_dir = Path(output_dir)

        time = datetime.now().isoformat(timespec="seconds").replace("-", "").replace(":", "")
        self._output_prefix = f"beam-{output_prefix}-{time}"

        self._prefix = self._output_dir.joinpath(self._output_prefix).as_posix()
        self._shard_name_template = ""
        self._suffix = ".json"

        # This is what beam.io.WriteToText does to construct the path.
        self.path = Path("".join([self._prefix, self._shard_name_template, self._suffix]))

    def expand(self, pcoll: PCollection) -> PCollection:
        """Writes the input PCollection to a JSON file."""
        return pcoll | "WriteToJson" >> (
            beam.Map(json.dumps)
            | beam.io.WriteToText(
                self._prefix,
                shard_name_template=self._shard_name_template,
                file_name_suffix=self._suffix,
            )
        )
        """
        Why not use :class:`beam.io.WriteToJson`?
        `WriteToJson` has issues writing to local files.
        WriteToJson raises a ValueError when the path does not point to a GCS location.
        It works when used together with `ReadFromBigQuery` and a GCS location is specified there.
        This makes it unreliable for local development or testing.

        Additionally, it internally relies on :meth:`pandas.DataFrame.to_json`,
        which introduces extra dependencies and may not preserve the original structure of
        dict-like records.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToJson

        Example usage of :class:`beam.io.WriteToJson`:
            .. code-block:: python
                from apache_beam.io.fileio import default_file_naming

                file_naming = default_file_naming(prefix=self._output_prefix, suffix=".json")
                return pcoll | beam.io.WriteToJson(
                    self._output_dir.as_posix(),
                    file_naming=file_naming,
                    lines=True,
                    indent=4,
                )

        For these reasons, we use :class:`beam.io.WriteToText` + :func:``json.dumps``,
        which is lightweight, predictable, and preserves control over formatting and encoding.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        """
