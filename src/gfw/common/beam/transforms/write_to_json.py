"""Module with reusable PTransforms for writing output PCollections."""

import json

from datetime import datetime
from pathlib import Path
from typing import Any

import apache_beam as beam

from apache_beam.pvalue import PCollection


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
        Why not use beam.io.WriteToJson?
        `WriteToJson` has issues writing to local files.
        WriteToJson raises a ValueError when the path does not point to a GCS location.
        It works when used together with `ReadFromBigQuery` and a GCS location is specified there.
        This makes it unreliable for local development or testing.

        Additionally, it internally relies on `pandas.DataFrame.to_json`, which introduces extra
        dependencies and may not preserve the original structure of dict-like records.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToJson

        Example usage:
            from apache_beam.io.fileio import default_file_naming

            file_naming = default_file_naming(prefix=self._output_prefix, suffix=".json")
            return pcoll | beam.io.WriteToJson(
                self._output_dir.as_posix(),
                file_naming=file_naming,
                lines=True,
                indent=4,
            )

        For these reasons, we use `WriteToText` + `json.dumps`, which is lightweight, predictable,
        and preserves control over formatting and encoding.
        https://beam.apache.org/releases/pydoc/current/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        """
