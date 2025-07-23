from pathlib import Path

import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.transforms.read_from_json import ReadFromJson
from gfw.common.io import json_save


@pytest.mark.parametrize("lines", [True, False], ids=["json-lines-true", "json-lines-false"])
def test_read_from_json_variants(tmp_path, lines):
    path = tmp_path / ("data.jsonl" if lines else "data.json")
    input_data = [{"x": 1}, {"x": 2}]
    json_save(path, input_data, lines=lines)

    with TestPipeline() as p:
        output = p | ReadFromJson(path, lines=lines)
        assert_that(output, equal_to(input_data))


def test_raises_if_file_does_not_exist():
    path = Path("/this/does/not/exist.json")

    with pytest.raises(ValueError, match="Input file does not exist"):
        with TestPipeline() as p:
            _ = p | ReadFromJson(path)
