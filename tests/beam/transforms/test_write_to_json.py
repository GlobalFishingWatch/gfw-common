from pathlib import Path

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from gfw.common.beam.transforms.write_to_json import WriteToJson
from gfw.common.io import json_load


def test_write_json(tmp_path):
    transform = WriteToJson(output_dir=tmp_path, output_prefix="test")

    messages = [{"x": 1}, {"x": 2}]

    with _TestPipeline() as p:
        inputs = p | beam.Create(messages)
        inputs | transform

    output_file = transform.path
    assert Path(output_file).is_file()

    output_messages = json_load(output_file, lines=True)
    assert len(output_messages) == len(messages)
