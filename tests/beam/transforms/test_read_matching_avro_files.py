from string import Template

import avro.schema
import pytest

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from gfw.common.beam.transforms import ReadMatchingAvroFiles
from gfw.common.datetime import datetime_from_isoformat


# Define the Avro schema for our test data
SCHEMA_STR = """
{
    "type": "record",
    "name": "TestRecord",
    "fields": [
        {"name": "data", "type": "bytes"},
        {"name": "timestamp", "type": "string"}
    ]
}
"""
SCHEMA = avro.schema.parse(SCHEMA_STR)


def record_time_fn(record: dict) -> bool:
    return datetime_from_isoformat(record["timestamp"])


def create_avro_file(filepath, records):
    """Create a valid Avro container file with the given records."""
    with open(filepath, "wb") as f:
        writer = DataFileWriter(f, DatumWriter(), SCHEMA)
        for record in records:
            writer.append(record)
        writer.close()


@pytest.fixture
def avro_files_base_path(tmp_path):
    """Creates a temporary directory with a set of Avro files."""
    base_path = tmp_path / "test_data"
    base_path.mkdir()

    # Define some records to write to the files
    record_0_data = {"data": b"test_data_0", "timestamp": "2025-08-14T00:04:00+00:00"}
    record_1_data = {"data": b"test_data_1", "timestamp": "2025-08-14T09:30:00+00:00"}
    record_2_data = {"data": b"test_data_2", "timestamp": "2025-08-15T04:00:00+00:00"}

    # Outside range
    record_4_data = {"data": b"test_data_3", "timestamp": "2025-08-15T06:00:00+00:00"}
    record_3_data = {"data": b"test_data_4", "timestamp": "2025-08-16T00:00:00+00:00"}

    # Create directories for each date
    dir_13 = base_path / "2025-08-13"
    dir_14 = base_path / "2025-08-14"
    dir_15 = base_path / "2025-08-15"
    dir_16 = base_path / "2025-08-16"

    dir_13.mkdir()
    dir_14.mkdir()
    dir_15.mkdir()
    dir_16.mkdir()

    # Create the Avro files inside the directories
    # We put the first record inside range the day before, since this can happen in prod.
    create_avro_file(dir_13 / "file-2025-08-13_23_59_00Z.avro", [record_0_data])
    create_avro_file(dir_14 / "file-2025-08-14_09_30_00Z.avro", [record_1_data])
    create_avro_file(dir_15 / "file-2025-08-15_04_00_00Z.avro", [record_2_data])
    create_avro_file(dir_15 / "file-2025-08-15_06_00_00Z.avro", [record_3_data])
    create_avro_file(dir_16 / "file-2025-08-16_00_00_00Z.avro", [record_4_data])

    return str(base_path)


def test_read_matching_avro_files(avro_files_base_path):
    """Tests the ReadMatchingAvroFiles PTransform with a local filesystem."""
    start_dt = "2025-08-14T00:00:00"
    end_dt = "2025-08-15T05:00:00"

    # Define the expected output based on the created files and the date range
    expected_output = [
        {"data": "test_data_0", "timestamp": "2025-08-14T00:04:00+00:00"},
        {"data": "test_data_1", "timestamp": "2025-08-14T09:30:00+00:00"},
        {"data": "test_data_2", "timestamp": "2025-08-15T04:00:00+00:00"},
    ]

    path_template = Template("${base_path}/{date}/*.avro")
    path = path_template.substitute(base_path=avro_files_base_path)

    with _TestPipeline() as p:
        output = p | "ReadMatchingAvroFiles" >> ReadMatchingAvroFiles(
            path=path,
            start_dt=start_dt,
            end_dt=end_dt,
            record_time_fn=record_time_fn,
            date_format="%Y-%m-%d",
            time_format="%H_%M_%SZ",
        )

        # Assert that the output PCollection matches our expected results
        assert_that(output, equal_to(expected_output))


def test_no_datetime_extraction_logs_error(caplog):
    transform = ReadMatchingAvroFiles(
        path="/tmp/some_path",  # doesn't matter for this test
        start_dt="2025-08-14T09:00:00",
        end_dt="2025-08-15T00:00:00",
    )

    bad_path = "/path/without/datetime/structure/file.avro"

    with pytest.raises(ValueError):
        transform.is_path_in_range(bad_path)
