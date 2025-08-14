import logging

import avro.schema
import pytest

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from gfw.common.beam.transforms import ReadMatchingAvroFiles


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
    record_1_data = {"data": b"test_data_1", "timestamp": "2025-08-14T09:30:00Z"}
    record_2_data = {"data": b"test_data_2", "timestamp": "2025-08-15T04:00:00Z"}
    record_4_data = {"data": b"test_data_3", "timestamp": "2025-08-15T06:00:00Z"}  # Outside range
    record_3_data = {"data": b"test_data_4", "timestamp": "2025-08-16T00:00:00Z"}  # Outside range

    # Create directories for each date
    dir_14 = base_path / "2025-08-14"
    dir_15 = base_path / "2025-08-15"
    dir_16 = base_path / "2025-08-16"

    dir_14.mkdir()
    dir_15.mkdir()
    dir_16.mkdir()

    # Create the Avro files inside the directories
    create_avro_file(dir_14 / "file-2025-08-14_09_30_00Z.avro", [record_1_data])
    create_avro_file(dir_15 / "file-2025-08-15_04_00_00Z.avro", [record_2_data])
    create_avro_file(dir_15 / "file-2025-08-15_06_00_00Z.avro", [record_3_data])
    create_avro_file(dir_16 / "file-2025-08-16_00_00_00Z.avro", [record_4_data])

    return str(base_path)


def test_read_matching_avro_files(avro_files_base_path):
    """Tests the ReadMatchingAvroFiles PTransform with a local filesystem."""
    start_dt = "2025-08-14T09:00:00"
    end_dt = "2025-08-15T05:00:00"

    # Define the expected output based on the created files and the date range
    expected_output = [
        {"data": "test_data_1", "timestamp": "2025-08-14T09:30:00Z"},
        {"data": "test_data_2", "timestamp": "2025-08-15T04:00:00Z"},
    ]

    with _TestPipeline() as p:
        output = p | "ReadMatchingAvroFiles" >> ReadMatchingAvroFiles(
            base_path=avro_files_base_path, start_dt=start_dt, end_dt=end_dt
        )

        # Assert that the output PCollection matches our expected results
        assert_that(output, equal_to(expected_output))


def test_no_datetime_extraction_logs_error(caplog):
    transform = ReadMatchingAvroFiles(
        base_path="/tmp/some_path",  # doesn't matter for this
        start_dt="2025-08-14T09:00:00",
        end_dt="2025-08-15T00:00:00",
    )

    bad_path = "/path/without/datetime/structure/file.avro"

    with caplog.at_level(logging.ERROR):
        result = transform.is_path_in_range(bad_path)  # or whatever your method is called

    # It should log the error
    assert any("Couldn't extract datetime" in msg for msg in caplog.messages)

    # And return False
    assert result is False
