from datetime import date
from types import SimpleNamespace

import pytest

from jinja2 import Environment

from gfw.common.beam.pipeline.config import ERROR_DATE, PipelineConfig, PipelineConfigError


def test_parsed_date_range_valid():
    cfg = PipelineConfig(date_range=("2023-01-01", "2023-12-31"))
    parsed = cfg.parsed_date_range
    assert isinstance(parsed, tuple)

    assert parsed[0] == date(2023, 1, 1)
    assert parsed[1] == date(2023, 12, 31)

    assert cfg.start_date == date(2023, 1, 1)
    assert cfg.end_date == date(2023, 12, 31)


def test_parsed_date_range_invalid_raises():
    invalid_range = ("2023-01-01", "not-a-date")
    cfg = PipelineConfig(date_range=invalid_range)
    with pytest.raises(PipelineConfigError) as exc_info:
        _ = cfg.parsed_date_range

    assert ERROR_DATE.format(invalid_range) in str(exc_info.value)


def test_to_dict_includes_fields():
    cfg = PipelineConfig(
        date_range=("2023-01-01", "2023-12-31"),
        unknown_parsed_args={"foo": "bar"},
        unknown_unparsed_args=["--baz"],
    )

    d = cfg.to_dict()
    assert d["date_range"] == ("2023-01-01", "2023-12-31")
    assert d["unknown_parsed_args"] == {"foo": "bar"}
    assert d["unknown_unparsed_args"] == ["--baz"]


def test_from_namespace_creates_config():
    namespace = SimpleNamespace(
        date_range=("2023-06-01", "2023-06-30"),
        unknown_parsed_args={"other_option": "value"},
    )
    cfg = PipelineConfig.from_namespace(namespace)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.date_range == ("2023-06-01", "2023-06-30")
    assert cfg.unknown_parsed_args.get("other_option") == "value"


def test_top_level_package():
    cfg = PipelineConfig(date_range=("2023-01-01", "2023-12-31"))
    assert cfg.top_level_package == "gfw"


def test_jinja_env():
    cfg = PipelineConfig(date_range=("2023-01-01", "2023-12-31"), jinja_folder="common/assets")
    assert isinstance(cfg.jinja_env, Environment)
