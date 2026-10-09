from dataclasses import FrozenInstanceError
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from jinja2 import Environment

from gfw.common.config import DateRangePipelineConfig, PipelineConfig, PipelineConfigError
from gfw.common.datetime import DateRange


LABELS = {"environment": "development"}


def test_labels_are_required():
    with pytest.raises(TypeError, match="labels"):
        PipelineConfig()


def test_labels_must_not_be_empty():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        PipelineConfig(labels={})


def test_to_dict_includes_fields():
    cfg = PipelineConfig(
        labels=LABELS,
        unknown_parsed_args={"foo": "bar"},
        unknown_unparsed_args=["--baz"],
    )

    d = cfg.to_dict()
    assert d["labels"] == LABELS
    assert d["unknown_parsed_args"] == {"foo": "bar"}
    assert d["unknown_unparsed_args"] == ["--baz"]


def test_from_namespace_creates_config():
    namespace = SimpleNamespace(labels=LABELS, unknown_parsed_args={"other_option": "value"})
    cfg = PipelineConfig.from_namespace(namespace)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.labels == LABELS
    assert cfg.unknown_parsed_args.get("other_option") == "value"


def test_top_level_package():
    cfg = PipelineConfig(labels=LABELS)
    assert cfg.top_level_package == "gfw"


def test_jinja_env():
    cfg = PipelineConfig(labels=LABELS, jinja_folder="common/assets")
    assert isinstance(cfg.jinja_env, Environment)


@pytest.mark.parametrize(
    "start_date, end_date",
    [
        ("2023-01-01", "2023-12-31"),
        (date(2023, 1, 1), date(2023, 12, 31)),
        ("2023-01-01", date(2023, 12, 31)),
    ],
    ids=["iso-strings", "dates", "mixed"],
)
def test_date_range_config_stores_dates(start_date, end_date):
    cfg = DateRangePipelineConfig(labels=LABELS, start_date=start_date, end_date=end_date)

    assert cfg.start_date == date(2023, 1, 1)
    assert cfg.end_date == date(2023, 12, 31)
    assert cfg.date_range == DateRange(date(2023, 1, 1), date(2023, 12, 31))


def test_date_range_config_requires_both_dates():
    with pytest.raises(TypeError, match="end_date"):
        DateRangePipelineConfig(labels=LABELS, start_date="2023-01-01")


@pytest.mark.parametrize(
    "start_date, end_date, field",
    [
        ("2023-01-01", "not-a-date", "end_date"),
        ("01/01/2023", "2023-12-31", "start_date"),
        (datetime(2023, 1, 1), "2023-12-31", "start_date"),
        (20230101, "2023-12-31", "start_date"),
    ],
    ids=["invalid-string", "not-iso", "datetime", "int"],
)
def test_date_range_config_rejects_invalid_dates(start_date, end_date, field):
    with pytest.raises(PipelineConfigError, match=f"{field} must be a date in ISO format"):
        DateRangePipelineConfig(labels=LABELS, start_date=start_date, end_date=end_date)


@pytest.mark.parametrize("end_date", ["2023-01-01", "2022-12-31"], ids=["empty", "reversed"])
def test_date_range_config_rejects_end_not_after_start(end_date):
    with pytest.raises(PipelineConfigError, match="must be after the start date"):
        DateRangePipelineConfig(labels=LABELS, start_date="2023-01-01", end_date=end_date)


def test_date_range_config_still_requires_labels():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        DateRangePipelineConfig(labels={}, start_date="2023-01-01", end_date="2023-01-02")


def test_date_range_config_from_namespace_parses_dates():
    namespace = SimpleNamespace(labels=LABELS, start_date="2023-06-01", end_date="2023-07-01")

    cfg = DateRangePipelineConfig.from_namespace(namespace)

    assert (cfg.start_date, cfg.end_date) == (date(2023, 6, 1), date(2023, 7, 1))


def test_date_range_config_is_frozen():
    cfg = DateRangePipelineConfig(labels=LABELS, start_date="2023-01-01", end_date="2023-01-02")

    with pytest.raises(FrozenInstanceError):
        cfg.start_date = date(2024, 1, 1)
