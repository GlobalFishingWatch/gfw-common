from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from jinja2 import Environment

from gfw.common.config import (
    DatePipelineConfig,
    DatetimePipelineConfig,
    PipelineConfig,
    PipelineConfigError,
)


LABELS = {"environment": "development"}
DATES = {"start_date": date(2023, 1, 1), "end_date": date(2023, 12, 31)}
START = datetime(2023, 1, 1, tzinfo=timezone.utc)
DATETIMES = {"start_datetime": START, "end_datetime": START + timedelta(hours=1)}


def test_labels_are_required():
    with pytest.raises(TypeError, match="labels"):
        PipelineConfig()


def test_labels_must_not_be_empty():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        PipelineConfig(labels={})


def test_the_base_config_has_no_range():
    cfg = PipelineConfig(labels=LABELS)

    assert not hasattr(cfg, "start_date")
    assert not hasattr(cfg, "start_datetime")


@pytest.mark.parametrize("cls", [DatePipelineConfig, DatetimePipelineConfig])
def test_the_range_configs_are_pipeline_configs(cls):
    assert issubclass(cls, PipelineConfig)


@pytest.mark.parametrize("missing", ["labels", "start_date", "end_date"])
def test_date_config_requires_labels_and_dates(missing):
    kwargs = {"labels": LABELS, **DATES}
    del kwargs[missing]

    with pytest.raises(TypeError, match=missing):
        DatePipelineConfig(**kwargs)


def test_date_config_holds_its_dates():
    cfg = DatePipelineConfig(labels=LABELS, **DATES)

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), date(2023, 12, 31))


@pytest.mark.parametrize("missing", ["start_date", "end_date"])
def test_date_config_dates_must_not_be_none(missing):
    with pytest.raises(PipelineConfigError, match="start_date and end_date must be set"):
        DatePipelineConfig(labels=LABELS, **{**DATES, missing: None})


@pytest.mark.parametrize(
    "end_date", [date(2023, 1, 1), date(2022, 12, 31)], ids=["empty", "reversed"]
)
def test_date_config_end_date_must_be_after_start_date(end_date):
    with pytest.raises(PipelineConfigError, match=r"end_date .* must be after start_date"):
        DatePipelineConfig(labels=LABELS, start_date=date(2023, 1, 1), end_date=end_date)


def test_date_config_validates_labels_too():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        DatePipelineConfig(labels={}, **DATES)


@pytest.mark.parametrize("missing", ["labels", "start_datetime", "end_datetime"])
def test_datetime_config_requires_labels_and_datetimes(missing):
    kwargs = {"labels": LABELS, **DATETIMES}
    del kwargs[missing]

    with pytest.raises(TypeError, match=missing):
        DatetimePipelineConfig(**kwargs)


def test_datetime_config_range_can_be_shorter_than_a_day():
    cfg = DatetimePipelineConfig(labels=LABELS, **DATETIMES)

    assert cfg.end_datetime - cfg.start_datetime == timedelta(hours=1)


@pytest.mark.parametrize("missing", ["start_datetime", "end_datetime"])
def test_datetime_config_datetimes_must_not_be_none(missing):
    with pytest.raises(PipelineConfigError, match="start_datetime and end_datetime must be set"):
        DatetimePipelineConfig(labels=LABELS, **{**DATETIMES, missing: None})


@pytest.mark.parametrize(
    "end_datetime", [START, START - timedelta(seconds=1)], ids=["empty", "reversed"]
)
def test_datetime_config_end_must_be_after_start(end_datetime):
    with pytest.raises(PipelineConfigError, match=r"end_datetime .* must be after start_datetime"):
        DatetimePipelineConfig(labels=LABELS, start_datetime=START, end_datetime=end_datetime)


@dataclass(frozen=True, kw_only=True)
class ExtraValidationConfig(DatePipelineConfig):
    max_days: int = 7

    def __post_init__(self) -> None:
        super().__post_init__()
        if (self.end_date - self.start_date).days > self.max_days:
            raise PipelineConfigError("range too long")


def test_a_subclass_extends_the_validations():
    with pytest.raises(PipelineConfigError, match="range too long"):
        ExtraValidationConfig(labels=LABELS, **DATES)

    with pytest.raises(PipelineConfigError, match="must be after start_date"):
        ExtraValidationConfig(
            labels=LABELS, start_date=date(2023, 1, 2), end_date=date(2023, 1, 1)
        )


def test_to_dict_includes_fields():
    cfg = DatePipelineConfig(
        labels=LABELS,
        **DATES,
        unknown_parsed_args={"foo": "bar"},
        unknown_unparsed_args=["--baz"],
    )

    d = cfg.to_dict()
    assert d["labels"] == LABELS
    assert d["start_date"] == date(2023, 1, 1)
    assert d["unknown_parsed_args"] == {"foo": "bar"}
    assert d["unknown_unparsed_args"] == ["--baz"]


def test_from_namespace_creates_config():
    namespace = SimpleNamespace(
        labels=LABELS, **DATES, unknown_parsed_args={"other_option": "value"}
    )
    cfg = DatePipelineConfig.from_namespace(namespace)

    assert isinstance(cfg, DatePipelineConfig)
    assert cfg.labels == LABELS
    assert cfg.end_date == date(2023, 12, 31)
    assert cfg.unknown_parsed_args.get("other_option") == "value"


@pytest.mark.parametrize(
    "start_date, end_date",
    [("2023-01-01", "2023-12-31"), ("2023-01-01", date(2023, 12, 31))],
    ids=["strings", "mixed"],
)
def test_from_namespace_parses_date_strings(start_date, end_date):
    namespace = SimpleNamespace(labels=LABELS, start_date=start_date, end_date=end_date)
    cfg = DatePipelineConfig.from_namespace(namespace)

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), date(2023, 12, 31))


def test_from_namespace_parses_datetime_strings_as_utc_unless_a_timezone_is_given():
    namespace = SimpleNamespace(
        labels=LABELS,
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2023-01-01T06:00:00-03:00",
    )
    cfg = DatetimePipelineConfig.from_namespace(namespace)

    assert cfg.start_datetime == START
    assert cfg.end_datetime == START + timedelta(hours=9)
    assert cfg.end_datetime.utcoffset() == timedelta(hours=-3)


def test_from_namespace_rejects_date_strings_not_in_iso_format():
    namespace = SimpleNamespace(labels=LABELS, start_date="01/01/2023", end_date="2023-12-31")

    with pytest.raises(PipelineConfigError, match="start_date must be a date in ISO format"):
        DatePipelineConfig.from_namespace(namespace)


def test_from_namespace_rejects_datetime_strings_not_in_iso_format():
    namespace = SimpleNamespace(
        labels=LABELS, start_datetime="01/01/2023", end_datetime="2023-12-31"
    )

    with pytest.raises(PipelineConfigError, match="start_datetime must be a datetime in ISO"):
        DatetimePipelineConfig.from_namespace(namespace)


@dataclass(frozen=True, kw_only=True)
class ExtraFieldsConfig(DatePipelineConfig):
    open_gaps_start_date: date = date(2019, 1, 1)
    backfill_start: datetime | None = None
    table_suffix: str = ""


def test_from_namespace_parses_only_date_and_datetime_fields_including_subclass_ones():
    namespace = SimpleNamespace(
        labels=LABELS,
        **DATES,
        open_gaps_start_date="2020-06-01",
        backfill_start="2020-06-01T06:00:00",
        table_suffix="2020-06-01",
    )
    cfg = ExtraFieldsConfig.from_namespace(namespace)

    assert cfg.open_gaps_start_date == date(2020, 6, 1)
    assert cfg.backfill_start == datetime(2020, 6, 1, 6, tzinfo=timezone.utc)
    assert cfg.table_suffix == "2020-06-01"


def test_from_namespace_leaves_unset_optional_fields_alone():
    cfg = ExtraFieldsConfig.from_namespace(SimpleNamespace(labels=LABELS, **DATES))

    assert cfg.backfill_start is None


def test_config_is_frozen():
    cfg = DatePipelineConfig(labels=LABELS, **DATES)

    with pytest.raises(FrozenInstanceError):
        cfg.start_date = date(2024, 1, 1)


def test_top_level_package():
    cfg = PipelineConfig(labels=LABELS)
    assert cfg.top_level_package == "gfw"


def test_jinja_env():
    cfg = PipelineConfig(labels=LABELS, jinja_folder="common/assets")
    assert isinstance(cfg.jinja_env, Environment)
