from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from jinja2 import Environment

from gfw.common.config import PipelineConfig, PipelineConfigError


UTC = timezone.utc
LABELS = {"environment": "development"}
START = datetime(2023, 1, 1, tzinfo=UTC)
END = datetime(2023, 12, 31, tzinfo=UTC)
DATETIMES = {"start_datetime": START, "end_datetime": END}
DATES = {"start_date": "2023-01-01", "end_date": "2023-12-31"}


@pytest.mark.parametrize("missing", ["labels", "start_datetime", "end_datetime"])
def test_labels_and_datetimes_are_required(missing):
    kwargs = {"labels": LABELS, **DATETIMES}
    del kwargs[missing]

    with pytest.raises(TypeError, match=missing):
        PipelineConfig(**kwargs)


def test_labels_must_not_be_empty():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        PipelineConfig(labels={}, **DATETIMES)


@pytest.mark.parametrize(
    "end_datetime", [START, START - timedelta(seconds=1)], ids=["empty", "reversed"]
)
def test_end_datetime_must_be_after_start_datetime(end_datetime):
    with pytest.raises(PipelineConfigError, match="must be after start_datetime"):
        PipelineConfig(labels=LABELS, start_datetime=START, end_datetime=end_datetime)


def test_start_date_and_end_date_are_the_dates_of_midnight_datetimes():
    cfg = PipelineConfig(labels=LABELS, **DATETIMES)

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), date(2023, 12, 31))


@pytest.mark.parametrize(
    "end_datetime",
    [END.replace(hour=6), END.replace(microsecond=1)],
    ids=["hours", "microsecond"],
)
def test_end_date_is_rounded_up_and_start_date_down(end_datetime):
    cfg = PipelineConfig(
        labels=LABELS, start_datetime=START.replace(hour=6), end_datetime=end_datetime
    )

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), date(2024, 1, 1))


@pytest.mark.parametrize("missing", ["start_datetime", "end_datetime"])
def test_datetimes_must_not_be_none(missing):
    kwargs = {**DATETIMES, missing: None}

    with pytest.raises(PipelineConfigError, match="start_datetime and end_datetime must be set"):
        PipelineConfig(labels=LABELS, **kwargs)


def test_the_range_can_be_shorter_than_a_day():
    end_datetime = START + timedelta(hours=1)
    cfg = PipelineConfig(labels=LABELS, start_datetime=START, end_datetime=end_datetime)

    assert cfg.end_datetime - cfg.start_datetime == timedelta(hours=1)


@dataclass(frozen=True, kw_only=True)
class ExtraValidationConfig(PipelineConfig):
    max_hours: int = 24

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.end_datetime - self.start_datetime > timedelta(hours=self.max_hours):
            raise PipelineConfigError("range too long")


def test_a_subclass_extends_the_validations():
    with pytest.raises(PipelineConfigError, match="range too long"):
        ExtraValidationConfig(labels=LABELS, **DATETIMES)

    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        ExtraValidationConfig(labels={}, **DATETIMES)


def test_to_dict_includes_fields():
    cfg = PipelineConfig(
        labels=LABELS,
        **DATETIMES,
        unknown_parsed_args={"foo": "bar"},
        unknown_unparsed_args=["--baz"],
    )

    d = cfg.to_dict()
    assert d["labels"] == LABELS
    assert d["start_datetime"] == START
    assert d["unknown_parsed_args"] == {"foo": "bar"}
    assert d["unknown_unparsed_args"] == ["--baz"]


def test_from_namespace_creates_config():
    namespace = SimpleNamespace(
        labels=LABELS, **DATES, unknown_parsed_args={"other_option": "value"}
    )
    cfg = PipelineConfig.from_namespace(namespace)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.labels == LABELS
    assert (cfg.start_datetime, cfg.end_datetime) == (START, END)
    assert "start_date" not in cfg.unknown_parsed_args
    assert cfg.unknown_parsed_args.get("other_option") == "value"


@pytest.mark.parametrize(
    "start_date, expected",
    [
        ("2023-01-01", START),
        ("2023-01-01T12:30:00", START.replace(hour=12, minute=30)),
        ("2023-01-01T12:30:00+00:00", START.replace(hour=12, minute=30)),
        (date(2023, 1, 1), START),
        (datetime(2023, 1, 1), START),
        (START, START),
    ],
    ids=["iso-date", "iso-datetime", "iso-datetime-utc", "date", "naive-datetime", "utc-datetime"],
)
def test_from_namespace_turns_dates_into_utc_datetimes(start_date, expected):
    namespace = SimpleNamespace(labels=LABELS, start_date=start_date, end_date="2023-12-31")
    cfg = PipelineConfig.from_namespace(namespace)

    assert cfg.start_datetime == expected
    assert cfg.start_datetime.tzinfo is not None
    assert cfg.end_datetime == END


def test_from_namespace_accepts_datetimes_directly():
    namespace = SimpleNamespace(
        labels=LABELS, start_datetime="2023-01-01T06:00:00", end_datetime=END
    )
    cfg = PipelineConfig.from_namespace(namespace)

    assert (cfg.start_datetime, cfg.end_datetime) == (START.replace(hour=6), END)


def test_from_namespace_keeps_a_given_timezone():
    namespace = SimpleNamespace(
        labels=LABELS, start_date="2023-01-01T00:00:00-03:00", end_date="2023-12-31"
    )
    cfg = PipelineConfig.from_namespace(namespace)

    assert cfg.start_datetime == START + timedelta(hours=3)
    assert cfg.start_datetime.utcoffset() == timedelta(hours=-3)


@pytest.mark.parametrize("start_date", ["not-a-date", "01/01/2023"], ids=["invalid", "not-iso"])
def test_from_namespace_rejects_invalid_strings(start_date):
    namespace = SimpleNamespace(labels=LABELS, start_date=start_date, end_date="2023-12-31")

    with pytest.raises(
        PipelineConfigError, match="start_datetime must be a date or datetime in ISO"
    ):
        PipelineConfig.from_namespace(namespace)


@dataclass(frozen=True, kw_only=True)
class ExtraDateConfig(PipelineConfig):
    open_gaps_start: datetime = datetime(2019, 1, 1, tzinfo=UTC)
    backfill_start: datetime | None = None
    open_gaps_day: date = date(2019, 1, 1)
    name_suffix: str = "2019-01-01"


def test_from_namespace_converts_only_datetime_fields_including_subclass_ones():
    namespace = SimpleNamespace(
        labels=LABELS,
        **DATES,
        open_gaps_start="2020-06-01",
        backfill_start="2020-06-01T06:00:00",
        open_gaps_day="2020-06-01",
        name_suffix="2020-06-01",
    )
    cfg = ExtraDateConfig.from_namespace(namespace)

    assert cfg.open_gaps_start == datetime(2020, 6, 1, tzinfo=UTC)
    assert cfg.backfill_start == datetime(2020, 6, 1, 6, tzinfo=UTC)
    assert cfg.open_gaps_day == "2020-06-01"
    assert cfg.name_suffix == "2020-06-01"


def test_from_namespace_leaves_unset_optional_fields_alone():
    cfg = ExtraDateConfig.from_namespace(SimpleNamespace(labels=LABELS, **DATES))

    assert cfg.backfill_start is None


def test_config_is_frozen():
    cfg = PipelineConfig(labels=LABELS, **DATETIMES)

    with pytest.raises(FrozenInstanceError):
        cfg.start_datetime = START


def test_top_level_package():
    cfg = PipelineConfig(labels=LABELS, **DATETIMES)
    assert cfg.top_level_package == "gfw"


def test_jinja_env():
    cfg = PipelineConfig(labels=LABELS, **DATETIMES, jinja_folder="common/assets")
    assert isinstance(cfg.jinja_env, Environment)
