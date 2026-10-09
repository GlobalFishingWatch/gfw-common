from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from datetime import date
from types import SimpleNamespace

import pytest

from jinja2 import Environment

from gfw.common.config import PipelineConfig, PipelineConfigError


LABELS = {"environment": "development"}
DATES = {"start_date": date(2023, 1, 1), "end_date": date(2023, 12, 31)}


@dataclass(frozen=True, kw_only=True)
class NoDatesConfig(PipelineConfig):
    """A pipeline that processes no dates, making them optional."""

    start_date: date | None = None
    end_date: date | None = None


@pytest.mark.parametrize("missing", ["labels", "start_date", "end_date"])
def test_labels_and_dates_are_required(missing):
    kwargs = {"labels": LABELS, **DATES}
    del kwargs[missing]

    with pytest.raises(TypeError, match=missing):
        PipelineConfig(**kwargs)


def test_labels_must_not_be_empty():
    with pytest.raises(PipelineConfigError, match="labels must not be empty"):
        PipelineConfig(labels={}, **DATES)


@pytest.mark.parametrize(
    "end_date", [date(2023, 1, 1), date(2022, 12, 31)], ids=["empty", "reversed"]
)
def test_end_date_must_be_after_start_date(end_date):
    with pytest.raises(PipelineConfigError, match="must be after the start date"):
        PipelineConfig(labels=LABELS, start_date=date(2023, 1, 1), end_date=end_date)


def test_a_subclass_can_make_the_dates_optional():
    cfg = NoDatesConfig(labels=LABELS, start_date=date(2023, 1, 1))

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), None)


def test_a_subclass_with_optional_dates_still_validates_the_range():
    with pytest.raises(PipelineConfigError, match="must be after the start date"):
        NoDatesConfig(labels=LABELS, start_date=date(2023, 1, 1), end_date=date(2023, 1, 1))


def test_to_dict_includes_fields():
    cfg = PipelineConfig(
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
    cfg = PipelineConfig.from_namespace(namespace)

    assert isinstance(cfg, PipelineConfig)
    assert cfg.labels == LABELS
    assert cfg.end_date == date(2023, 12, 31)
    assert cfg.unknown_parsed_args.get("other_option") == "value"


@pytest.mark.parametrize(
    "start_date, end_date",
    [
        ("2023-01-01", "2023-12-31"),
        ("2023-01-01", date(2023, 12, 31)),
    ],
    ids=["iso-strings", "mixed"],
)
def test_from_namespace_parses_iso_strings(start_date, end_date):
    namespace = SimpleNamespace(labels=LABELS, start_date=start_date, end_date=end_date)
    cfg = PipelineConfig.from_namespace(namespace)

    assert (cfg.start_date, cfg.end_date) == (date(2023, 1, 1), date(2023, 12, 31))


@pytest.mark.parametrize("start_date", ["not-a-date", "01/01/2023"], ids=["invalid", "not-iso"])
def test_from_namespace_rejects_invalid_strings(start_date):
    namespace = SimpleNamespace(labels=LABELS, start_date=start_date, end_date="2023-12-31")

    with pytest.raises(PipelineConfigError, match="start_date must be a date in ISO format"):
        PipelineConfig.from_namespace(namespace)


@dataclass(frozen=True, kw_only=True)
class ExtraDateConfig(PipelineConfig):
    open_gaps_start_date: date = date(2019, 1, 1)
    name_suffix: str = "2019-01-01"


def test_from_namespace_parses_only_date_fields_including_subclass_ones():
    namespace = SimpleNamespace(
        labels=LABELS,
        **DATES,
        open_gaps_start_date="2020-06-01",
        name_suffix="2020-06-01",
    )
    cfg = ExtraDateConfig.from_namespace(namespace)

    assert cfg.open_gaps_start_date == date(2020, 6, 1)
    assert cfg.name_suffix == "2020-06-01"


def test_from_namespace_leaves_missing_optional_dates_alone():
    cfg = NoDatesConfig.from_namespace(SimpleNamespace(labels=LABELS))

    assert (cfg.start_date, cfg.end_date) == (None, None)


def test_config_is_frozen():
    cfg = PipelineConfig(labels=LABELS, **DATES)

    with pytest.raises(FrozenInstanceError):
        cfg.start_date = date(2024, 1, 1)


def test_top_level_package():
    cfg = PipelineConfig(labels=LABELS, **DATES)
    assert cfg.top_level_package == "gfw"


def test_jinja_env():
    cfg = PipelineConfig(labels=LABELS, **DATES, jinja_folder="common/assets")
    assert isinstance(cfg.jinja_env, Environment)
