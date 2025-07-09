import apache_beam as beam
import pytest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from gfw.common.beam.transforms import GroupBy


@pytest.mark.parametrize(
    "keys, custom_label, expected_label",
    [
        pytest.param(
            ["city", "country"], "", "GroupByCityAndCountry", id="without-elements-label"
        ),
        pytest.param(
            ["city", "country"], "Users", "GroupUsersByCityAndCountry", id="with-elements-label"
        ),
    ],
)
def test_groupby_label(keys, custom_label, expected_label):
    transform = GroupBy(*keys, elements=custom_label)
    assert transform.label == expected_label


def test_groupby_groups_elements_correctly():
    input_data = [
        {"user": "alice", "country": "US", "value": 1},
        {"user": "bob", "country": "US", "value": 2},
        {"user": "alice", "country": "CA", "value": 3},
        {"user": "alice", "country": "US", "value": 4},
    ]

    expected = [
        {"user": "alice", "country": "US", "value": [1, 4]},
        {"user": "bob", "country": "US", "value": [2]},
        {"user": "alice", "country": "CA", "value": [3]},
    ]

    with TestPipeline() as p:
        pcoll = p | beam.Create(input_data)

        # Apply GroupBy on keys 'user' and 'country'
        grouped = pcoll | GroupBy("user", "country")

        # Now transform grouped elements to a uniform format for checking:
        # grouped elements have keys plus grouped fields as lists (Beam groups automatically)

        def format_group(grouped):
            key, elements = grouped
            return {
                "user": key.user,
                "country": key.country,
                "value": sorted(e["value"] for e in elements),
            }

        formatted = grouped | beam.Map(format_group)

        assert_that(formatted, equal_to(expected))
