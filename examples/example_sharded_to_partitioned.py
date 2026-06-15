"""Example: consolidate sharded AIS normalized tables into a single daily-partitioned table."""
from __future__ import annotations

from pathlib import Path

from gfw.common.bigquery import ShardedToPartitioned


def main():
    ShardedToPartitioned(
        tables=[
            "world-fishing-827.pipe_ais_sources_v20201001.normalized_orbcomm",
            "world-fishing-827.pipe_ais_sources_v20201001.normalized_spire",
            "world-fishing-827.pipe_ais_sources_v20201001.pipe_nmea_normalized",
            "world-fishing-827.pipe_ais_sources_v20220628.normalized_orbcomm",
            "world-fishing-827.pipe_ais_sources_v20220628.normalized_spire",
            "world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_normalized",
            "world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_marinetraffic_normalized",
        ],
        target="world-fishing-827.pipe_ais_sources_v20220628.normalized_consolidated",
        schema=Path(__file__).parent / "assets/normalize-schema.json",
        execution_project="world-fishing-827",
        partition_field="timestamp",
    ).run(dry_run=True, overwrite=False, limit=1)


if __name__ == "__main__":
    main()
