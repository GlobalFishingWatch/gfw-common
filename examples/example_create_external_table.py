import logging

from gfw.common.bigquery import BigQueryHelper

logger = logging.getLogger(__name__)


bq = BigQueryHelper(project="world-fishing-827")
schema = bq.client.schema_from_json("./assets/parsed-nmea-schema.json")

bq.create_external_table(
    table="gfw-int-ais-datalake.vessel_transmissions_v1.messages_creation_test",
    source_uris=["gs://gfw-int-ais-datalake-vessel-transmissions-v1/messages/*.parquet"],
    description="Testing description",
    schema=schema,
    hive_partition_uri_prefix="gs://gfw-int-ais-datalake-vessel-transmissions-v1/messages/",
    require_partition_filter=True,
    replace=True,
)
