from unittest import mock

from apache_beam import PTransform

from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.dag.factory import DagFactory, LinearDagFactory
from gfw.common.bigquery.helper import BigQueryHelper


class DummyTransform(PTransform):
    pass


class DummyConfig:
    def __init__(self, mock_bq_clients=False):
        self.mock_bq_clients = mock_bq_clients


class ConcreteDagFactory(DagFactory):
    def __init__(self, config):
        self.config = config

    def build_dag(self):
        return ()


class ConcreteLinearDagFactory(LinearDagFactory):
    def __init__(self, config):
        self.config = config

    @property
    def sources(self):
        return (DummyTransform(),)

    @property
    def core(self):
        return DummyTransform()

    @property
    def side_inputs(self):
        return DummyTransform()

    @property
    def sinks(self):
        return (DummyTransform(),)


def test_write_to_bigquery_factory_returns_callable():
    factory = ConcreteDagFactory(config=DummyConfig(mock_bq_clients=False))
    client_factory = factory.write_to_bigquery_factory
    assert callable(client_factory)


def test_bigquery_helper_factory_returns_bigquery_helper_instance():
    factory = ConcreteDagFactory(config=DummyConfig(mock_bq_clients=False))
    helper = factory.bigquery_helper_factory()
    assert isinstance(helper, BigQueryHelper)


def test_bigquery_helper_factory_returns_mocked_client():
    factory = ConcreteDagFactory(config=DummyConfig(mock_bq_clients=True))
    helper = factory.bigquery_helper_factory()
    assert isinstance(helper, BigQueryHelper)
    # Should contain a mocked BigQuery client
    assert isinstance(helper.client, mock.NonCallableMagicMock)


def test_linear_dag_factory_builds_linear_dag():
    factory = ConcreteLinearDagFactory(config=DummyConfig())
    dag = factory.build_dag()
    assert isinstance(dag, LinearDag)
