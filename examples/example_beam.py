import logging
import apache_beam as beam

from gfw.common.beam.pipeline import LinearDag, Pipeline
logger = logging.getLogger(__name__)


class SourcePTransform(beam.PTransform):
    def expand(self, p):
        return p | beam.Create(["a", "b", "c"])


dag = LinearDag(sources=[SourcePTransform()])
pipeline = Pipeline(dag=dag, project="world-fishing-827")
result, _ = pipeline.run()
print(result.state)
