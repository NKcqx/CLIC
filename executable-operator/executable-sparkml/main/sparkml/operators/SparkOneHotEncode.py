import traceback

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/25 15:07
@Author     : Jimmy
@Description: Spark ML onehot编码
"""

logger = Logger('OperatorLogger').logger


class SparkOneHotEncode(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkOneHotEncode", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            cols = [x.strip() for x in self.params["cols"].split(',')]
            indexers = [StringIndexer(inputCol=c, outputCol=c + '-idx') for c in cols]
            encoders = [OneHotEncoder(inputCol=c + '-idx', outputCol=c + "-onehot", dropLast=False) for c in cols]
            pipeline = Pipeline(stages=indexers + encoders)
            df = pipeline.fit(df).transform(df)
            for c in cols:
                df = df.drop(c).drop(c + '-idx')
                df = df.withColumnRenamed(c + "-onehot", c)

            self.setOutputData("result", df)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
