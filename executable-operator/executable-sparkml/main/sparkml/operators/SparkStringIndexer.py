import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import StringIndexer

"""
@ProjectName: CLIC
@Time       : 2021/4/12 16:06
@Author     : Jimmy
@Description: Spark ML 标签索引器
"""

logger = Logger('OperatorLogger').logger


class SparkStringIndexer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkStringIndexer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["inputCol"]
            output_col = self.params["outputCol"]

            indexer = StringIndexer(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", indexer.fit(df).transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
