import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import StopWordsRemover

"""
@ProjectName: CLIC
@Time       : 2020/12/7 13:51
@Author     : Jimmy
@Description: Spark ML 去停用词
"""

logger = Logger('OperatorLogger').logger


class SparkStopWordsRemover(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkStopWordsRemover", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["inputCol"]
            output_col = self.params["outputCol"]

            remover = StopWordsRemover(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", remover.transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
