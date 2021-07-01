import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import VectorAssembler
"""
@ProjectName: CLIC
@Time       : 2020/12/8 16:04
@Author     : Jimmy
@Description: Spark ML 将多个特征拼接成一个向量
"""

logger = Logger('OperatorLogger').logger


class SparkVectorAssembler(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkVectorAssembler", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_cols = self.params["inputCols"]
            output_col = self.params["outputCol"]
            handle_invalid = self.params["handleInvalid"] if "handleInvalid" in self.params else None
            handle_invalid = 'skip' if handle_invalid is not None and str(handle_invalid).lower() == 'skip' else 'keep'

            assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col, handleInvalid=handle_invalid)

            self.setOutputData("result", assembler.transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
