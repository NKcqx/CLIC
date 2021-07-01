import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import RegexTokenizer

"""
@ProjectName: CLIC
@Time       : 2020/12/1 16:43
@Author     : Jimmy
@Description: Spark ML 正则表达式匹配或者分割字符串
"""

logger = Logger('OperatorLogger').logger


class SparkRegexTokenizer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkRegexTokenizer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["inputCol"]
            pattern = self.params["pattern"]
            output_col = self.params["outputCol"]

            tokenizer = RegexTokenizer(inputCol=input_col, outputCol=output_col, pattern=pattern)

            self.setOutputData("result", tokenizer.transform(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
