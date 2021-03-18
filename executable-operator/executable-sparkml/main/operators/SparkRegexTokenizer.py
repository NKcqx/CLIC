import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.feature import RegexTokenizer

"""
@ProjectName: CLIC
@Time       : 2020/12/1 16:43
@Author     : jimmy
@Description: Spark 分割字符串
"""


class SparkRegexTokenizer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkRegexTokenizer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["input_col"]
            pattern = self.params["pattern"]
            output_col = self.params["output_col"]

            tokenizer = RegexTokenizer(inputCol=input_col, outputCol=output_col, pattern=pattern)

            self.setOutputData("result", tokenizer.transform(df))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
