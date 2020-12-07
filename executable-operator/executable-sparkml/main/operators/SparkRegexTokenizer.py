import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.feature import RegexTokenizer

"""
@ProjectName: CLIC
@Time       : 2020/12/1 16:43
@Author     : jimmy
@Description: 分割字符串
"""


class SparkRegexTokenizer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkRegexTokenizer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            col = self.params["col"]
            pattern = self.params["pattern"]

            tokenizer = RegexTokenizer(inputCol=col, outputCol=col + '-arr', pattern=pattern)

            self.setOutputData("result", tokenizer.transform(df)
                               .drop(col)
                               .withColumnRenamed(col + '-arr', col))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
